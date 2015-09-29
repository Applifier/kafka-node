'use strict';

var net  = require('net'),
    util = require('util'),
      _ = require('lodash'),
    inspect = function(obj) {
        return JSON.stringify(obj);
    },
    async = require('async'),
    retry = require('retry'),
    events = require('events'),
    errors = require('./errors'),
    Binary = require('binary'),
    getCodec = require('./codec'),
    protocol = require('./protocol'),
    encodeMessageSet = protocol.encodeMessageSet,
    Message = protocol.Message,
    zk = require('./zookeeper'),
    Zookeeper = zk.Zookeeper,
    debug = require('debug')('kafka-node:Client');
/**
 * Communicates with kafka brokers
 * Uses zookeeper to discover all the kafka brokers to connect to
 *
 * @example <caption>Non chrooted connection to a single zookeeper host</caption>
 * var client = new Client('localhost:2181')
 *
 * @example <caption>Chrooted connection to multiple zookeeper hosts</caption>
 * var client = new Client('localhost:2181,localhost:2182/exmaple/chroot
 *
 * @param {String} [connectionString='localhost:2181/kafka0.8'] A string containing a list of zookeeper hosts:port
 *      and the zookeeper chroot
 * @param {String} [clientId='kafka-node-client'] The client id to register with zookeeper, helpful for debugging
 * @param {Object} zkOptions Pass through options to the zookeeper client library
 * @param {Function} logger Logging function to use. Will default to debug.
 *
 * @constructor
 */
var Client = function (connectionString, clientId, zkOptions, logger) {
    if (this instanceof Client === false) {
        return new Client(connectionString, clientId);
    }

    this.connectionString = connectionString || 'localhost:2181/';
    this.clientId = clientId || 'kafka-node-client';
    var logfn = logger ? logger : debug;
    var prefix = "[kafka-node " + clientId + "] ";
    this.logger = function(msg) {
        logfn(prefix + msg);
    };
    this.zkOptions = zkOptions;
    this.brokers = {};
    this.longpollingBrokers = {};
    this.topicMetadata = {};
    this.topicPartitions = {};
    this.correlationId = 0;
    this._socketId = 0;
    this.cbqueue = {};
    this.brokerMetadata = {};
    this.ready = false;
    this.connect();
};

util.inherits(Client, events.EventEmitter);

Client.prototype.connect = function () {
    var zk = this.zk = new Zookeeper(this.connectionString, this.zkOptions);
    var self = this;
    this.logger("kafka-node client connecting");
    zk.once('init', function (brokers) {
        self.logger("zk init event. broker metadata " + inspect(brokers));
        self.ready = true;
        self.brokerMetadata = brokers;
        Object
            .keys(brokers)
            .some(function (key, index) {
                var broker = brokers[key];
                var addr = broker.host + ':' + broker.port;
                self.brokers[addr] = self.createBroker(broker.host, broker.port);
                // Only connect one broker
                return !index;
            });
        self.emit('ready');
    });
    zk.on('brokersChanged', function (brokerMetadata) {
        self.logger("zk brokersChanged event");
        self.refreshBrokers(brokerMetadata);
        // Emit after a 3 seconds
        setTimeout(function () {
            self.emit('brokersChanged');
        }, 3000);
    });
    zk.on('error', function (err) {
        self.logger("zk client emitted error: " + err);
        self.emit('error', err);
    });
};

Client.prototype.close = function (cb) {
    this.closeBrokers(this.brokers);
    this.closeBrokers(this.longpollingBrokers);
    this.zk.close();
    cb && cb();
};

Client.prototype.closeBrokers = function (brokers) {
    this.logger("closeBrokers " + _.map(brokers, _.property("addr")));
    _.each(brokers, function (broker) {
        broker.closing = true;
        broker.end();
    });
};

Client.prototype.sendFetchRequest = function (consumer, payloads, fetchMaxWaitMs, fetchMinBytes, maxTickMessages) {
    var self = this;
    var encoder = protocol.encodeFetchRequest(fetchMaxWaitMs, fetchMinBytes);
    var decoder = protocol.decodeFetchResponse(function (err, type, message) {
            if (err) {
                if (err.message === 'OffsetOutOfRange') {
                    return consumer.emit('offsetOutOfRange', err);
                } else if (err.message === 'NotLeaderForPartition') {
                    return self.emit('brokersChanged');
                }

                return consumer.emit('error', err);
            }

            var encoding = consumer.options.encoding;

            if (type === 'message') {
                if (encoding !== 'buffer' && message.value) {
                    message.value = message.value.toString(encoding);
                }

                consumer.emit('message', message);
            } else {
                consumer.emit('done', message);
            }
        }, maxTickMessages);

    this.send(payloads, encoder, decoder, function (err) {
        if (err) {
            Array.prototype.unshift.call(arguments, 'error');
            consumer.emit.apply(consumer, arguments);
        }
    });
};

Client.prototype.sendProduceRequest = function (payloads, requireAcks, ackTimeoutMs, cb) {
    var encoder = protocol.encodeProduceRequest(requireAcks, ackTimeoutMs);
    var decoder = protocol.decodeProduceResponse;
    var self = this;

    decoder.requireAcks = requireAcks;
    async.each(payloads, buildRequest, function (err) {
        if (err) return cb(err);
        self.send(payloads, encoder, decoder, function (err, result) {
            if (err) {
                if (err.message === 'NotLeaderForPartition') {
                    self.emit('brokersChanged');
                }
                cb(err);
            } else {
                cb(null, result);
            }
        });
    });

    function buildRequest (payload, cb) {
        var attributes = payload.attributes;
        var codec = getCodec(attributes);

        if (!codec) return cb();

        var innerSet = encodeMessageSet(payload.messages);
        codec.encode(innerSet, function(err, message) {
            if (err) return cb(err);
            payload.messages = [ new Message(0, attributes, '', message) ];
            cb();
        });
    }
};

Client.prototype.sendOffsetCommitRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetCommitRequest(group),
        decoder = protocol.decodeOffsetCommitResponse;
    this.send(payloads, encoder, decoder, cb);
};

Client.prototype.sendOffsetFetchRequest = function (group, payloads, cb) {
    var encoder = protocol.encodeOffsetFetchRequest(group),
        decoder = protocol.decodeOffsetFetchResponse;
    this.send(payloads, encoder, decoder, cb);
};

Client.prototype.sendOffsetRequest = function (payloads, cb) {
    var encoder = protocol.encodeOffsetRequest,
        decoder = protocol.decodeOffsetResponse;
    this.send(payloads, encoder, decoder, cb);
};

/*
 *  Helper method
 *  topic in paylods may send to different broker, so we cache data util all request came back
 */
function wrap(payloads, cb) {
    var out = {};
    var count = Object.keys(payloads).length;

    return function (err, data) {
        // data: { topicName1: {}, topicName2: {} }
        if (err) return cb && cb(err);
        _.merge(out, data);
        count -= 1;
        // Waiting for all request return
        if (count !== 0) return;
        cb && cb(null, out);
    }
}

/**
 * Fetches metadata information for a topic
 * This includes an array containing a each zookeeper node, their nodeId, host name, and port. As well as an object
 * containing the topic name, partition, leader number, replica count, and in sync replicas per partition.
 *
 * @param {Array} topics An array of topics to load the metadata for
 * @param {Client~loadMetadataForTopicsCallback} cb Function to call once all metadata is loaded
 */
Client.prototype.loadMetadataForTopics = function (topics, cb) {
    var correlationId = this.nextId();
    var request = protocol.encodeMetadataRequest(this.clientId, correlationId, topics);
    var broker = this.brokerForLeader();
    this.logger(util.format("loadMetadataForTopics topics %s broker %s (socketId %s)",
      topics.join(", "), (broker && broker.addr) || "N/A", (broker && broker.socketId.toString()) || "N/A"));
    if (!broker || broker.error) {
        var msg;
        if(broker && broker.error) {
            msg = util.format("Metadata loading for broker %s failed due to %s", broker.addr, broker.error);
        } else {
            msg = "Don't have any brokers to load metadata from";
        }
        this.logger(msg);
        return cb(new errors.BrokerNotAvailableError(msg));
    }

    this.queueCallback(broker, correlationId, [protocol.decodeMetadataResponse, cb]);
    broker.write(request);
};

Client.prototype.createTopics = function (topics, isAsync, cb) {
    topics = typeof topics === 'string' ? [topics] : topics;

    if (typeof isAsync === 'function' && typeof cb === 'undefined') {
        cb = isAsync;
        isAsync = true;
    }
    var self = this;

    // first, load metadata to create topics
    this.loadMetadataForTopics(topics, function (err, resp) {
        if (err) return cb(err);
        if (isAsync) return cb(null, 'All requests sent');
        var topicMetadata = resp[1].metadata;
        // ommit existed topics
        var existed = Object.keys(topicMetadata);
        var topicsNotExists = topics.filter(function (topic) {
            return !~existed.indexOf(topic);
        });

        function attemptCreateTopics (topics, cb) {
            var operation = retry.operation({ minTimeout: 200, maxTimeout: 2000 });
            operation.attempt(function(currentAttempt) {
                debug('create topics currentAttempt', currentAttempt);
                self.loadMetadataForTopics(topics, function (err, resp) {
                    if (resp) {
                        var topicMetadata = resp[1].metadata;
                        var created = Object.keys(topicMetadata).length === topics.length;
                        if (!created) err = new Error('Topic creation pending');
                    }
                    if (operation.retry(err)) {
                        return;
                    }

                    cb(err, 'All created');
                });
            });
        }

        if (!topicsNotExists.length) return cb(null, 'All created');

        debug('create topic by sending metadata request');
        attemptCreateTopics(topicsNotExists, cb);
    });
};

/**
 * Checks to see if a given array of topics exists
 *
 * @param {Array} topics An array of topic names to check
 *
 * @param {Client~topicExistsCallback} cb A function to call after all topics have been checked
 */
Client.prototype.topicExists = function (topics, cb) {
    var notExistsTopics = [];
    var self = this;

    async.each(topics, checkZK, function (err) {
      if (err) return cb(err);
      if (notExistsTopics.length) return cb(new errors.TopicsNotExistError(notExistsTopics));
      cb();
    });

    function checkZK (topic, cb) {
        self.zk.topicExists(topic, function (err, existed, topic) {
            if (err) return cb(err);
            if (!existed) notExistsTopics.push(topic);
            cb();
        });
    }
};

Client.prototype.addTopics = function (topics, cb) {
    var self = this;
    this.topicExists(topics, function (err) {
        if (err) return cb(err);
        self.loadMetadataForTopics(topics, function (err, resp) {
            if (err) return cb(err);
            self.updateMetadatas(resp);
            cb(null, topics);
        });
    });
};

Client.prototype.nextId = function () {
    return this.correlationId++;
};

Client.prototype.nextSocketId = function () {
    return this._socketId++;
};

Client.prototype.nextPartition = (function cycle() {
    var c = 0;
    return function (topic) {
        if (_.isEmpty(this.topicPartitions)) return 0;
        if (_.isEmpty(this.topicPartitions[topic])) return 0;
        return this.topicPartitions[topic][ c++ % this.topicPartitions[topic].length ];
    }
})();

Client.prototype.refreshBrokers = function (brokerMetadata) {
    var self = this;
    this.logger(util.format("refreshBrokers brokerMetadata %j, old metadata %j", brokerMetadata, this.brokerMetadata));
    this.brokerMetadata = brokerMetadata;
    deleteDeadBrokers(this.brokers);
    deleteDeadBrokers(this.longpollingBrokers);
    function deleteDeadBrokers (brokers) {
        Object.keys(brokers).filter(function (k) {
            return !~_.values(brokerMetadata).map(function (b) { return b.host + ':' + b.port }).indexOf(k);
        }).forEach(function (deadKey) {
            self.logger(util.format("refreshBrokers closing dead broker %s", deadKey));
            self.closeBrokers([brokers[deadKey]]);
            delete brokers[deadKey];
        }.bind(this));
    }
};

Client.prototype.refreshMetadata = function (topicNames, cb) {
    var self = this;
    if (!topicNames.length) return cb();
    attemptRequestMetadata(topicNames, cb);

    function attemptRequestMetadata (topics, cb) {
        var operation = retry.operation({ minTimeout: 200, maxTimeout: 1000 });
        operation.attempt(function(currentAttempt) {
            debug('refresh metadata currentAttempt', currentAttempt);
            self.loadMetadataForTopics(topics, function (err, resp) {
                err = err || resp[1].error;
                if (operation.retry(err)) {
                    return;
                }
                if (err) {
                    debug('refresh metadata error', err.message)
                    return cb(err);
                }
                self.updateMetadatas(resp);
                cb();
            });
        });
    }
};

Client.prototype.send = function (payloads, encoder, decoder, cb) {
    var self = this, _payloads = payloads;
    // payloads: [ [metadata exists], [metadta not exists] ]
    payloads = this.checkMetadatas(payloads);
    if (payloads[0].length && !payloads[1].length) {
        this.sendToBroker(_.flatten(payloads), encoder, decoder, cb);
        return;
    }
    if (payloads[1].length) {
        var topicNames = payloads[1].map(function (p) { return p.topic; });
        this.loadMetadataForTopics(topicNames, function (err, resp) {
            if (err) {
                return cb(err);
            }

            var error = resp[1].error;
            if (error) {
                return cb(error);
            }

            self.updateMetadatas(resp);
            // check payloads again
            payloads = self.checkMetadatas(_payloads);
            if (payloads[1].length) {
                return cb(new errors.BrokerNotAvailableError('Could not find the leader'));
            }

            self.sendToBroker(payloads[1].concat(payloads[0]), encoder, decoder, cb);
        });
    }
};

Client.prototype.sendToBroker = function (payloads, encoder, decoder, cb) {
    var longpolling = encoder.name === 'encodeFetchRequest';
    payloads = this.payloadsByLeader(payloads);
    if (!longpolling) {
        cb = wrap(payloads, cb);
    }
    for (var leader in payloads) {
        var correlationId = this.nextId();
        var request = encoder.call(null, this.clientId, correlationId, payloads[leader]);
        var broker = this.brokerForLeader(leader, longpolling);
        if (broker.error || broker.closing) {
            return cb(new errors.BrokerNotAvailableError('Could not find the leader'), payloads[leader]);
        }

        if (longpolling) {
            if (broker.waitting) continue;
            broker.waitting = true;
        }

        if (decoder.requireAcks == 0) {
            broker.write(request);
            cb(null, { result: 'no ack' });
        } else {
            this.queueCallback(broker, correlationId, [decoder, cb]);
            broker.write(request);
        }
    }
};

Client.prototype.checkMetadatas = function (payloads) {
    if (_.isEmpty(this.topicMetadata)) return [ [],payloads ];
    // out: [ [metadata exists], [metadta not exists] ]
    var out = [ [], [] ];
    payloads.forEach(function (p) {
        if (this.hasMetadata(p.topic, p.partition)) out[0].push(p)
        else out[1].push(p)
    }.bind(this));
    return out;
};

Client.prototype.hasMetadata = function (topic, partition) {
    var brokerMetadata = this.brokerMetadata,
        topicMetadata = this.topicMetadata;
    var leader = this.leaderByPartition(topic, partition);

    return (leader !== undefined) && brokerMetadata[leader];
};

Client.prototype.updateMetadatas = function (metadatas) {
    // _.extend(this.brokerMetadata, metadatas[0]);
    this.logger("updateMetadatas");

    _.extend(this.topicMetadata, metadatas[1].metadata);
    for(var topic in this.topicMetadata) {
        this.topicPartitions[topic] = Object.keys(this.topicMetadata[topic]).map(function(val) {
            return parseInt(val, 10);
        });
    }
};

Client.prototype.removeTopicMetadata = function (topics, cb) {
    topics.forEach(function (t) {
        if (this.topicMetadata[t]) delete this.topicMetadata[t];
    }.bind(this));
    cb(null, topics.length);
};

Client.prototype.payloadsByLeader = function (payloads) {
    return payloads.reduce(function (out, p) {
        var leader = this.leaderByPartition(p.topic, p.partition);
        out[leader] = out[leader] || [];
        out[leader].push(p);
        return out;
    }.bind(this), {});
};

Client.prototype.leaderByPartition = function (topic, partition) {
    var topicMetadata = this.topicMetadata;
    return topicMetadata[topic] && topicMetadata[topic][partition] && topicMetadata[topic][partition]['leader'];
};

Client.prototype.brokerForLeader = function (leader, longpolling) {
    var brokers = longpolling ? this.longpollingBrokers : this.brokers;
    // If leader is not give, choose the first broker as leader
    if (typeof leader === 'undefined') {
        if (!_.isEmpty(brokers)) {
            var addr = Object.keys(brokers)[0];
            return brokers[addr];
        } else if (!_.isEmpty(this.brokerMetadata)) {
            leader = Object.keys(this.brokerMetadata)[0];
        } else {
            this.logger("brokerForLeader no brokers found");
            this.emit('error', new errors.BrokerNotAvailableError('Could not find a broker'));
            return;
        }
    }
    var metadata = this.brokerMetadata[leader],
        addr = metadata.host + ':' + metadata.port;
    brokers[addr] = brokers[addr] || this.createBroker(metadata.host, metadata.port, longpolling);
    return brokers[addr];
};

Client.prototype.createBroker = function connect(host, port, longpolling) {
    var self = this;
    var socket = net.createConnection(port, host);
    socket.addr = host + ':' + port;
    socket.host = host;
    socket.port = port;
    socket.socketId = this.nextSocketId();
    if (longpolling) socket.longpolling = true;
    self.logger("Creating broker at " + socket.addr + " socketId " + socket.socketId + " longpolling " + !!longpolling);
    socket.on('connect', function () {
        this.error = null;
        self.logger(("socketId " + this.socketId  +  " connected to " + this.addr));
        self.emit('connect');
    });
    socket.on('error', function (err) {
        this.error = err;
        self.logger(util.format("connection to %s socketId %s emitted error: %s", this.addr, this.socketId, err));
        self.emit('error', err);
    });
    socket.on('close', function (had_error) {
        self.emit('close', this);
        self.logger(util.format("connection to %s socketId %s closed. had_error %s, this.error %s",
          this.addr, this.socketId, had_error, this.error));
        if (had_error) {
            self.clearCallbackQueue(this, this.error);
        }
        else {
            self.clearCallbackQueue(this, new errors.BrokerNotAvailableError('Broker not available'));
        }
        retry(this);
    });
    socket.on('end', function () {
        self.logger("socketId " + this.socketId + " emitted 'end', retrying");
        retry(this);
    });
    socket.buffer = new Buffer([]);
    socket.on('data', function (data) {
        this.buffer = Buffer.concat([this.buffer, data]);
        self.handleReceivedData(this);
    });
    socket.setKeepAlive(true, 60000);

    function retry(s) {
        if(s.retrying || s.closing) return;
        self.logger("retry connection to " + s.addr + " socketId " + s.socketId);
        s.retrying = true;
        s.retryTimer = setTimeout(function () {
            s.retrying = false;
            self.logger(util.format("retry %s socketId %s calling connect(%d, %s)", s.addr, s.socketId, s.port, s.host));
            s.connect(s.port, s.host);
        }, 500);
    }
    return socket;
};

Client.prototype.handleReceivedData = function (socket) {
    var vars = Binary.parse(socket.buffer).word32bu('size').word32bu('correlationId').vars,
        size = vars.size + 4,
        correlationId = vars.correlationId;
    if (socket.buffer.length >= size) {
        var resp = socket.buffer.slice(0, size);
        var handlers = this.unqueueCallback(socket, correlationId);

        if (!handlers) return;
        var decoder = handlers[0];
        var cb = handlers[1];
        var result = decoder(resp);
        (result instanceof Error)
            ? cb.call(this, result)
            : cb.call(this, null, result);
        socket.buffer = socket.buffer.slice(size);
        if (socket.longpolling) socket.waitting = false;
    } else { return }

    if (socket.buffer.length)
        setImmediate(function () { this.handleReceivedData(socket);}.bind(this));
};

Client.prototype.queueCallback = function (broker, id, data) {
    var brokerId = broker.socketId;
    var queue;

    if (this.cbqueue.hasOwnProperty(brokerId)) {
        queue = this.cbqueue[brokerId];
    }
    else {
        queue = {};
        this.cbqueue[brokerId] = queue;
    }

    queue[id] = data;
};

Client.prototype.unqueueCallback = function (broker, id) {
    var brokerId = broker.socketId;

    if (!this.cbqueue.hasOwnProperty(brokerId)) {
        this.logger(util.format("unqueueCallback couldn't find brokerId %s in queue (correlationId %s)", brokerId, id));
        return null;
    }

    var queue = this.cbqueue[brokerId];
    if (!queue.hasOwnProperty(id)) {
        this.logger(util.format("unqueueCallback brokerId %s did not have correlationId %s queued", brokerId, id));
        return null;
    }

    var result = queue[id];

    // cleanup socket queue
    delete queue[id];
    if (!Object.keys(queue).length) {
        delete this.cbqueue[brokerId];
    }

    return result;
};

Client.prototype.clearCallbackQueue = function (broker, error) {
    var brokerId = broker.socketId;
    var longpolling = broker.longpolling;

    this.logger("clearCallbackQueue brokerId/socketId " + brokerId + " longpolling " + !!longpolling + " error " + error);

    if (!this.cbqueue.hasOwnProperty(brokerId)) {
        this.logger("No callbacks queued");
        return;
    }

    var queue = this.cbqueue[brokerId];
    this.logger(queue.length||0 + " callbacks queued");
    if (!longpolling) {
        Object.keys(queue).forEach(function (key) {
            var handlers = queue[key];
            var cb = handlers[1];
            cb(error);
        });
    }

    delete this.cbqueue[brokerId];
};

module.exports = Client;
