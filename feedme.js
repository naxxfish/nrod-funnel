#!/usr/bin/env node

const bunyan = require('bunyan');
var config = require('./config')
const statware = require('statware');
var VSTPDebug = require('debug')('nrod-vstp')
var tdParser = require('./lib/tdParser')
var trustParser = require('./lib/trustParser')
var vstpParser = require('./lib/vstpParser')

var sys = require('util');
var stompit = require('stompit');
var chalk = require('chalk');
var moment = require('moment')
var makeSource = require("stream-json");

var Parser = require("stream-json/Parser");
var Streamer = require("stream-json/Streamer");
var Assembler = require("stream-json/utils/Assembler");

var MongoClient = require('mongodb').MongoClient;

var numMessages = 0;
var numMessagesSinceLast = 0;
var numTDMessages = 0;
var numTRUSTMessages = 0;
var numTDMessagesSinceLast = 0;
var numTRUSTMessagesSinceLast = 0;

var MongoClient = require('mongodb').MongoClient;
var lastUpdateTime = moment()
var updateI = 0
var log = bunyan.createLogger({
    name: 'nrod-feed'
});

log.info({
    'securityToken': config.securityToken
})

var stats = statware()

stats.installProcessInfo()
stats.installSystemInfo()

MongoClient.connect(config.mongo.connectionString, function(err, db) {
    if (err) {
        log.fatal("Error connecting to DB: " + err)
        return
    }
    var manager = new stompit.ConnectFailover(
        [{
            'host': config.stompHost,
            'port': config.stompPort,
            'connectHeaders': {
                'heart-beat': '5000,5000',
                'host': config.stompHost,
                'login': config.username,
                'passcode': config.password
            },
            'useExponentialBackOff': true
        }], {
            'maxReconnects': 10
        })
    manager.connect(function(error, client, reconnect) {
        if (error) {
            console.fatal('Terminal problem connecting to STOMP host ' + error)
            return
        }
        client.on('error', function(error_frame) {
            log.error({
                'stompError': error_frame.body
            });
            reconnect()
        });
        log.info('client.on.connected')
        setInterval(() => {
            stats.getStats((metrics) => {


                log.info({
                    'timestamp': moment(),
                    'numMessages': metrics.numMessages,
                    'numTdMessages': metrics.numTDMessages,
                    'numTRUSTMessages': metrics.numTRUSTMessages,
                })
            })
        }, 10000)
        if (config.feeds.TD != undefined) {
            client.subscribe({
                destination: '/topic/' + config.tdChannel,
                ack: 'auto'
            }, (error, msg) => {
                message_callback_wrap(error, msg, td_message_callback)
            })
        }
        if (config.feeds.TRUST == true) {
            client.subscribe({
                destination: '/topic/' + config.movementChannel,
                ack: 'auto'
            }, (error, msg) => {
                message_callback_wrap(error, msg, movements_message_callback)
            });
        }
        if (config.feeds.VSTP == true) {
            client.subscribe({
                destination: '/topic/VSTP_ALL',
                ack: 'auto'
            }, (error, msg) => {
                message_callback_wrap(error, msg, vstp_message_callback)
            })
        }
        log.info('Connected to STOMP server')
    })

    process.on('SIGINT', function() {
        log.info({
            'state': 'exiting',
            'totalMsgs': numMessages,
            'tdMessages': numTDMessages,
            'trustMessages': numTRUSTMessages
        })
        process.exit();
    });

    function message_callback_wrap(error, msg, cb) {
        stats.increment('numMessages')
        return cb(error, msg)
    }

    function vstp_message_callback(error, msg) {

        var source = makeSource();
        var assembler = new Assembler();
        if (msg == undefined) {
            return;
        }
        msg.pipe(source.input)
        source.output.on('data', (message) => {
            log.debug('vstp_message', message)
            stats.increment('numVSTPMessages')
            //vstpParser.parse(message['VSTPCIFMsgV1'])
        })
    }

    function td_message_callback(error, msg) {
        var assembler = new Assembler();
        var source = makeSource();
        if (msg == undefined) {
            return
        }
        msg.pipe(source.input)
        source.output.on('data', (chunk) => {
            assembler[chunk.name] && assembler[chunk.name](chunk.value);
        }).
        on('end', function() {
            var messages = assembler.current
            log.debug('td_message', messages)
            messages.forEach((message) => {
                stats.increment('numTDMessages')
                tdParser.parse(db, message)
            })
        })
    }

    function movements_message_callback(error, msg) {
        var assembler = new Assembler();
        var source = makeSource();
        if (msg == undefined) {
            return;
        }
        msg.pipe(source.input)
        source.output.on('data', (chunk) => {
            assembler[chunk.name] && assembler[chunk.name](chunk.value);
        }).on('end', function() {
            var messages = assembler.current
            log.debug('movements_message', assembler.current)
            messages.forEach((message) => {
                stats.increment('numTRUSTMessages')
                trustParser.parse(db, message)
            })
        })
    }
})
