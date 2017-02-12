#!/usr/bin/env node

var config = require('./config')

var debug = require('debug')('nrod-main')

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
var Assembler  = require("stream-json/utils/Assembler");

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
debug('main',config.securityToken)

MongoClient.connect(config.mongo.connectionString, function (err, db)
{
	if (err)
	{
		console.log("Error connecting to DB: " + err)
		return
	}
   var manager = new stompit.ConnectFailover(
      [
         {
            'host': config.stompHost,
            'port': config.stompPort,
            'connectHeaders': {
               'heart-beat':'5000,5000',
               'host': config.stompHost,
               'login': config.username,
               'passcode': config.password
            },
            'useExponentialBackOff': true
         }
      ],
      {
      'maxReconnects': 10
   })
   manager.connect (function (error, client, reconnect) {
      if (error)
      {
         console.error('Terminal problem connecting to STOMP host ' + error)
         return
      }
      client.on('error', function(error_frame) {
   		console.error(error_frame.body);
         reconnect()
   	});
		debug('client.on.connected')
		setInterval(function ()
		{
			var timeSinceLast = (moment().diff(lastUpdateTime))/1000
			lastUpdateTime = moment()
			if (updateI % 10 == 0)
			{
				// print headers
				console.log('Batches (total/per sec)\tTD (total /per sec)\tTRUST (total/per sec)');
			}
			console.log(numMessages + '/' +
				Math.round(numMessagesSinceLast / timeSinceLast) + '\t\t\t' +
				numTDMessages + '/' +
				Math.round(numTDMessagesSinceLast / timeSinceLast) + '\t\t\t' +
				numTRUSTMessages + '/' +
				Math.round(numTRUSTMessagesSinceLast / timeSinceLast));
			updateI++
			numTRUSTMessagesSinceLast = 0
			numTDMessagesSinceLast = 0
			numMessagesSinceLast = 0

		}, 5000)
		if (config.feeds.TD != undefined)
		{
			client.subscribe({destination: '/topic/' + config.tdChannel, ack: 'auto'} , td_message_callback)
		}
		if (config.feeds.TRUST == true)
		{
			client.subscribe({destination: '/topic/' + config.movementChannel, ack: 'auto'} , movements_message_callback);
		}
		if (config.feeds.VSTP == true)
		{
			client.subscribe({destination: '/topic/VSTP_ALL', ack: 'auto'}, vstp_message_callback)
		}
		console.log(chalk.yellow('Connected session '))
   })

	process.on('SIGINT', function() {
		console.log(chalk.green('\nConsumed ' + numMessages + ' message batches, TD messages: ' + numTDMessages + ', TRUST messages: ' + numTRUSTMessages));
		process.exit();
	});

	function vstp_message_callback(error, msg) {
		numMessages++
		numMessagesSinceLast++
      var source = makeSource();
      var assembler = new Assembler();
      msg.pipe(source.input)
         source.output.on('data', function (message) {
         debug('vstp_message',message)
         //vstpParser.parse(message['VSTPCIFMsgV1'])
      })
	}

	function td_message_callback(error, msg) {
		numMessages++
		numMessagesSinceLast++
      var assembler = new Assembler();
      var source = makeSource();
      msg.pipe(source.input)
      source.output.on('data', function (chunk) {
         assembler[chunk.name] && assembler[chunk.name](chunk.value);
      }).
      on('end', function () {
         var messages = assembler.current
         debug('td_message',messages)
   		messages.forEach(function (message)
   		{
   			numTDMessages++
   			numTDMessagesSinceLast++
   			tdParser.parse(db, message)
   		})
      })
	}

	function movements_message_callback(error, msg)
	{
		numMessages++
		numMessagesSinceLast++
      var assembler = new Assembler();
      var source = makeSource();
      msg.pipe(source.input)
      source.output.on('data', function (chunk) {
         assembler[chunk.name] && assembler[chunk.name](chunk.value);
      }).on('end', function () {
         var messages = assembler.current
         debug('movements_message',assembler.current)
   		messages.forEach(function (message) {
   			numTRUSTMessages++
   			numTRUSTMessagesSinceLast++
   			trustParser.parse(db, message)
   		})
      })
	}
})
