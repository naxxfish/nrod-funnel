#!/usr/bin/env node

var config = require('./config')

var debug = require('debug')('nrod-main')

var VSTPDebug = require('debug')('nrod-vstp')
var tdParser = require('./lib/tdParser')
var trustParser = require('./lib/trustParser')

var sys = require('util');
var stomp = require('stomp-client');
var chalk = require('chalk');
var moment = require('moment')

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

	var client = new stomp(config.stompHost, config.stompPort, config.username, config.password);

	client.on('error', function(error_frame) {
		console.log(error_frame.body);
		client.disconnect();
	});

	process.on('SIGINT', function() {
		console.log(chalk.green('\nConsumed ' + numMessages + ' message batches, TD messages: ' + numTDMessages + ', TRUST messages: ' + numTRUSTMessages));
		client.disconnect();
		process.exit();
	});

	client.connect(function (sessionId) {
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
			client.subscribe('/topic/' + config.tdChannel, td_message_callback)
		}
		if (config.feeds.TRUST == true)
		{
			client.subscribe('/topic/' + config.movementChannel, movements_message_callback);
		}
		if (config.feeds.VSTP == true)
		{
			client.subscribe('/topic/VSTP_ALL', vstp_message_callback)
		}
		console.log(chalk.yellow('Connected session ' + sessionId))
	});

	function vstp_message_callback(body, headers) {
		numMessages++
		numMessagesSinceLast++
		var message = JSON.parse(body)['VSTPCIFMsgV1']
		var schedule = message['schedule']
		VSTPDebug('VSTPFeed',schedule, schedule['schedule_segment'])
	}

	function td_message_callback(body, headers) {
		numMessages++
		numMessagesSinceLast++
		messages = JSON.parse(body)
		messages.forEach(function (message)
		{
			numTDMessages++
			numTDMessagesSinceLast++
			tdParser.parse(db, message)
		})
	}

	function movements_message_callback(body, headers)
	{
		numMessages++
		numMessagesSinceLast++
		messages = JSON.parse(body)
		messages.forEach(function (message) {
			numTRUSTMessages++
			numTRUSTMessagesSinceLast++
			trustParser.parse(db, message)
		})
	}
})

