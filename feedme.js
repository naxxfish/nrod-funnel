#!/usr/bin/env node

var config = require('./config')
var debug = require('debug')('trainmon-main')
var tdDebug = require('debug')('trainmon-td')
var tdSDebug = require('debug')('trainmon-tds')
var TRUSTDebug = require('debug')('trainmon-trust')
var sys = require('util');
var stomp = require('stomp-client');
var chalk = require('chalk');
var moment = require('moment')
var tdLookup = require('./tdlookup')
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
		client.subscribe('/topic/' + config.tdChannel, td_message_callback) 
		client.subscribe('/topic/' + config.movementChannel, movements_message_callback);
		console.log(chalk.yellow('Connected session ' + sessionId))
	});

	function td_message_callback(body, headers) {
		numMessages++
		numMessagesSinceLast++
		messages = JSON.parse(body)
		messages.forEach(function (message)
		{
			numTDMessages++
			numTDMessagesSinceLast++
			switch (Object.keys(message)[0])
			{
				case 'CA_MSG':
					// Berth Step
					processC_MSG('CA',message.CA_MSG)
					break;
				case 'CB_MSG':
					// Berth Cancel
					processC_MSG('CB',message.CB_MSG)
					break;
				case 'CC_MSG':
					// Berth Interpose
					processC_MSG('CC',message.CC_MSG)
					break;
				case 'CT_MSG':
					// Heartbeat
					processC_MSG('CT',message.CT_MSG)
					break;				
				case 'SF_MSG':
					// Signalling Update
					processS_MSG('SF',message.SF_MSG)
					break;
				case 'SG_MSG':
					// Signalling Refresh
					break;
				case 'SH_MSG':
					// Signalling Refresh Finished	
					break;
				default:
					console.log("unknown message: " + message[0])	
			}
		});
	}

	function processS_MSG(msgType, message)
	{
		var signals = db.collection('SIGNALS')
		switch(msgType)
		{
			case 'SF':
				var dp = {
					'area.id': message.area_id,
					'area.name': tdLookup[message.area_id]
				}
				dp['memory.'+message.address] = message.data
				signals.update({'area.id': message.area_id}, 
				{$set: dp }, 
				{upsert:true},function (err, update) {
					tdSDebug('S Class', 'SF Update ' + message.address + '(area ' + message.area_id + ') to ' + message.data)
				})
				break;
			case 'SG':
				// err, dunno
				break;
			case 'SH':
				// errr, dunoo?
				break;
			default:
				tdSDebug('S Class', 'Unknown')
		}
	}

	function processC_MSG(msgType, message)
	{
		//debug('processCA_MSG', message)
		
		var smart = db.collection('SMART')
		var corpus = db.collection('CORPUS')
		var berths = db.collection('BERTHS')
		// Berth tracking
		switch(msgType)
		{
			case "CA":
				berths.update({'berth': message.to}, {$push: {'describers': message.descr}}, {upsert:true}, function (err, update) {
					tdDebug('Moved IN ' + message.descr + ' into berth ' + message.to)
				})
				berths.update({'berth': message.from}, {$pull: {'describers': message.descr}}, function (err,update) {
					tdDebug('Moved OUT ' + message.descr + ' from berth ' + message.from)
				})
				break;
			case "CB":
				berths.update({'berth': message.from}, {$pull: {'describers': message.descr}}, function (err,update) {
					tdDebug('Cancelled ' + message.descr + ' from berth ' + message.from)
				})
				break;
			case "CC":
				berths.update({'berth': message.to}, {$set: {'describers': [ message.descr ]}}, {upsert:true}, function (err,update) {
					tdDebug('Interposed ' + message.descr + ' into berth ' + message.to)
				})
		}
		smart.findOne({'FROMBERTH': message.from, 'TOBERTH': message.to, 'STEPTYPE': 'B'}, function (err, berth) {
			if (berth != null)
			{
				corpus.findOne({'STANOX': berth.STANOX}, function (err, stanox) {
					if (stanox != null) 
					{
						var reference = db.collection('REFERENCE')
						reference.findOne({'TIPLOC': stanox.TIPLOC, 'refType': 'GeographicData'}, function (err, location)
						{
							var record = {
									'currentBerth': message.to,
									'tdActive': true,
									'lastSeen': { 
										'td': message, 
										'berth': berth,
										'stanox': stanox,
										'location': location
									},
									'lastUpdate': moment().unix()
								}
							var trains = db.collection('TRAINS')
							trains.update({'descr': message.descr} , {$set: record } , {upsert: true}, function (error, myRecord) {
								debug('TD Update', message.descr, record.lastSeen.td.to, record.lastSeen.td.from)
							})
						})	
					} else {
						//console.log("No valid stanox!")
					}
				})
			}
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
			switch(message['header']['msg_type'])
			{
				case '0001':
					// Train Activation
					movements_activation(message['body'], message['header'])
					break;
				case '0002':
					// Train cancellation
					movements_cancellation(message['body'], message['header'])
				case '0003':
					// Train Movement
					movements_movement(message['body'], message['header'])
					break;
				case '0004':
					console.error("Unidentified train")
					break;
				case '0005':
					// Train Reinstatement
					movements_reinstatement(message['body'], message['header'])
					break;
				case '0006':
					// Change of Origin
					movements_coo(message['body'], message['header'])
					break;
				case '0007':
					// Change of Identity
					movements_coi(message['body'], message['header'])
					break;
				default:
					console.error("Unknown Movement message")
			}
		})
	}
	
	function movements_movement(body, header)
	{
		var trains  = db.collection('TRAINS')
		var trainDescr = body.train_id.substring(2,6)

		var record = {
			$set: {
				'trustID': body.train_id,
				'descr': trainDescr,
				'movementActive': true,
				'lastMovement': body,
				'lastUpdate': moment().unix()
			}
		}
		trains.update({'trustID': body.train_id}, record, {upsert:true}, function () {
			debug("TRUST movement",  body.train_id,  "STANOX (" + body.loc_stanox + ")")
		})
	}
		
	function movements_activation(body, header)
	{
		var trains = db.collection('TRAINS')
		var trainDescr = body.train_id.substring(2,6)
		var schedule = db.collection('SCHEDULE')
		schedule.findOne({
			'CIF_train_uid': body.train_uid,
			"schedule_end_date": body.schedule_end_date,
			"schedule_start_date": body.schedule_start_date
		}, function (error, record) 
		{
			var scheduleActive = false
			if (record != null)
			{
				scheduleActive = true
			}
			var record = {
				$set: {
					'trustActivated': true,
					'trustID': body.train_id,
					'movementActivation': body,
					'schedule': record,
					'scheduleActive': scheduleActive,
					'lastUpdate': moment().unix()
				}
			}
			trains.update({'descr': trainDescr} , record, {upsert: true}, function (error, record) {
				debug('TRUST activation', body.train_id, error, record)
			})
		})
	}
	
	function movements_cancellation (body, header) 
	{
		var trains  = db.collection('TRAINS')	
		var record = {
			$set: {
				'lastMovement': body,
				'lastUpdate': moment().unix()
			}
		}
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('TRUST cancellation',body.train_id, error, record)
		})
	}
	
	function movements_reinstatement (body, header) 
	{
		var trains  = db.collection('TRAINS')
		var record = {
			$set: {
				'lastMovement': body,
				'lastUpdate': moment().unix()
			}
		}
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('TRUST reinstatement ',body.train_id,error, record)
		})
	}

	function movements_coo (body, header) 
	{
		var trains  = db.collection('TRAINS')
		var record = {
			$set: {
				'coo': body,
				'lastUpdate': moment().unix()
			}
		}
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('TRUST Change of Origin', body.train_id, error, record)
		})
	}
	
	function movements_coi (body, header) 
	{
		var trains  = db.collection('TRAINS')
		var record = {
			$set: {
				'coi': body,
				'lastUpdate': moment().unix()
			}
		}
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('TRUST Change of Identity', body.train_id, error, record)
		})
	}

	
})

