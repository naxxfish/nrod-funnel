#!/usr/bin/env node

var config = require('./config')
var debug = require('debug')('trainmon-main')
var sys = require('util');
var stomp = require('stomp');
var JSONStream = require('JSONStream')
var chalk = require('chalk');

var MongoClient = require('mongodb').MongoClient;

debug('main',config.securityToken)

var numMessages = 0;

var sys = require('util');

// 'activemq.prefetchSize' is optional.
// Specified number will 'fetch' that many messages
// and dump it to the client.
var tdHeaders = {
    destination: '/topic/' + config.tdChannel,
    ack: 'client'
//    'activemq.prefetchSize': '10'
};

var movementHeaders = {
	destination: '/topic/' + config.movementChannel,
	ack: 'client'
};

var messages = 0;
var MongoClient = require('mongodb').MongoClient;

debug('main',config.securityToken)

MongoClient.connect(config.mongo.connectionString, function (err, db) 
{
	if (err)
	{
		console.log("Error connecting to DB: " + err)
		return
	}
	var stomp = require('stomp');

	// Set debug to true for more verbose output.
	// login and passcode are optional (required by rabbitMQ)
	var stomp_args = {
		port: config.stompPort,
		host: config.stompHost,
		debug: false,
		login: config.username,
		passcode: config.password,
	};
	debug('main',config)

	var client = new stomp.Stomp(stomp_args);

	client.connect();

	function td_message_callback(body, headers) {
		//console.log('Message Callback Fired!');
		//console.log('Headers: ' + sys.inspect(headers));
		messages = JSON.parse(body)
		//debug('message_callback',messages)
		for (var i=0;i<messages.length;i++)
		{
			message = messages[i]
			// debug('switchmessage',message)
			switch (Object.keys(message)[0])
			{
				case 'CA_MSG':
					//console.log("Got CA message!")
					processC_MSG(message.CA_MSG)
					break;
				case 'CB_MSG':
					//console.log("Got CB message!")
					processC_MSG(message.CB_MSG)
					break;
				case 'CC_MSG':
					//console.log("Got CC message!")
					//processC_MSG(message.CC_MSG)
					break;
				case 'CT_MSG':
					//console.log("Got CT message!")
					//processC_MSG(message.CT_MSG)
					break;				
				case 'SF_MSG':
					//console.log("Got SF message!")
					//processCA_MSG(message)
					//processCA_MSG(message)
					break;
				case 'SG_MSG':
					//console.log("Got SG message!")
					//processCA_MSG(message)
					break;
				case 'SH_MSG':
					//console.log("Got SH message!")
					//processCA_MSG(message)
					break;

					default:
					console.log("unknown message: " + message[0])
					
			}
		}
	}

	function processC_MSG(message)
	{
		//debug('processCA_MSG', message)
		
		var smart = db.collection('SMART')
		var corpus = db.collection('CORPUS')
		// debug('processC_MSG', smart)
		//debug('processC_MSG',{'FROMBERTH': message.from, 'TOBERTH': message.to})
		smart.findOne({'FROMBERTH': message.from, 'TOBERTH': message.to, 'STEPTYPE': 'B'}, function (err, berth) {
			//debug('processC_MSG.matchingBerths',berth)
			if (berth != null)
			{
				corpus.findOne({'STANOX': berth.STANOX}, function (err, stanox) {
					if (stanox != null) 
					{
						//debug('processC_MSG.matchingStanox', stanox)
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
									}
								}
							// debug('processC_MSG', record)
							var trains = db.collection('TRAINS')
							trains.update({'descr': message.descr} , {$set: record } , {upsert: true}, function (error, record) {
								debug('Train update',record)
							})
							//if (
							//	record.lastSeen.location.northing > 536000 && 
							//	record.lastSeen.location.northing < 539000 &&
							//	record.lastSeen.location.easting > 175600 && 
							//	record.lastSeen.location.easting < 178000
							//	)
							//	{

									
									debug('trainNearLewisham', record)
									//console.log("TRAIN NEAR LEWISHAM!!!")
									//console.log('\u0007\u0007');
									if ((record['lastSeen']['location']['TIPLOC'] == "LEWISHM" &&
										(record['lastSeen']['berth']['EVENT'] == "B" || record['lastSeen']['berth']['EVENT'] == "C")))
									{
										console.log()
										console.log(chalk.red("Train going past flat!"))
										console.log("\u0007")
										console.log("Train " + record['descr'] + " has moved from berth " + record['lastSeen']['berth']['FROMBERTH'] + " to berth " + record['lastSeen']['berth']['TOBERTH'])
										console.log("At " + record['lastSeen']['location']['locationName'] + " platform " + record['lastSeen']['berth']['PLATFORM'])
										console.log("SMART Berth detail: " + JSON.stringify(record['lastSeen']['berth'],{}, true))
										console.log("CORPUS detail: " + JSON.stringify(record['lastSeen']['location'],{},true))
										switch (record['lastSeen']['berth']['EVENT'])
										{
											case 'A':
												console.log("Arrived in the 'up' directon")
												break;
											case 'B':
												console.log("Departure in the 'up' direction")
												break;
											case 'C':
												console.log("Arrival in the 'down' direction")
												break;
											case 'D':
												console.log("Departure in the 'down' direction")
												break;
											default:
												console.log("Unknown event!")
										}
									}
								//}
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
		//console.log('Message Callback Fired!');
		//console.log('Headers: ' + sys.inspect(headers));
		messages = JSON.parse(body)
		//console.log("Movement Messages:")
		messages.forEach(function (message) {
			//console.log("Message")
			switch(message['header']['msg_type'])
			{
				case '0001':
					//console.log("Train Activation");
					movements_activation(message['body'], message['header'])
					break;
				case '0002':
					//console.log("Train cancellation")
					movements_cancellation(message['body'], message['header'])
				case '0003':
					//console.log("Train Movement");
					movements_movement(message['body'], message['header'])
					break;
				case '0004':
					console.error("Unidentified train")
					break;
				case '0005':
					console.log("Train Reinstatement")
					movements_reinstatement(message['body'], message['header'])
					break;
				case '0006':
					console.log("Change of Origin")
					movements_coo(message['body'], message['header'])
					break;
				case '0007':
					console.log("Change of Identity")
					movements_coi(message['body'], message['header'])
					break;
				default:
					console.error("Unknown Movement message")
			}
			//console.log(item)
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
				'lastMovement': body
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
			'CIF_train_uid': body.train_uid
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
					'scheduleActive': scheduleActive
				}
			}
			debug("TRUST activation", trainDescr + " TRUST ID (" + body.train_id + ")")
			trains.update({'descr': trainDescr} , record, {upsert: true}, function (error, record) {
				debug('trainUpdate', error, record)
			})
		})
	}
	
	function movements_cancellation (body, header) 
	{
		var trains  = db.collection('TRAINS')	
		var record = {
			$set: {
				'lastMovement': body
			}
		}
		debug("Train cancellation: TRUST ID " + body.train_id)
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('trainUpdate',error, record)
		})
	}
	
	function movements_reinstatement (body, header) 
	{
		var trains  = db.collection('TRAINS')
		var record = {
			$set: {
				'lastMovement': body
			}
		}
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('trainUpdate',error, record)
		})
	}

	function movements_coo (body, header) 
	{
		var trains  = db.collection('TRAINS')
		var record = {
			$set: {
				'coo': body
			}
		}
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('trainUpdate',error, record)
		})
	}
	
	function movements_coi (body, header) 
	{
		var trains  = db.collection('TRAINS')
		var record = {
			$set: {
				'coi': body
			}
		}
		trains.update({'trustID': body.train_id},record, function (error, record) {
			debug('trainUpdate',error, record)
		})
	}

	
	client.on('connected', function() {
		debug('client.on.connected')
		client.subscribe(tdHeaders, td_message_callback);
		client.subscribe(movementHeaders, movements_message_callback);
		console.log(chalk.yellow('Connected'))
	});

	client.on('message', function(message) {
		//console.log("HEADERS: " + sys.inspect(message.headers));
		//console.log("BODY: " + message.body);
		//console.log("Got message: " + message.headers['message-id']);
		client.ack(message.headers['message-id']);
		numMessages++;
	});

	client.on('error', function(error_frame) {
		console.log(error_frame.body);
		client.disconnect();
	});
	process.on('SIGINT', function() {
		console.log('\nConsumed ' + numMessages + ' messages');
		client.disconnect();
		process.exit();
	});

})

