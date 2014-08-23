var debug = require('debug')('nrod-td')
var moment = require('moment')
var config = require('../config')

var tdLookup = require('./tdlookup')

var db = null

exports.parse = function (indb, message)
{
	db = indb
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
}

function processS_MSG(msgType, message)
{
	if (config.feeds.TD.indexOf('S') == -1)
	{
		debug('processS_MSG', 'S class messages disabled')
		return
	}
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
				debug('S Class', 'SF Update ' + message.address + '(area ' + message.area_id + ') to ' + message.data)
			})
			break;
		case 'SG':
			// err, dunno
			break;
		case 'SH':
			// errr, dunoo?
			break;
		default:
			debug('S Class', 'Unknown')
	}
}

function processC_MSG(msgType, message)
{
	if (config.feeds.TD.indexOf('C') == -1)
	{
		debug('processS_MSG', 'C class messages disabled')
		return
	}
	//debug('processCA_MSG', message)
	var smart = db.collection('SMART')
	var corpus = db.collection('CORPUS')
	var berths = db.collection('BERTHS')
	// Berth tracking
	switch(msgType)
	{
		case "CA":
			berths.update({'berth': message.to}, {$push: {'describers': message.descr}}, {upsert:true}, function (err, update) {
				debug('Moved IN ' + message.descr + ' into berth ' + message.to)
			})
			berths.update({'berth': message.from}, {$pull: {'describers': message.descr}}, function (err,update) {
				debug('Moved OUT ' + message.descr + ' from berth ' + message.from)
			})
			break;
		case "CB":
			berths.update({'berth': message.from}, {$pull: {'describers': message.descr}}, function (err,update) {
				debug('Cancelled ' + message.descr + ' from berth ' + message.from)
			})
			break;
		case "CC":
			berths.update({'berth': message.to}, {$set: {'describers': [ message.descr ]}}, {upsert:true}, function (err,update) {
				debug('Interposed ' + message.descr + ' into berth ' + message.to)
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
