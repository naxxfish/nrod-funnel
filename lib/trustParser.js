var debug = require('debug')('nrod-trust')
var moment = require('moment')
var config = require('../config')

var db = null

exports.parse = function (indb, message) {
	db = indb
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


