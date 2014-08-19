#!/usr/bin/env node

var config = require('./config')
var debug = require('debug')('trainmon-installer')
var sys = require('util');
var readline = require('readline')
var zlib = require('zlib')
var JSONStream = require('JSONStream')
var csv = require('csv-stream')
var es = require('event-stream') 
var request = require('request')
var chalk = require('chalk')

// Retrieve
var MongoClient = require('mongodb').MongoClient;

function main() {
debug('main',config.securityToken)
MongoClient.connect(config.mongo.connectionString, function (err, db) {
	if (err)
	{
		console.log("Error: " + err)
		return
	}
	var referenceDataFilename = '20140116_ReferenceData.gz'
	console.log("Initialising datasets")
	console.log("Importing reference data from " + referenceDataFilename)
	importReferenceData(db, referenceDataFilename , function () {
		console.log(chalk.green("Reference Data imported"))	
			importSMART(db, function() {
			console.log("SMART data imported")
			importCORPUS(db , function () {
				console.log(chalk.green("CORPUS data imported"))
				console.log("Importing initial (full) SCHEDULE data")
				importSchedule(db, {update: false}, function (){
					console.log(chalk.green("Schedule data imported!"))
					db.close()
					process.exit()
				})
			})
		})
	})
})
}
if (require.main === module)
{
	main()
}
function importSMART(db, cb)
{
	debug('importSMART')
	var fs = require('fs')
	var gzip = zlib.createGunzip()
	var getUrl = "http://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=SMART"
	request(
	{
		uri: getUrl, 
		method: "GET",
		gzip: true,
		auth: {
			'username': config.username, 
			'pass': config.password
		},
		followRedirect: true
	}, function (error, response, body) {
		if (error)
		{
			console.error(error)
			return
		}
		var dataURI = response.request.uri.href
		var dataStream = request(
		{
			uri: dataURI, 
			method: "GET",
			gzip: true,
			followRedirect: true
		}, function (err, response, body) {
			if (err)
			{
				console.error(err)
			}
		})
		var scheduleItems = 0
		var associationItems = 0
		var tiplocItems = 0
		var records = 0
		
		var jstream = JSONStream.parse("BERTHDATA.*")
		function anno ( items )
		{
			console.log("Written " + items + " SMART Data rows")
		}
		var rows = 0 

		db.collection('SMART', function (err, collection ) {
			collection.ensureIndex({'FROMBERTH':1, 'TOBERTH':1}, {}, function () {
				console.log("Index created")
			 })
			var documents = []
			dataStream.pipe(gzip).pipe(jstream).pipe(es.mapSync(function (data) 
			{
				rows++
				data.type = 'SMART'
				//console.log(data)
				documents.push(data)
//				debug('insertDocuments','documents.length',documents.length )
				if (documents.length >= 500)
				{
					debug('insertDocuments','inserting batch of documents', documents.length)
					collection.insert( documents , {w:1}, function (err, records) { 
						if (err)
						{
							console.error(err)
						}
						debug('insertDocuments', 'inserted', records.length, 'records')
						//anno(Object.keys(records)) 
					} )
					documents = []
				}
			})).on('end', function() {
				collection.insert( documents , {w:1}, function (err, records) { 
					if (err)
					{
						console.error(err)
					}
					debug('insertDocumentsFinally',records.length)
					anno(rows) 
					cb();
				} )
			})
		})	
	})
}

function importCORPUS(db, cb)
{
	debug('importCORPUS')
	var fs = require('fs')
	var gzip = zlib.createGunzip()
	var jstream = JSONStream.parse("TIPLOCDATA.*")
	var getUrl = "http://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS"
	request(
	{
		uri: getUrl, 
		method: "GET",
		gzip: true,
		auth: {
			'username': config.username, 
			'pass': config.password
		},
		followRedirect: true
	}, function (error, response, body) {
		var dataURI = response.request.uri.href

		function anno ( items )
		{
			console.log("Processed " + items + " CORPUS rows")
		}

		db.collection('CORPUS', function (err, collection)
		{		
			var rows= 0 
			var documents = []
			collection.ensureIndex({'STANOX':1}, {}, function() {
				console.log("Indexed on STANOX")
			})
			var dataStream = request(
			{
				uri: dataURI,
				method: "GET",
				gzip: true,
				followRedirect: false
			}).pipe(gzip)

			dataStream.pipe(jstream).pipe(es.mapSync(function (doc) {
				rows++
				doc.type = 'CORPUS'
				documents.push(doc)
				if (documents.length >= 500)
				{
					debug('insertDocuments', 'inserting batch of CORPUS records', documents.length)
					collection.insert( documents , function (error, records) {
						anno(records.length)
					})
					documents = []
				}
			})).on('end', function () {
				collection.insert(documents, {w:1}, function (err, records) {
					anno(records.length)
					console.log("Completed inserting " + rows + " CORPUS rows")
					cb();
				})
			})
		})
	})
}

function importReferenceData(db, filename, cb)
{
	debug('importReferenceData')
	db.collection('REFERENCE', function (err, collection) {
		collection.ensureIndex({'TIPLOC':1,'refType':1}, {}, function () {
			console.log("Indexed on TIPLOC")
		})
		var fs = require('fs')
		var gzip = zlib.createGunzip()
		var filein = fs.createReadStream(filename)
		var options = {
			delimiter: "\t",
			endLine: "\n",
			columns: 
				['recordType', 
				'field1', 
				'field2', 
				'field3', 
				'field4', 
				'field5', 
				'field6',
				'field7',
				'field8',
				'field9',
				'field10',
				'field11',
				'field12',
				'field13',
				'field14',
				'field15',
				'field16',
				'field17',
				'field18',
				'field18',
				'field20'
				]
		}
		var tsvStream = csv.createStream(options)

		var rows= 0 
		var insertedRows = 0
		var documents = []
		tsvStream.on('data', function (data) {
			rows++
			var record = data
			var doc = {}
			doc.type = 'REFERENCE'
			switch(record.recordType)
			{
				case 'PIF':
					doc.refType = 'InterfaceSpecification'
					doc.fileVersion = record['field1']
					doc.sourceSystem = record['field2']
					doc.TOCid = record['field3']
					doc.timetableStartDate = record['field4']
					doc.timetableEndDate = record['field5']
					doc.cylcleType = record['field6']
					doc.cycleStage = record['field7']
					doc.creationDate = record['field8']
					doc.fileSequenceNumber = record['field9']
					break;
				case 'REF':
					doc.refType = 'ReferenceCode'
					doc.actionCode = record['field1']
					doc.referenceCodeType = record['field2']
					doc.description = record['field3']
					break;
				case 'TLD':
					doc.refType = 'TimingLoad'
					doc.actionCode = record['field1']
					doc.tractionType = record['field2']
					doc.trailingLoad = record['field3']
					doc.speed = record['field4']
					doc.raGauge = record['field5']
					doc.description = record['field6']
					doc.ITPSPowerType = record['field7']
					doc.ITPSLoad = record['field8']
					doc.limitingSpeed = record['field9']
					break;
				case 'LOC':
					doc.refType = 'GeographicData'
					doc.actionCode = record['field1']
					doc.TIPLOC = record['field2']
					doc.locationName = record['field3']
					doc.startDate = record['field4']
					doc.endDate = record['field5']
					doc.northing = record['field6']
					doc.easting = record['field7']
					doc.timingPointType = record['field8']
					doc.zone = record['field9']
					doc.STANOXcode = record['field10']
					doc.offNetworkIndicator = record['field11']
					doc.forceLPB = record['field12']
					break;
				case 'PLT':
					doc.refType = 'Platform'
					doc.actionCode = record['field1']
					doc.locationCode = record['field2']
					doc.platformID = record['field3']
					doc.startDate = record['field4']
					doc.endDate = record['field5']
					doc.length = record['field6']
					doc.powerSupplyType = record['field7']
					doc.DDOPassenger = record['field8']
					doc.DDONonPassenger = record['field9']
					break;
				case 'NWK':
					doc.refType = 'NetworkLink'
					doc.actioncode = record['field1']
					doc.originLocation = record['field2']
					doc.destinationLocation = record['field3']
					doc.runningLineCode = record['field4']
					doc.runningLineDescription = record['field5']
					doc.startDate = record['field6']
					doc.endDate = record['field7']
					doc.initialDirection = record['field8']
					doc.finalDirection = record['field9']
					doc.DDOPassenger = record['field10']
					doc.DDONonPassenger = record['field11']
					doc.RETB = record['field12']
					doc.zone = record['field13']
					doc.reversible = record['field14']
					doc.powerSupplyType = record['field15']
					doc.RA = record['field16']
					doc.maxTrainLength = record['field17']
					break;
				case 'TLK':
					doc.refType = 'TimingLink'
					doc.actionCode = record['field1']
					doc.originLocation = record['field2']
					doc.destinationLocation = record['field3']
					doc.runningLineCode = record['field4']
					doc.tractionType = record['field5']
					doc.trailingLoad = record['field6']
					doc.speed = record['field7']
					doc.RAGague = record['field8']
					doc.entrySpeed = record['field9']
					doc.exitSpeed = record['field10']
					doc.startDate = record['field11']
					doc.endDate = record['field12']
					doc.sectionalRunningTime = record['field13']
					doc.description = record['field14']
					break;
				default:
					doc = {}
			} 
			documents.push(doc)
			//debug('loadDocuments')
			if (documents.length >= 1000)
			{
				//debug('insertDocuments', documents.length)
				collection.insert( documents , function (error, records) {
				insertedRows += records.length
				console.log(insertedRows + " REFERENCE records inserted")
				})
				documents = []
			}
		})
		tsvStream.on('end', function () {
			console.log("Inserting last REFERENCE documents")
			collection.insert( documents , function (error, records ){
				insertedRows += records.length
				console.log(insertedRows + " records inserted")
				console.log("Processed " + rows + " rows")
				cb()
			})
		})
		filein.pipe(gzip).pipe(tsvStream)
	})
}

function importSchedule(db, options , cb)
{	
	var d = new Date();
	var weekday = [ "sun", "mon",  "tue", "wed", "thu",  "fri", "sat"];

	var dayOfWeek = weekday[d.getDay()];
	var gzip = zlib.createGunzip()	

	var	getUrl = "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full"
	if (options['update'] == true)
	{
		getUrl = "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-" + dayOfWeek
	} 
	var recordsParsed = 0
	console.log("Fetching " + getUrl)
	request(
	{
		uri: getUrl, 
		method: "GET",
		gzip: true,
		auth: {
			'username': config.username, 
			'pass': config.password
		},
		followRedirect: true
	}, function (error, response, body) {
		var scheduleJsonStream = JSONStream.parse("JsonScheduleV1")
		var associationJsonStream = JSONStream.parse("JsonAssociationV1")
		var tiplocJsonStream = JSONStream.parse("TiplocV1")
		var scheduleItems = 0
		var schedulesDeleted = 0
		var associationItems = 0
		var associationDeleted = 0
		var tiplocItems = 0
		var tiplocDeleted = 0
		var records = 0
		var dataURI = response.request.uri.href
		var dataStream = request(
		{
			uri: dataURI,
			method: "GET",
			gzip: true,
			followRedirect: false
		})
		if (options['update'])
		{
			dataStream = dataStream.pipe(gzip)
		}
		var schedDone = false
		var assocDone = false
		var tiplocDone = false

		function callbackWhenAllDone()
		{
			if (schedDone && assocDone && tiplocDone)
			{
				if (options['update'] == true)
				{
					console.log("Completed processing file: ")
					console.log(scheduleItems + " schedules inserted, " + schedulesDeleted + " deleted")
					console.log(associationItems + " assications inserted, " + associationDeleted + " deleted")
					console.log(tiplocItems + " tiploc items inserted, " + tiplocDeleted + " items deleted")
				} else {
					console.log("Completed processing file: " + scheduleItems + " schedules, " + associationItems + " associations, " + tiplocItems + " tiploc items")
				}
				cb()
			}
		}

		db.collection('SCHEDULE', function (err, collection ) {
			collection.ensureIndex({'CIF_train_uid':1}, function (err) {
				console.log("SCHEDULE indexed on CIF_train_uid")
			})
			var scheduleInserts = []
			var insertedScheduleRecords = 0
			dataStream.pipe(scheduleJsonStream).pipe(es.mapSync(function (data) {
				scheduleItems++
				var txType = data['transaction_type']
				delete data['transaction_type']
				switch(txType)
				{
					case 'Create':
						scheduleInserts.push(data)
						break;
					case 'Delete':
						collection.remove(data, {w:1}, function () {
							schedulesDeleted++
						})
						break;
				}
				if (scheduleInserts.length >= 500)
				{
					collection.insert(scheduleInserts, {w:1}, function (error, records) {
						if (error)
						{
							debug('inserts', error)
							console.log(error)
						}
						//debug('scheduleInserts', records, error)
						insertedScheduleRecords += records.length
						console.log("Inserted " + insertedScheduleRecords + " SCHEDULE records")
					})
					scheduleInserts = []
				}
			})).on('end',function () {
				if (scheduleInserts.length >= 1)
				{
					console.log("Inserting last Schedules")
					collection.insert(scheduleInserts, {w:1}, function (error, records) {
						insertedScheduleRecords += records.length
						console.log("Inserted " + insertedScheduleRecords + " SCHEDULE records")
						schedDone = true
						callbackWhenAllDone()
					})
				} else {
					schedDone = true
					callbackWhenAllDone()
				}
			})
		})

		db.collection('ASSOCIATION', function (err, collection ) {
			var assocInserts = []
			var insertedAssocRecords = 0
			collection.ensureIndex({'main_train_uid':1}, function () {
				console.log("ASSOCIATION indexed on main_train_uid")
			})
			dataStream.pipe(associationJsonStream).pipe(es.mapSync(function (data) {
				associationItems++
				var txType = data['transaction_type']
				delete data['transaction_type']
				data['type'] = 'association'
				switch(txType)
				{
					case 'Create':
						assocInserts.push( data )
						break;
					case 'Delete':
						collection.remove(data, {w:1}, function () { associationDeleted++ })
						break;
				}
				if (assocInserts.length >= 1000)
				{
					collection.insert(assocInserts, {w:1}, function (error, records) {
						insertedAssocRecords += records.length
						console.log("Inserted " + insertedAssocRecords + " ASSOCIATION records")
					})
					assocInserts = []
				}
			})).on('end', function () {
				console.log("Inserting last associations")
				if (assocInserts.length >= 1)
				{
					collection.insert(assocInserts, {w:1}, function (error, records) {						
						if (error)
						{
							console.log(error)
						}
						insertedAssocRecords += records.length
						console.log("Inserted " + insertedAssocRecords + " ASSOCIATION records")
						assocDone = true
						callbackWhenAllDone()
					})
				} else {
					assocDone = true
					callbackWhenAllDone()
				}
			})
		})
		
		db.collection('TIPLOC', function (err, collection ) {	
			collection.ensureIndex({'tiploc_code':1, 'stanox':1,'nalco':1},function ()
			{
				console.log("TIPLOC indexed on tiploc_code, stanox and nalco")
			})
			var tiplocInserts = []
			var insertedTiplocRows = 0
			dataStream.pipe(tiplocJsonStream).pipe(es.mapSync(function (data) {
				tiplocItems++
				var txType = data['transaction_type']
				delete data['transaction_type']
				data['type'] = 'tiploc'
				switch(txType)
				{
					case 'Create':
						tiplocInserts.push(data)
						break;
					case 'Delete':
						collection.remove(data, {w:1}, function () { tiplocDeleted++ })
						break;
				}
				if (tiplocInserts.length >= 1000) {
					collection.insert ( tiplocInserts, {w:1}, function (error, records) {
						if (error)
						{
							console.log(error)
						}
						insertedTiplocRows += records.length
						console.log("Inserted " + insertedTiplocRows + " TIPLOC records")
					})
					tiplocInserts = []
				}
			})).on('end', function () {
				console.log("Inserting last TIPLOC records")
				if (tiplocInserts.length >= 1)
				{
					collection.insert( tiplocInserts , {w:1} , function (error, records) {
						if (error)
						{
							console.log(error)
						}
						insertedTiplocRows += records.length
						console.log("Inserted " + insertedTiplocRows + " TIPLOC records")
						tiplocDone = true
						callbackWhenAllDone()
					})
				} else {
					tiplocDone = true
					callbackWhenAllDone()
				}
			})
		})
	})
}


module.exports = {
	'importSMART': importSMART,
	'importCORPUS': importCORPUS,
	'importReferenceData': importReferenceData,
	'importSchedule': importSchedule
}
