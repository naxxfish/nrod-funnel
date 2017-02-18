#!/usr/bin/env node

const config = require('./config')
const bunyan = require('bunyan');
const zlib = require('zlib')
const JSONStream = require('JSONStream')
const csv = require('csv-stream')
const es = require('event-stream')
const request = require('request')
const sftpClient = require('ftp');
const xmlNodes = require('xml-nodes');
const xmlObjects = require('xml-objects');
var log = bunyan.createLogger({
    name: 'nrod-installer'
});

// Retrieve
const MongoClient = require('mongodb').MongoClient;

function main() {
    log.debug({
        'main': config.securityToken
    })
    MongoClient.connect(config.mongo.connectionString, (err, db) => {
        if (err) {
            log.fatal("Error: " + err)
            return
        }

        var referenceDataFilename = '20140116_ReferenceData.gz'
        log.info("Initialising datasets")
        log.info("Importing reference data from " + referenceDataFilename)
        importReferenceData(db, referenceDataFilename, () => {
            log.info("Reference Data imported")
            importSMART(db, () => {
                log.info("SMART data imported")
                importCORPUS(db, () => {
                    log.info("CORPUS data imported")
                    log.info("Importing initial (full) SCHEDULE data")
                    importSchedule(db, {
                        update: false
                    }, () => {
                        log.info("Schedule data imported!")
                        importDarwinReference(db, () => {
                            log.info("Darwin Reference data imported!")
                            importDarwinSchedule(db, () => {
                                log.info("Darwin Schedule data imported!")
                                db.close()
                                process.exit()
                            })
                        })
                    })
                })
            })
        })
    })
}

if (require.main === module) {
    main()
}

function importSMART(db, cb) {
    log.debug('importSMART')
    var fs = require('fs')
    var gzip = zlib.createGunzip()
    var getUrl = "http://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=SMART"
    request({
        uri: getUrl,
        method: "GET",
        gzip: true,
        auth: {
            'username': config.username,
            'pass': config.password
        },
        followRedirect: true
    }, (error, response, body) => {
        if (error) {
            log.error(error)
            return
        }
        var dataURI = response.request.uri.href
        var dataStream = request({
            uri: dataURI,
            method: "GET",
            gzip: true,
            followRedirect: true
        }, (err, response, body) => {
            if (err) {
                log.error(err)
            }
        })
        var scheduleItems = 0
        var associationItems = 0
        var tiplocItems = 0
        var records = 0

        var jstream = JSONStream.parse("BERTHDATA.*")

        function anno(items) {
            log.info("Written " + items + " SMART Data rows")
        }
        var rows = 0

        db.collection('SMART', (err, collection) => {
            collection.ensureIndex({
                'FROMBERTH': 1,
                'TOBERTH': 1
            }, {}, () => {
                log.info("Index created")
            })
            var documents = []
            dataStream.pipe(gzip).pipe(jstream).pipe(es.mapSync((data) => {
                rows++
                data.type = 'SMART'
                //log.info(data)
                documents.push(data)
                //				log.debug('insertDocuments','documents.length',documents.length )
                if (documents.length >= 500) {
                    log.debug('insertDocuments', 'inserting batch of documents', documents.length)
                    collection.insert(documents, {
                        w: 1
                    }, (err, records) => {
                        if (err) {
                            log.error(err)
                        }
                        log.debug('insertDocuments', 'inserted', records.length, 'records')
                        //anno(Object.keys(records))
                    })
                    documents = []
                }
            })).on('end', function() {
                collection.insert(documents, {
                    w: 1
                }, (err, records) => {
                    if (err) {
                        log.error(err)
                    }
                    log.debug('insertDocumentsFinally', records.length)
                    anno(rows)
                    cb();
                })
            })
        })
    })
}

function importCORPUS(db, cb) {
    log.debug('importCORPUS')
    var fs = require('fs')
    var gzip = zlib.createGunzip()
    var jstream = JSONStream.parse("TIPLOCDATA.*")
    var getUrl = "http://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS"
    request({
        uri: getUrl,
        method: "GET",
        gzip: true,
        auth: {
            'username': config.username,
            'pass': config.password
        },
        followRedirect: true
    }, (error, response, body) => {
        var dataURI = response.request.uri.href

        function anno(items) {
            log.info("Processed " + items + " CORPUS rows")
        }

        db.collection('CORPUS', (err, collection) => {
            var rows = 0
            var documents = []
            collection.ensureIndex({
                'STANOX': 1
            }, {}, () => {
                log.info("Indexed on STANOX")
            })
            var dataStream = request({
                uri: dataURI,
                method: "GET",
                gzip: true,
                followRedirect: false
            }).pipe(gzip)

            dataStream.pipe(jstream).pipe(es.mapSync((doc) => {
                rows++
                doc.type = 'CORPUS'
                documents.push(doc)
                if (documents.length >= 500) {
                    log.debug('insertDocuments', 'inserting batch of CORPUS records', documents.length)
                    collection.insert(documents, (error, records) => {
                        anno(records.length)
                    })
                    documents = []
                }
            })).on('end', () => {
                collection.insert(documents, {
                    w: 1
                }, (err, records) => {
                    anno(records.length)
                    log.info("Completed inserting " + rows + " CORPUS rows")
                    cb();
                })
            })
        })
    })
}

function importReferenceData(db, filename, cb) {
    log.debug('importReferenceData')
    db.collection('REFERENCE', (err, collection) => {
        collection.ensureIndex({
            'TIPLOC': 1,
            'refType': 1
        }, {}, () => {
            log.info("Indexed on TIPLOC")
        })
        var fs = require('fs')
        var gzip = zlib.createGunzip()
        var filein = fs.createReadStream(filename)
        var options = {
            delimiter: "\t",
            endLine: "\n",
            columns: ['recordType',
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

        var rows = 0
        var insertedRows = 0
        var documents = []
        tsvStream.on('data', (data) => {
            rows++
            var record = data
            var doc = {}
            doc.type = 'REFERENCE'
            switch (record.recordType) {
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
            //log.debug('loadDocuments')
            if (documents.length >= 1000) {
                //log.debug('insertDocuments', documents.length)
                collection.insert(documents, (error, records) => {
                    insertedRows += records.length
                    log.info(insertedRows + " REFERENCE records inserted")
                })
                documents = []
            }
        })
        tsvStream.on('end', () => {
            log.info("Inserting last REFERENCE documents")
            collection.insert(documents, (error, records) => {
                insertedRows += records.length
                log.info(insertedRows + " records inserted")
                log.info("Processed " + rows + " rows")
                cb()
            })
        })
        filein.pipe(gzip).pipe(tsvStream)
    })
}

function importSchedule(db, options, cb) {
    var d = new Date();
    var weekday = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];

    var dayOfWeek = weekday[d.getDay()];
    var gzip = zlib.createGunzip()

    var getUrl = "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full"
    if (options['update'] == true) {
        getUrl = "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-" + dayOfWeek
    }
    var recordsParsed = 0
    log.info("Fetching " + getUrl)
    request({
        uri: getUrl,
        method: "GET",
        gzip: true,
        auth: {
            'username': config.username,
            'pass': config.password
        },
        followRedirect: true
    }, (error, response, body) => {
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
        var dataStream = request({
            uri: dataURI,
            method: "GET",
            gzip: true,
            followRedirect: false
        })
        //if (!options['update']) { // turns out update are gzipped too
        dataStream = dataStream.pipe(gzip)
        //}
        var schedDone = false
        var assocDone = false
        var tiplocDone = false

        function callbackWhenAllDone() {
            if (schedDone && assocDone && tiplocDone) {
                if (options['update'] == true) {
                    log.info("Completed processing file: ")
                    log.info(scheduleItems + " schedules inserted, " + schedulesDeleted + " deleted")
                    log.info(associationItems + " assications inserted, " + associationDeleted + " deleted")
                    log.info(tiplocItems + " tiploc items inserted, " + tiplocDeleted + " items deleted")
                } else {
                    log.info("Completed processing file: " + scheduleItems + " schedules, " + associationItems + " associations, " + tiplocItems + " tiploc items")
                }
                cb()
            }
        }

        db.collection('SCHEDULE', (err, collection) => {
            collection.ensureIndex({
                'CIF_train_uid': 1
            }, (err) => {
                log.info("SCHEDULE indexed on CIF_train_uid")
            })
            var scheduleInserts = []
            var insertedScheduleRecords = 0
            dataStream.pipe(scheduleJsonStream).pipe(es.mapSync((data) => {
                scheduleItems++
                var txType = data['transaction_type']
                delete data['transaction_type']
                switch (txType) {
                    case 'Create':
                        scheduleInserts.push(data)
                        break;
                    case 'Delete':
                        collection.remove(data, {
                            w: 1
                        }, () => {
                            schedulesDeleted++
                        })
                        break;
                }
                if (scheduleInserts.length >= 500) {
                    collection.insert(scheduleInserts, {
                        w: 1
                    }, (error, records) => {
                        if (error) {
                            log.debug('inserts', error)
                            log.info(error)
                        }
                        //log.debug('scheduleInserts', records, error)
                        if (records != null){
                           insertedScheduleRecords += records.length
                        }
                        log.info("Inserted " + insertedScheduleRecords + " SCHEDULE records")
                    })
                    scheduleInserts = []
                }
            })).on('end', function() {
                if (scheduleInserts.length >= 1) {
                    log.info("Inserting last Schedules")
                    collection.insert(scheduleInserts, {
                        w: 1
                    }, (error, records) => {
                        if (records != null) {
                            insertedScheduleRecords += records.length
                            log.info("Inserted " + insertedScheduleRecords + " SCHEDULE records")
                        }
                        schedDone = true
                        callbackWhenAllDone()
                    })
                } else {
                    schedDone = true
                    callbackWhenAllDone()
                }
            })
        })

        db.collection('ASSOCIATION', (err, collection) => {
            var assocInserts = []
            var insertedAssocRecords = 0
            collection.ensureIndex({
                'main_train_uid': 1
            }, () => {
                log.info("ASSOCIATION indexed on main_train_uid")
            })
            dataStream.pipe(associationJsonStream).pipe(es.mapSync((data) => {
                associationItems++
                var txType = data['transaction_type']
                delete data['transaction_type']
                data['type'] = 'association'
                switch (txType) {
                    case 'Create':
                        assocInserts.push(data)
                        break;
                    case 'Delete':
                        collection.remove(data, {
                            w: 1
                        }, () => {
                            associationDeleted++
                        })
                        break;
                }
                if (assocInserts.length >= 1000) {
                    collection.insert(assocInserts, {
                        w: 1
                    }, (error, records) => {
                       if (records != null)
                       {
                          insertedAssocRecords += records.length
                       }

                        log.info("Inserted " + insertedAssocRecords + " ASSOCIATION records")
                    })
                    assocInserts = []
                }
            })).on('end', function() {
                log.info("Inserting last associations")
                if (assocInserts.length >= 1) {
                    collection.insert(assocInserts, {
                        w: 1
                    }, (error, records) => {
                        if (error) {
                            log.error(error)
                        }
                        insertedAssocRecords += records.length
                        log.info("Inserted " + insertedAssocRecords + " ASSOCIATION records")
                        assocDone = true
                        callbackWhenAllDone()
                    })
                } else {
                    assocDone = true
                    callbackWhenAllDone()
                }
            })
        })

        db.collection('TIPLOC', (err, collection) => {
            collection.ensureIndex({
                'tiploc_code': 1,
                'stanox': 1,
                'nalco': 1
            }, () => {
                log.info("TIPLOC indexed on tiploc_code, stanox and nalco")
            })
            var tiplocInserts = []
            var insertedTiplocRows = 0
            dataStream.pipe(tiplocJsonStream).pipe(es.mapSync((data) => {
                tiplocItems++
                var txType = data['transaction_type']
                delete data['transaction_type']
                data['type'] = 'tiploc'
                switch (txType) {
                    case 'Create':
                        tiplocInserts.push(data)
                        break;
                    case 'Delete':
                        collection.remove(data, {
                            w: 1
                        }, () => {
                            tiplocDeleted++
                        })
                        break;
                }
                if (tiplocInserts.length >= 1000) {
                    collection.insert(tiplocInserts, {
                        w: 1
                    }, (error, records) => {
                        if (error) {
                            log.error(error)
                        }
                        insertedTiplocRows += records.length
                        log.info("Inserted " + insertedTiplocRows + " TIPLOC records")
                    })
                    tiplocInserts = []
                }
            })).on('end', function() {
                log.info("Inserting last TIPLOC records")
                if (tiplocInserts.length >= 1) {
                    collection.insert(tiplocInserts, {
                        w: 1
                    }, (error, records) => {
                        if (error) {
                            log.error(error)
                        }
                        insertedTiplocRows += records.length
                        log.info("Inserted " + insertedTiplocRows + " TIPLOC records")
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

function importDarwinReference(db, cb) {
    var c = new ftpClient();
    var gzip = zlib.createGunzip()
    db.collection('REFERENCE', (err, collection) => {
        c.on('ready', () => {
            c.list('.', (err, list) => {
                list.forEach((file) => {
                    if (file.name.match(/ref_v3.xml.gz$/) != null) {
                        log.debug('importDarwinReference', "retrieve " + file.name)
                        c.get(file.name, (err, stream) => {
                            if (err) throw err;
                            stream.once('close', () => {
                                c.end();
                            });
                            var unzipped = stream.pipe(gzip)
                            unzipped.pipe(xmlNodes('LocationRef')).pipe(xmlObjects()).on('data', (data) => {
                                log.debug('LocationRef', data.LocationRef)
                                var location = data.LocationRef.$
                                // update a location that already exists with the darwin data - lookup by TIPLOC
                                var updateDoc = {
                                    'darwin': {
                                        'locname': location.locname,
                                        'crs': location.crs,
                                        'toc': location.toc
                                    }
                                }
                                log.debug('update', updateDoc)
                                collection.update({
                                    $and: [{
                                        'refType': {
                                            $eq: 'GeographicData'
                                        }
                                    }, {
                                        'TIPLOC': {
                                            $eq: location.tpl
                                        }
                                    }]
                                }, {
                                    $set: updateDoc
                                }, {
                                    upsert: true
                                })
                            });
                            unzipped.pipe(xmlNodes('Via')).pipe(xmlObjects()).on('data', (data) => {
                                log.debug('Via', data.Via)
                                var viaPoint = data.Via.$
                                viaPoint.refType = 'Via'
                                collection.insert(viaPoint)
                            })
                            unzipped.pipe(xmlNodes('TocRef')).pipe(xmlObjects()).on('data', (data) => {
                                //log.debug('TocRef', data.Via)
                                var tocRecord = data.TocRef.$
                                tocRecord.refType = 'TocRef'
                                collection.insert(tocRecord)
                            })
                            unzipped.pipe(xmlNodes('LateRunningReasons')).pipe(xmlNodes('Reason')).pipe(xmlObjects()).on('data', (data) => {
                                //log.debug('LateRunningReason', data.Reason)
                                var lateReason = data.Reason.$
                                lateReason.refType = 'LateRunningReason'
                                collection.update({
                                    'refType': 'LateRunningReason',
                                    code: lateReason.code
                                }, lateReason, {
                                    upsert: true
                                })
                            })
                            unzipped.pipe(xmlNodes('CancellationReasons')).pipe(xmlNodes('Reason')).pipe(xmlObjects()).on('data', (data) => {
                                //log.debug('CancellationReason', data.Reason)
                                var cancelledReason = data.Reason.$
                                cancelledReason.refType = 'LateRunningReason'
                                collection.update({
                                    'refType': 'CancelledReason',
                                    code: cancelledReason.code
                                }, cancelledReason, {
                                    upsert: true
                                })
                            })
                        })
                    }
                })
            })
        })
        // connect to localhost:21 as anonymous
        c.connect({
            host: config.darwin.ftpHost,
            port: 21,
            user: config.darwin.ftpUser,
            password: config.darwin.ftpPassword
        });
    })
}

function importDarwinSchedule(db, cb) {
    var c = new ftpClient();
    var gzip = zlib.createGunzip()
    db.collection('DSCHEDULE', function(err, collection) {
        c.on('ready', () => {
            c.list('.', (err, list) => {
                list.forEach((file) => {
                    if (file.name.match(/_v8.xml.gz$/) != null) {
                        log.debug('importDarwinSchedule', "retrieve " + file.name)
                        c.get(file.name, (err, stream) => {
                            if (err) throw err;
                            stream.once('close', () => {
                                c.end();
                            });
                            var unzipped = stream.pipe(gzip)
                            unzipped.pipe(xmlNodes('Journey')).pipe(xmlObjects({
                                explicitChildren: true,
                                preserveChildrenOrder: true
                            })).on('data', function(data) {
                                log.debug('Journey', data.Journey)
                                var journey = data.Journey.$
                                // update a location that already exists with the darwin data - lookup by TIPLOC

                                log.debug('update', journey)
                                collection.update({
                                    'rid': journey.rid
                                }, {
                                    $set: journey
                                }, {
                                    upsert: true
                                })
                            });

                        })
                    }
                })
            })
        })
        // connect to localhost:21 as anonymous
        c.connect({
            host: config.darwin.ftpHost,
            port: 21,
            user: config.darwin.ftpUser,
            password: config.darwin.ftpPassword
        });
    })
}

module.exports = {
    'importSMART': importSMART,
    'importCORPUS': importCORPUS,
    'importReferenceData': importReferenceData,
    'importSchedule': importSchedule,
    'importDarwinReference': importDarwinReference,
    'importDarwinSchedule': importDarwinSchedule
}
