#!/usr/bin/env node

const config = require('./config')
const bunyan = require('bunyan');
const zlib = require('zlib')
const JSONStream = require('JSONStream')
const csv = require('csv-stream')
const es = require('event-stream')
const request = require('request')
const ftpClient = require('ftp');
const xmlNodes = require('xml-nodes');
const xmlObjects = require('xml-objects');
var log = bunyan.createLogger({
    name: 'nrod-installer',
    stream: process.stdout,
    level: "debug"
});
const boot = require('onboot');

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
        boot.strap((done) => {
            var referenceDataFilename = '20140116_ReferenceData.gz'
            log.info("Importing reference data from " + referenceDataFilename)
            importReferenceData(db, referenceDataFilename, () => {
                log.info("Reference Data imported")
                done()
            })
        })

        boot.strap((done) => {
            importSMART(db, () => {
                log.info("SMART data imported")
                done()
            })
        })

        boot.strap((done) => {
            importCORPUS(db, () => {
                log.info("CORPUS data imported")
                log.info("Importing initial (full) SCHEDULE data")
                done()
            })
        })

        boot.strap((done) => {
            importSchedule(db, {
                update: false
            }, () => {
                log.info("Schedule imported")
                done()
            })
        })


        boot.up((done) => {
            importDarwinReference(db, () => {
                log.info("Darwin Reference data imported!")
                importDarwinSchedule(db, () => {
                    log.info("Darwin Schedule data imported!")
                    log.info('install complete')
                    db.close()
                    process.exit()
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
            collection.createIndexes({
                'FROMBERTH': 1,
                'TOBERTH': 1
            }, {}, () => {
                log.info("Index created on SMART collection")
            })
            dataStream.pipe(gzip).pipe(jstream).pipe(es.mapSync((data) => {
                rows++
                data.type = 'SMART'
                //log.info(data)
                collection.update({
                        type: 'SMART',
                        'FROMBERTH': data.FROMBERTH,
                        'TOBERTH': data.TOBERTH
                    },
                    data, {
                        w: 1,
                        upsert: true
                    },
                    (err, result) => {
                        if (err) {
                            log.error(err)
                        }
                    })
            })).on('error', (error) => {
                log.error({
                    "referenceInsertError": error
                })
            })
        })
    })
}

function importCORPUS(db, cb) {
    log.debug('importCORPUS')
    var insertedCount = 0
    var progress = 0
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
            collection.remove({}, {}, () => {
                log.info('CORPUS purged')
                collection.ensureIndex({
                    'STANOX': 1
                }, {}, () => {
                    log.info("CORPUS Indexed on STANOX")
                })
                var dataStream = request({
                    uri: dataURI,
                    method: "GET",
                    gzip: true,
                    followRedirect: false
                }).pipe(gzip)

                dataStream.pipe(jstream).pipe(es.mapSync((doc) => {
                    dataStream.pause()
                    doc.type = 'CORPUS'
                    collection.insertOne(doc, {}, (error, result) => {
                        dataStream.resume()
                        if (progress > 1000) {
                            progress = 0
                            log.info({
                                'type': 'CORPUS',
                                'insertedCount': insertedCount
                            })
                        }
                        insertedCount += 1
                        progress += 1
                    })
                })).on('error', (pipeErr) => {
                    log.error("Error in CORPUS stream", pipeErr)
                }).on('end', () => {
                    log.info("CORPUS import complete")
                    cb()
                })
            })
        })
    })
}

function importReferenceData(db, filename, cb) {
    log.debug('importReferenceData')
    db.collection('REFERENCE', (err, collection) => {
        collection.remove({}, {}, () => { // delete the entire contents first, then repopulate it
            log.info('reference collection purged')
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
            var progress = 0
            var documents = []
            tsvStream.on('data', (data) => {
                tsvStream.pause()
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
                        collection.insertOne(doc, {}, (error, result) => {
                            if (error) {
                                log.error(error)
                            }
                            insertedRows += 1
                            progress += 1
                            tsvStream.resume()
                        })
                        break;
                    case 'REF':
                        doc.refType = 'ReferenceCode'
                        doc.actionCode = record['field1']
                        doc.referenceCodeType = record['field2']
                        doc.description = record['field3']
                        collection.insert(doc, {}, (error, result) => {
                            if (error) {
                                log.error(error)
                            }
                            insertedRows += 1
                            progress += 1
                            tsvStream.resume()
                        })
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
                        collection.insertOne(doc, {}, (error, result) => {
                            if (error) {
                                log.error(error)
                            }
                            insertedRows += 1
                            progress += 1
                            tsvStream.resume()
                        })
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
                        collection.insert(doc, {}, (error, result) => {
                            if (error) {
                                log.error(error)
                            }
                            insertedRows += 1
                            progress += 1
                            tsvStream.resume()
                        })
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
                        collection.insertOne(doc, {}, (error, result) => {
                            if (error) {
                                log.error(error)
                            }
                            insertedRows += 1
                            progress += 1
                            tsvStream.resume()
                        })
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
                        collection.insert(doc, {}, (error, result) => {
                            if (error) {
                                log.error(error)
                            }
                            insertedRows += 1
                            progress += 1
                            tsvStream.resume()
                        })
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
                        collection.insert(doc, {}, (error, result) => {
                            if (error) {
                                log.error(error)
                            }
                            insertedRows += 1
                            progress += 1
                            tsvStream.resume()
                        })
                        break;
                }
                if (progress > 1000) {
                    progress = 0
                    log.info({
                        'referenceRows': insertedRows
                    })
                }
            }).on('error', (pipeErr) => {
                log.error("Error in REFERENCE stream", pipeErr)
            })
            filein.pipe(gzip).pipe(tsvStream).on('end', () => {
                log.info({
                    'completed': true,
                    'referenceRows': insertedRows
                })
                cb()
            })
        })
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
    log.info('scheduleImport', "Fetching " + getUrl)
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
                log.info({
                    'status': 'completed',
                    schedule: {
                        inserted: insertedScheduleRecords,
                        deleted: schedulesDeleted,
                    },
                    assoc: {
                        inserted: associationItems,
                        deleted: associationDeleted
                    },
                    tiploc: {
                        inserted: tiplocItems,
                        deleted: tiplocDeleted
                    }
                })
                cb()
            } else {
                log.debug("not everything is done")
            }
        }
        var insertedScheduleRecords = 0

        function insertScheduleRecords(collection, scheduleInserts) {
            collection.insertMany(scheduleInserts, {
                w: 1
            }, (error, result) => {
                if (error) {
                    log.debug('inserts', error)
                    log.info(error)
                }
                insertedScheduleRecords += result.insertedCount
                log.info({
                    'status': 'in_progress',
                    'schedule': {
                        inserted: insertedScheduleRecords
                    }
                })
            })
        }

        db.collection('SCHEDULE', (err, collection) => {
            collection.ensureIndex({
                'CIF_train_uid': 1
            }, (err) => {
                log.info("SCHEDULE indexed on CIF_train_uid")
            })
            var scheduleInserts = []

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
                    insertScheduleRecords(collection, scheduleInserts)
                    scheduleInserts = []
                }
            })).on('end', function() {
                if (scheduleInserts.length >= 1) {
                    log.info("Inserting last Schedules")
                    insertScheduleRecords(collection, scheduleInserts)
                }
                schedDone = true
                callbackWhenAllDone()
            }).on('error', (pipeErr) => {
                log.error("Error in SCHEDULE stream", pipeErr)
            })
        })



        db.collection('ASSOCIATION', (err, collection) => {
            if (err) {
                log.error(err)
            }
            var assocInserts = []
            var insertedAssocRecords = 0
            collection.createIndexes({
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
                    collection.insertMany(assocInserts, {
                        w: 1
                    }, (error, result) => {
                        if (records != null) {
                            insertedAssocRecords += result.insertedCount
                        }

                        log.info({
                            'status': 'in_progress',
                            'assoc': {
                                inserted: insertedAssocRecords,
                                deleted: associationDeleted
                            }
                        })
                    })
                    assocInserts = []
                }
            })).on('end', function() {
                log.info("Inserting last associations")
                if (assocInserts.length >= 1) {
                    collection.insert(assocInserts, {
                        w: 1
                    }, (error, result) => {
                        if (error) {
                            log.error(error)
                        }
                        insertedAssocRecords += result.insertedCount
                        log.info({
                            'status': 'in_progress',
                            'assoc': {
                                inserted: insertedAssocRecords,
                                deleted: associationDeleted
                            }
                        })
                        assocDone = true
                        callbackWhenAllDone()
                    })
                } else {
                    assocDone = true
                    callbackWhenAllDone()
                }
            }).on('error', (pipeErr) => {
                log.error("Error in ASSOCIATION stream", pipeErr)
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
                    }, (error, result) => {
                        if (error) {
                            log.error(error)
                        }
                        insertedTiplocRows += result.insertedCount
                        log.info({
                            'status': 'in_progress',
                            'tiploc': {
                                inserted: insertedTiplocRows,
                                deleted: tiplocDeleted
                            }
                        })
                    })
                    tiplocInserts = []
                }
            })).on('end', function() {
                log.info("Inserting last TIPLOC records")
                if (tiplocInserts.length >= 1) {
                    collection.insert(tiplocInserts, {
                        w: 1
                    }, (error, result) => {
                        if (error) {
                            log.error(error)
                        }
                        insertedTiplocRows += result.insertedCount
                        log.info({
                            'status': 'in_progress',
                            'tiploc': {
                                inserted: insertedTiplocRows,
                                deleted: tiplocDeleted
                            }
                        })
                        tiplocDone = true
                        callbackWhenAllDone()
                    })
                } else {
                    tiplocDone = true
                    callbackWhenAllDone()
                }
            }).on('error', (pipeErr) => {
                log.error("Error in TIPLOC stream", pipeErr)
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
