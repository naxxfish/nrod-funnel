var config = require('./config')
var sys = require('sys')
var zlib = require('zlib')
var fs = require('fs')
var MongoClient = require('mongodb').MongoClient;
var install = require('./install')
var chalk = require('chalk')
var moment = require('moment')
const bunyan = require('bunyan');
var log = bunyan.createLogger({name: 'nrod-updater'});

MongoClient.connect(config.mongo.connectionString, (err, db) => {
    if (err) {
        console.log(err)
        return
    }
    install.importSchedule(db, {
        update: true
    }, () => {
        log.info("Schedule data updated!")
        process.exit()
    }) // update
    // clean up old trains
    var lastWeek = moment().subtract('7', 'days')
    log.info("Deleting TRAINS older than 7 days old (" + lastWeek.unix() + ")")
    var trains = db.collection('TRAINS')
    trains.remove({
        'lastUpdated': {
            $lt: lastWeek.unix()
        }
    }, (docs) => {
        log.info("Removed " + docs + " docs")
        db.close()
    })
})
