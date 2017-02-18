const config = require('./config')
const install = require('./install')

const zlib = require('zlib')
const fs = require('fs')
const MongoClient = require('mongodb').MongoClient;
const moment = require('moment')
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
