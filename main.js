const restify = require('restify');
const compression = require('compression')
const MongoClient = require('mongodb').MongoClient;
const config = require('./config')
const bunyan = require('bunyan');
var log = bunyan.createLogger({name: 'nrod-api'});

MongoClient.connect(config.mongo.connectionString, (err, db) =>
{
   var server = restify.createServer({
      log: log,
      name: 'nrod-api'
   })
   server.on('after', restify.auditLogger({
     log: log
   }));

	server.get( '/test', function (req, resp, next)
	{
		var collection = db.collection('TRAINS')
		collection.find({'movementActive':true, 'tdActive': true, 'lastSeen.location.TIPLOC': 'LEWISHM'}).toArray( (error, doc) => {
         log.debug(doc)
			resp.end(JSON.stringify(doc))
         next()
		})
	})

	server.listen(3000, () => {
		log.info('%s listening at %s', server.name, server.url)
	})
})
