var express = require('express');
var app = express();
var compression = require('compression')
var MongoClient = require('mongodb').MongoClient;
var config = require('./config')

MongoClient.connect(config.mongo.connectionString, function (err, db)
{
	app.use( compression({ threshold: 512 }) )

	app.use(express.static(__dirname + '/wwwroot'));

	app.get( '/test', function (req, resp)
	{
		var collection = db.collection('TRAINS')
		collection.find({'movementActive':true, 'tdActive': true, 'lastSeen.location.TIPLOC': 'LEWISHM'}, {}, function (error, doc) {
			resp.send(JSON.stringify(doc))
		})
	})

	var server = app.listen(3000, function () {
		console.log("Listening on port %d", server.address().port)
	})
})
