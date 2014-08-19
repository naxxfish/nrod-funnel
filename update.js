
var config = require('./config')
var sys = require('sys')
var zlib = require('zlib')
var fs = require('fs')
var MongoClient = require('mongodb').MongoClient;
var install = require('./install')
var chalk = require('chalk')

MongoClient.connect('mongodb://trainmon:trainmon@localhost:27017/trainmon?w=0', function (err, db) {
	if (err)
	{
		console.log(err)
		return
	}
	install.importSchedule(db, {update: true}, function () {
		console.log(chalk.green("Schedule data updated!"))
		process.exit()
	}) // update

})
