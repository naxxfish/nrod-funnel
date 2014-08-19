var Datastore = require('nedb'), db = new Datastore({filename: 'trainmon.db', autoload: true});
db.remove({type: 'config', key: 'referenceDataImported'}, {multi:true})
db.update({type: 'config', key: 'referenceDataImported'}, {$set: {'status': 'false'}},{multi:true }, function (err, numReplace) {
	console.log("Updated corpusImported key:" + numReplace)
	db.persistence.compactDatafile()
	db.find({type: 'config', key: 'referenceDataImported'}, function(err, doc)
	{
		console.log(doc)
	})
})