var Datastore = require('nedb'), db = new Datastore({filename: 'trainmon.db', autoload: true});

console.log("indexing by type")
db.ensureIndex({
fieldName: 'type'
},function (err)
{
	console.log("indexed type" + err)
})

db.ensureIndex({
fieldName: 'STANOX'
},function (err)
{
	console.log("indexed STANOX" + err)
})

db.find({type: 'CORPUS'}, function (err, docs) {
console.log(docs)

})