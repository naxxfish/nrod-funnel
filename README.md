NROD Funnel
===========

My attempt at making use of the Network Rail Open Data feeds https://datafeeds.networkrail.co.uk/

This uses a MongoDB to keep track of the current state of things on the network, as well as all the schedule and reference data.

You need a config.js file that looks something like this:

	var config = {
	        "securityToken": "the security token they ask you to 'display in your source code'",
	        "stompHost": "datafeeds.networkrail.co.uk",
	        "stompPort": 61618,
	        "tdChannel": "TD_ALL_SIG_AREA", // find which one you want to subscribe to here http://nrodwiki.rockshore.net/index.php/TD
	        'movementChannel': 'TRAIN_MVT_ALL_TOC', // find which one you want to subscribe to here http://nrodwiki.rockshore.net/index.php/TD
	        "username": "your username",
	        "password": "your password",
	        "mongo": {
	                'connectionString': 'mongodb://mongouser:mongopassword@mongohost:27017/mongodbname'
	        }
	        "feeds": {              // feeds which are enabled
	                'TD' : [
	                        'C', // have this here to enable processing C class messages
	                        'S'  // have this here to enable processing S class messages
	                ],
	                'TRUST': true, // enable processing TRUST messages
	                'VSTP': true,  // enable processing VSTP messges (not implemented yet)
	                'SCHEDULE': true // enable processing SCHEDULE feed
	        }
	}

	module.exports = config

Setup 
-----

You'll need to download the [Train Planning Data file](http://nrodwiki.rockshore.net/images/1/14/20140116_ReferenceData.gz) and stick it 
in the same directory as the installer, with that filename (20140116_ReferenceData.gz).  If a new one is published, you'll either need 
to rename it to the old name, or edit installer.js.

Run the installer to initialise your database with reference and schedule data:

	node install.js

This will take a while, as it streams a gzipped file off Amazon S3 and inserts the records into your DB.  This is all the static 
[Reference Data](http://nrodwiki.rockshore.net/index.php/Reference_data) as well as the last full export of the SCHEDULE feed.

And set up a cron job to updte the SCHEDULE feed once a day at about 0430 (to be safe)

	30 4 * * * node /home/chris/trainmon/update.js

This will get a daily "changes" export of the SCHEDULE feed and apply it to your DB. 

Running
-------

You'll need to run feedme.js to turn on the hose and start updating your MongoDB.

	node feedme.js

If you want to see the messages come in, you can set the DEBUG environment variable to trainmon-main

	DEBUG=trainmon-main node feedme.js

So far, I've only written a fairly dumb HTTP API that doesn't do much.  

Data Strcuture
--------------

There are a few pre-initialised collections, REFERENCE, SMART and CORPUS.  These are basically direct imports of the data - see the NROD 
wiki for more info: [Reference data](http://nrodwiki.rockshore.net/index.php/Reference_data)

The SCHEDULE collection is initialised with a full import, and kept up to date on a daily basis as the files are released on the 
SCHEDULE feed.  The data structure is basically a direct import from the [SCHEDULE 
feed](http://nrodwiki.rockshore.net/index.php/SCHEDULE).

The collection you're probably interested in is the TRAINS collection, which has a record for every train that's been seen on the 
network. This includes trains which have been [TRUST](http://en.wikipedia.org/wiki/TRUST) [activated](http://nrodwiki.rockshore.net/index.php/Train_Activation), have been 
seen by a [Train Describer](http://nrodwiki.rockshore.net/index.php/TD).  The records are a linking of the SCHEDULE, TD and Train 
Movement feeds, as well as the SMART/CORPUS data about their current location.  For more information about these things, see these links:

* [Train Movements](http://nrodwiki.rockshore.net/index.php/Train_Movements)
* [Train Describer](http://nrodwiki.rockshore.net/index.php/TD)

The NROD wiki is *the* reference for what on earth all this stuff means, as well as the [Open Rail Data Google Group](https://groups.google.com/forum/#!topic/openraildata-talk)

Current Limitations
-------------------

S Class messages are now being logged, however not decoded.  The state of each TD is kept in SIGNALS collection.  Some effort has been put into decoding them on the 
[ORD google group](https://groups.google.com/forum/#!topic/openraildata-talk/Y1_5Bu6sb1w), but none of this is implemented at the moment. 

At the moment there's no REST API for getting at the data - you currently need to query the DB directly yourself

Currently doesn't process VSTP feed - but does subscribe to it.

.