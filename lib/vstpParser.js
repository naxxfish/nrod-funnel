var debug = require('debug')('nrod-vstp')
var moment = require('moment')
var config = require('../config')

var db = null

exports.parse = function (indb, message)
{
	db = indb
   if (message !== undefined)
   {
      if (message.hasOwnProperty('schedule'))
      {
         var schedule = message['schedule']
         debug('vstParse', schedule)
      }
   }
}
