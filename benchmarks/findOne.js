// Userland modules
var async = require('async')

var execTime = require('exec-time')

// Local modules

var commonUtilities = require('./commonUtilities')

// Local variables

var benchDb = 'workspace/findOne.bench.db'

var profiler = new execTime('FINDONE BENCH')

var config = commonUtilities.getConfiguration(benchDb)

var d = config.d

var n = config.n

async.waterfall([
  async.apply(commonUtilities.prepareDb, benchDb),
  function (cb) {
    d.loadDatabase(function (err) {
      if (err) { return cb(err) }
      if (config.program.withIndex) { d.ensureIndex({ fieldName: 'docNumber' }) }
      cb()
    })
  },
  function (cb) { profiler.beginProfiling(); return cb() },
  async.apply(commonUtilities.insertDocs, d, n, profiler),
  function (cb) { setTimeout(function () { cb() }, 500) },
  async.apply(commonUtilities.findOneDocs, d, n, profiler)
], function (err) {
  profiler.step('Benchmark finished')

  if (err) { return console.log('An error was encountered: ', err) }
})
