// Userland modules
var async = require('async')

var execTime = require('exec-time')

// Local modules

var commonUtilities = require('./commonUtilities')

// Local variables

var benchDb = 'workspace/remove.bench.db'

var profiler = new execTime('REMOVE BENCH')

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

  // Test with remove only one document
  function (cb) { profiler.step('MULTI: FALSE'); return cb() },
  async.apply(commonUtilities.removeDocs, { multi: false }, d, n, profiler),
  // Test with multiple documents
  function (cb) { d.remove({}, { multi: true }, function () { return cb() }) },
  async.apply(commonUtilities.insertDocs, d, n, profiler),
  function (cb) { profiler.step('MULTI: TRUE'); return cb() },
  async.apply(commonUtilities.removeDocs, { multi: true }, d, n, profiler)
], function (err) {
  profiler.step('Benchmark finished')

  if (err) { return console.log('An error was encountered: ', err) }
})
