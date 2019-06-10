// Userland modules
var async = require('async')

var execTime = require('exec-time')

var program = require('commander')

// Local modules

var Datastore = require('../lib/datastore')

var commonUtilities = require('./commonUtilities')

// Local variables

var benchDb = 'workspace/insert.bench.db'

var profiler = new execTime('INSERT BENCH')

var d = new Datastore(benchDb)

var n

program
  .option('-n --number [number]', 'Size of the collection to test on', parseInt)
  .option('-i --with-index', 'Test with an index')
  .parse(process.argv)

n = program.number || 10000

console.log('----------------------------')
console.log('Test with ' + n + ' documents')
console.log('----------------------------')

async.waterfall([
  async.apply(commonUtilities.prepareDb, benchDb),
  function (cb) {
    d.loadDatabase(function (err) {
      if (err) { return cb(err) }
      cb()
    })
  },
  function (cb) { profiler.beginProfiling(); return cb() },
  async.apply(commonUtilities.insertDocs, d, n, profiler),
  function (cb) {
    var i

    profiler.step('Begin calling ensureIndex ' + n + ' times')

    for (i = 0; i < n; i += 1) {
      d.ensureIndex({ fieldName: 'docNumber' })
      delete d.indexes.docNumber
    }

    console.log('Average time for one ensureIndex: ' + (profiler.elapsedSinceLastStep() / n) + 'ms')
    profiler.step('Finished calling ensureIndex ' + n + ' times')
  }
], function (err) {
  profiler.step('Benchmark finished')

  if (err) { return console.log('An error was encountered: ', err) }
})
