// Userland modules
var async = require('async')

var execTime = require('exec-time')

var program = require('commander')

// Local modules

var Datastore = require('../lib/datastore')

var commonUtilities = require('./commonUtilities')

// Local variables

var benchDb = 'workspace/loaddb.bench.db'

var profiler = new execTime('LOADDB BENCH')

var d = new Datastore(benchDb)

var n

program
  .option('-n --number [number]', 'Size of the collection to test on', parseInt)
  .option('-i --with-index', 'Test with an index')
  .parse(process.argv)

n = program.number || 10000

console.log('----------------------------')
console.log('Test with ' + n + ' documents')
console.log(program.withIndex ? 'Use an index' : "Don't use an index")
console.log('----------------------------')

async.waterfall([
  async.apply(commonUtilities.prepareDb, benchDb),
  function (cb) {
    d.loadDatabase(cb)
  },
  function (cb) { profiler.beginProfiling(); return cb() },
  async.apply(commonUtilities.insertDocs, d, n, profiler),
  async.apply(commonUtilities.loadDatabase, d, n, profiler)
], function (err) {
  profiler.step('Benchmark finished')

  if (err) { return console.log('An error was encountered: ', err) }
})
