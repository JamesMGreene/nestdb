/* eslint-env node, mocha */

// Node.js core modules
import { open, closeSync } from 'fs'

// Userland modules

import { whilst, waterfall } from 'async'

// Local modules

import NestDB from '..'

// Local variables

var db = new NestDB({ filename: './workspace/openfds.db', autoload: true })

var N = 64 // Half the allowed file descriptors
var i; var fds

function multipleOpen (filename, N, callback) {
  whilst(function () { return i < N }
    , function (cb) {
      open(filename, 'r', function (err, fd) {
        i += 1
        if (fd) { fds.push(fd) }
        return cb(err)
      })
    }
    , callback)
}

waterfall([
  // Check that ulimit has been set to the correct value
  function (cb) {
    i = 0
    fds = []
    multipleOpen('./test_lac/openFdsTestFile', 2 * N + 1, function (err) {
      if (!err) { console.log('No error occured while opening a file too many times') }
      fds.forEach(function (fd) { closeSync(fd) })
      return cb()
    })
  },
  function (cb) {
    i = 0
    fds = []
    multipleOpen('./test_lac/openFdsTestFile2', N, function (err) {
      if (err) { console.log('An unexpected error occured when opening file not too many times: ' + err) }
      fds.forEach(function (fd) { closeSync(fd) })
      return cb()
    })
  },
  // Then actually test NestDB persistence
  function () {
    db.remove({}, { multi: true }, function (err) {
      if (err) { console.log(err) }
      db.insert({ hello: 'world' }, function (err) {
        if (err) { console.log(err) }

        i = 0
        whilst(function () { return i < 2 * N + 1 }
          , function (cb) {
            db.persistence.persistCachedDatabase(function (err) {
              if (err) { return cb(err) }
              i += 1
              return cb()
            })
          }
          , function (err) {
            if (err) { console.log('Got unexpected error during one peresistence operation: ' + err) }
          }
        )
      })
    })
  }
])
