/**
 * Way data is stored for this database
 * For a Node.js/Node Webkit database it's the file system
 * For a browser-side database it's localforage which chooses the best option depending on user browser (IndexedDB then WebSQL then localStorage)
 *
 * This version is the Node.js/Node Webkit version
 * It's essentially fs, mkdirp and crash safe write and read functions
 */

var fs = require('fs')
  , mkdirp = require('mkdirp')
  , async = require('async')
  , path = require('path')
  , through2 = require('through2')
  , storage = {}
  ;


/**
 * Read the file in a streaming fashion, providing the data via standard
 * Readable Stream event emissions: "data", "error", "end"
 */
storage.readAsStream = function (file) {
  return fs.createReadStream(file, { encoding: 'utf8', autoClose: true });
};


/**
 * Read the file asynchronously, providing the data to a callback
 */
storage.read = function (file, callback) {
  fs.readFile(file, { encoding: 'utf8' }, callback);
};


/**
 * Append new data to the file asynchronously
 */
storage.append = function (file, data, callback) {
  fs.appendFile(file, data, { encoding: 'utf8' }, callback);
};


/**
 * Delete the file
 */
storage.remove = function (file, callback) {
  fs.exists(file, function (exists) {
    if (!exists) { return callback(null); }

    fs.unlink(file, function (err) { return callback(err); });
  });
};


/**
 * Flush data in OS buffer to storage if corresponding option is set
 * @param {String} options.filename
 * @param {Boolean} options.isDir Optional, defaults to false
 * @private
 */
function flushToStorage(options, callback) {
  var filename = options.filename
    , flags = options.isDir ? 'r' : 'r+';

  // Windows can't fsync (FlushFileBuffers) directories. We can live with this as it cannot cause 100% dataloss
  // except in the very rare event of the first time database is loaded and a crash happens
  if (flags === 'r' && (process.platform === 'win32' || process.platform === 'win64')) { return callback(null); }

  fs.open(filename, flags, function (err, fd) {
    if (err) { return callback(err); }
    fs.fsync(fd, function (errFS) {
      fs.close(fd, function (errC) {
        if (errFS || errC) {
          var e = new Error('Failed to flush to storage');
          e.errorOnFsync = errFS;
          e.errorOnClose = errC;
          return callback(e);
        } else {
          return callback(null);
        }
      });
    });
  });
}


/**
 * Fully write or rewrite the datafile in a streaming fashion, immune to crashes during the write operation (data will not be lost)
 * @param {String} filename
 */
storage.writeAsStream = function (filename) {
  var tempFilename = filename + '~'
    , firstWrite = true
    , outStream = fs.createWriteStream(tempFilename, { defaultEncoding: 'utf8', autoClose: true })
    , transformer = through2(
        function (chunk, enc, callback) {
          if (!firstWrite) {
            return callback(null, chunk);
          }

          firstWrite = false;

          async.waterfall(
            [
              async.apply(flushToStorage, { filename: path.dirname(filename), isDir: true })
            , function (cb) {
                fs.exists(filename, function (exists) {
                  if (exists) {
                    flushToStorage({ filename: filename }, function (err) { return cb(err); });
                  } else {
                    return cb();
                  }
                });
              }
            ]
          , function (err) {
              return callback(err, chunk);
            }
          );
        },
        function (callback) {
          async.waterfall(
            [
              async.apply(flushToStorage, { filename: tempFilename })
            , function (cb) {
                fs.rename(tempFilename, filename, function (err) { return cb(err); });
              }
            , async.apply(flushToStorage, { filename: path.dirname(filename), isDir: true })
            ],
            callback
          );
        }
      )
    ;

  transformer.pipe(outStream);

  return transformer;
};


/**
 * Fully write or rewrite the datafile, immune to crashes during the write operation (data will not be lost)
 * @param {String} filename
 * @param {String} data
 * @param {Function} cb Optional callback, signature: err
 */
storage.write = function (filename, data, cb) {
  var callback = cb || function () {}
    , tempFilename = filename + '~';

  async.waterfall([
    async.apply(flushToStorage, { filename: path.dirname(filename), isDir: true })
  , function (cb) {
      fs.exists(filename, function (exists) {
        if (exists) {
          flushToStorage({ filename: filename }, function (err) { return cb(err); });
        } else {
          return cb();
        }
      });
    }
  , function (cb) {
      fs.writeFile(tempFilename, data, { encoding: 'utf8' }, function (err) { return cb(err); });
    }
  , async.apply(flushToStorage, { filename: tempFilename })
  , function (cb) {
      fs.rename(tempFilename, filename, function (err) { return cb(err); });
    }
  , async.apply(flushToStorage, { filename: path.dirname(filename), isDir: true })
  ], function (err) { return callback(err); })
};


/**
 * Ensure the datafile contains all the data, even if there was a crash during a full file write,
 * or else create a new empty datafile and any missing ancestor directories in its path
 * @param {String} filename
 * @param {Function} callback signature: err
 */
storage.init = function (filename, callback) {
  var tempFilename = filename + '~';

  fs.exists(filename, function (filenameExists) {
    // Write was successful
    if (filenameExists) { return callback(null); }

    fs.exists(tempFilename, function (oldFilenameExists) {
      // Write failed, use old version
      if (oldFilenameExists) {
        return fs.rename(tempFilename, filename, function (err) { return callback(err); });
      }

      // New database!
      // Ensure that all of its ancestor directories exist
      mkdirp(path.dirname(filename), function (err) {
        if (err) {
          return callback(err);
        }
        // And then create an empty datafile
        return fs.writeFile(filename, '', { encoding: 'utf8' }, function (err) { callback(err); });
      });
    });
  });
};



// Interface
module.exports = storage;
