/**
 * Way data is stored for this database
 * For a Node.js/Node Webkit database it's the file system
 * For a browser-side database it's localforage which chooses the best option depending on user browser (IndexedDB then WebSQL then localStorage)
 *
 * This version is the Node.js/Node Webkit version
 * It's essentially fs, mkdirp and crash safe write and read functions
 */

// Node.js core modules
var fs = require('fs')
  , path = require('path')

// Userland modules
  , mkdirp = require('mkdirp')
  , async = require('async')


// Local variables
  , storage = {}
  ;


/**
 * Read the file asynchronously, providing the data to a callback
 * @public
 */
storage.read = function (file, callback) {
  fs.readFile(file, { encoding: 'utf8' }, callback);
};


/**
 * Append new data to the file asynchronously
 * @public
 */
storage.append = function (file, data, callback) {
  fs.appendFile(file, data, { encoding: 'utf8' }, callback);
};


/**
 * Delete the file
 * @public
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

  /**
   * Some OSes and/or storage backends (augmented node fs) do not support fsync (FlushFileBuffers) directories,
   * or calling open() on directories at all. Flushing fails silently in this case, supported by following heuristics:
   *  + isDir === true
   *  |-- open(<dir>) -> (err.code === 'EISDIR'): can't call open() on directories (eg. BrowserFS)
   *  `-- fsync(<dir>) -> (errFS.code === 'EPERM' || errFS.code === 'EISDIR'): can't fsync directory: permissions are checked
   *        on open(); EPERM error should only occur on fsync incapability and not for general lack of permissions (e.g. Windows)
   * 
   * We can live with this as it cannot cause 100% dataloss except in the very rare event of the first time
   * database is loaded and a crash happens.
   */

  fs.open(filename, flags, function (err, fd) {
    if (err) {
      return callback((err.code === 'EISDIR' && options.isDir) ? null : err);
    }
    fs.fsync(fd, function (errFS) {
      fs.close(fd, function (errC) {
        if ((errFS || errC) && !((errFS.code === 'EPERM' || errFS.code === 'EISDIR') && options.isDir)) {
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
