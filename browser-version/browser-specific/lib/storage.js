/**
 * How data is stored for this database
 * For a Node.js/Node Webkit database it's the file system
 * For a browser-side database it's localforage, which uses the best backend available (IndexedDB then WebSQL then localStorage)
 *
 * This version is the browser version
 */

var localforage = require('localforage')
  , storage = {}
  ;

// Configure localforage to display NestDB name for now. Would be a good idea to let user use his own app name
localforage.config({
  name: 'NestDB'
, storeName: 'nestdbdata'
});


// No need for a crash-safe function in the browser
storage.write = function (filename, contents, callback) {
  localforage.setItem(filename, contents, callback);
};


storage.append = function (filename, toAppend, callback) {
  localforage.getItem(filename, function (err, contents) {
    if (err) {
      return callback(err);
    }
    contents = contents || '';
    contents += toAppend;
    localforage.setItem(filename, contents, callback);
  });
};


storage.read = function (filename, callback) {
  localforage.getItem(filename, function (err, contents) {
    if (err) {
      return callback(err);
    }
    callback(null, contents || '');
  });
};


storage.remove = function (filename, callback) {
  localforage.removeItem(filename, callback);
};


// Nothing to do because:
//  - no data corruption is possible in the browser
//  - no directory creation is possible in the browser
//  - no need to initialize an empty "file" for the datastore in the browser
storage.init = function (filename, callback) {
  return callback(null);
};


// Interface
module.exports.init = storage.init;
module.exports.read = storage.read;
module.exports.write = storage.write;
module.exports.append = storage.append;
module.exports.remove = storage.remove;
