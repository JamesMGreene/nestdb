/**
 * Handle every persistence-related task
 * The interface Datastore expects to be implemented is
 * * Persistence.loadDatabase(callback) and callback has signature err
 * * Persistence.persistNewState(newDocs, callback) where newDocs is an array of documents and callback has signature err
 */

// Userland modules
var async = require('async')

  // Local modules
  , defaultStorage = require('./storage')
  , model = require('./model')
  , customUtils = require('./customUtils')
  , Index = require('./indexes')
  ;


/**
 * Create a new Persistence object for database options.db
 * @param {Datastore} options.db
 * @param {Object} options.storage Optional, custom storage engine for the database files. Must implement all methods exported by the standard "storage" module included in NestDB
 */
function Persistence(options, done) {
  
  var i, j, randomString,self=this;

  this.db = options.db;
  this.inMemoryOnly = this.db.inMemoryOnly;
  this.filename = this.db.filename;
  this.corruptAlertThreshold = options.corruptAlertThreshold !== undefined ? options.corruptAlertThreshold : 0.1;
  this.storage = options.storage || defaultStorage;

  if (!this.inMemoryOnly && this.filename && this.filename.charAt(this.filename.length - 1) === '~') {
    throw new Error("The datafile name can't end with a ~, which is reserved for crash safe backup files");
  }

  // After serialization and before deserialization hooks with some basic sanity checks
  if (options.afterSerialization && !options.beforeDeserialization) {
    throw new Error("Serialization hook defined but deserialization hook undefined, cautiously refusing to start NestDB to prevent dataloss");
  }
  if (!options.afterSerialization && options.beforeDeserialization) {
    throw new Error("Serialization hook undefined but deserialization hook defined, cautiously refusing to start NestDB to prevent dataloss");
  }
  this.afterSerialization = options.afterSerialization || function (s) { return s; };
  this.beforeDeserialization = options.beforeDeserialization || function (s) { return s; };
  for (i = 1; i < 30; i += 1) {
    for (j = 0; j < 10; j += 1) {
      randomString = customUtils.uid(i);
      if (this.beforeDeserialization(this.afterSerialization(randomString)) !== randomString) {
        throw new Error("beforeDeserialization is not the reverse of afterSerialization, cautiously refusing to start NestDB to prevent dataloss");
      }
    }
  }
  if (!this.inMemoryOnly && this.db.encrypt && this.storage.initJRE) {
    this.storage.initJRE(this.db.encrypt).then(function () {
      self.db.emit('encryptionReady');
      //console.log('Encrypted storage ready')
    });
  }
  return this;
};


/**
 * Check if a directory exists and create it on the fly if it is not the case
 * cb is optional, signature: err
 * @deprecated Use `require('mkdirp')` instead
 */
Persistence.prototype.ensureDirectoryExists = function (dir, cb) {
  throw new Error('Deprecated! Use `require("mkdirp")` instead.');
};


/**
 * Persist cached database
 * This serves as a compaction function since the cache always contains only the number of documents in the collection
 * while the data file is append-only so it may grow larger
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.persistCachedDatabase = function (cb) {
  var callback = cb || function () { }
    , toPersist = ''
    , self = this
    ;

  if (this.inMemoryOnly) { return callback(null); }

  this.db.getAllData().forEach(function (doc) {
    toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
  });
  Object.keys(this.db.indexes).forEach(function (fieldName) {
    if (fieldName != "_id") {   // The special _id index is managed by datastore.js, the others need to be persisted
      toPersist += self.afterSerialization(model.serialize({ $$indexCreated: { fieldName: fieldName, unique: self.db.indexes[fieldName].unique, sparse: self.db.indexes[fieldName].sparse } })) + '\n';
    }
  });

  this.storage.write(this.filename, toPersist, function (err) {
    if (err) { return callback(err); }
    self.db.emit('compacted');
    return callback(null);
  });
};


/**
 * Queue a rewrite of the datafile
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.compactDatafile = function () {
  this.db.executor.push({ this: this, fn: this.persistCachedDatabase, arguments: arguments });
};


/**
 * Set automatic compaction every interval ms
 * @param {Number} interval in milliseconds, with an enforced minimum of 5 seconds
 */
Persistence.prototype.setAutocompactionInterval = function (interval) {
  var self = this
    , minInterval = 5000
    , realInterval = Math.max(interval || 0, minInterval)
    ;

  this.stopAutocompaction();

  this.autocompactionIntervalId = setInterval(function () {
    self.compactDatafile();
  }, realInterval);
};


/**
 * Stop autocompaction (do nothing if autocompaction was not running)
 */
Persistence.prototype.stopAutocompaction = function () {
  if (this.autocompactionIntervalId) { clearInterval(this.autocompactionIntervalId); }
};


/**
 * Persist new state for the given newDocs (can be insertion, update or removal)
 * Use an append-only format
 * @param {Array} newDocs Can be empty if no doc was updated/removed
 * @param {Function} cb Optional, signature: err
 */
Persistence.prototype.persistNewState = function (newDocs, cb) {
  var self = this
    , toPersist = ''
    , callback = cb || function () { }
    ;

  // In-memory only datastore
  if (self.inMemoryOnly) { return callback(null); }

  newDocs.forEach(function (doc) {
    toPersist += self.afterSerialization(model.serialize(doc)) + '\n';
  });

  if (toPersist.length === 0) { return callback(null); }

  this.storage.append(self.filename, toPersist, function (err) {
    return callback(err);
  });
};


/**
 * From a database's raw data, return the corresponding
 * machine understandable collection
 */
Persistence.prototype.treatRawData = function (rawData) {
  var data = rawData.trim().split('\n')
    , dataById = {}
    , tdata = []
    , i
    , indexes = {}
    , corruptData=[]
    , corruptItems = 0   // Last line of every data file is usually blank so not really corrupt
    ;
  for (i = 0; i < data.length; i ++) {
    var doc;
    if(!data[i]) continue;

    try {
      doc = model.deserialize(this.beforeDeserialization(data[i]));
      if (doc._id) {
        if (doc.$$deleted === true) {
          delete dataById[doc._id];
        } else {
          dataById[doc._id] = doc;
        }
      } else if (doc.$$indexCreated && doc.$$indexCreated.fieldName != undefined) {
        indexes[doc.$$indexCreated.fieldName] = doc.$$indexCreated;
      } else if (typeof doc.$$indexRemoved === "string") {
        delete indexes[doc.$$indexRemoved];
      }
    } catch (e) {
      corruptData.push(data[i]);
      //console.error('treatRawData error:'+e.message,'type:'+typeof data[i]+' of '+data[i].length,data[i])
      corruptItems ++;
    }
  }
  //console.log('Corrupt items:'+corruptItems)
  // A bit lenient on corruption
  if (data.length > 0 && corruptItems / data.length > this.corruptAlertThreshold) {
    console.log('corruptData',corruptData)
    throw new Error("More than " + Math.floor(100 * this.corruptAlertThreshold) + "% of the data file is corrupt, the wrong "+
                "beforeDeserialization hook may be used. Cautiously refusing to start NestDB to prevent dataloss. (found "+corruptItems+")");
  }

  Object.keys(dataById).forEach(function (k) {
    tdata.push(dataById[k]);
  });

  return { data: tdata, indexes: indexes };
};


/**
 * Load the database
 * 1) Create all indexes
 * 2) Insert all data
 * 3) Compact the database
 * This means pulling data out of the data file or creating it if it doesn't exist
 * Also, all data is persisted right away, which has the effect of compacting the database file
 * This operation is very quick at startup for a big collection (60ms for ~10k docs)
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.loadDatabase = function (cb) {
  var callback = cb || function () { }
    , self = this
    ;

  self.db.resetIndexes();

  // In-memory only datastore
  if (self.inMemoryOnly) { return callback(null); }

  async.waterfall([
    function (cb) {
      self.storage.init(self.filename, function (err) {
        if (err) { return cb(err); }

        self.storage.read(self.filename, function (err, rawData) {
          if (err) { return cb(err); }
          //console.log('\n\nrawData for '+self.filename,rawData);
          try {
            var treatedData = self.treatRawData(rawData);
          } catch (e) {
            return cb(e);
          }
          //console.log('treatedData for '+self.filename,treatedData,'\n\n');

          // Recreate all indexes in the datafile
          Object.keys(treatedData.indexes).forEach(function (key) {
            self.db.indexes[key] = new Index(treatedData.indexes[key]);
          });

          // Fill cached database (i.e. all indexes) with data
          try {
            self.db.resetIndexes(treatedData.data);
          } catch (e) {
            self.db.resetIndexes();   // Rollback any index which didn't fail
            return cb(e);
          }

          self.db.persistence.persistCachedDatabase(cb);
        });
      });
    }
  ], function (err) {
    if (err) { return callback(err); }

    self.db.executor.processBuffer();
    return callback(null);
  });
};


/**
 * Destroy the database
 * This means destroying the data file if it exists
 *
 * @param {Function} cb Optional callback, signature: err
 */
Persistence.prototype.destroyDatabase = function (cb) {
  var callback = cb || function () { }
    , self = this
    ;

  self.db.resetIndexes();

  // In-memory only datastore
  if (self.inMemoryOnly) { return callback(null); }

  self.storage.remove(self.filename, callback);
};



// Interface
module.exports = Persistence;
