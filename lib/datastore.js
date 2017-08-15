var customUtils = require('./customUtils')
  , model = require('./model')
  , async = require('async')
  , Executor = require('./executor')
  , Index = require('./indexes')
  , util = require('util')
  , _ = require('underscore')
  , Persistence = require('./persistence')
  , Cursor = require('./cursor')
  , EventEmitter = require('events')
  ;


/**
 * Create a new collection
 *
 * @param {String} options.filename Optional, datastore will be in-memory only if not provided
 * @param {Boolean} options.timestampData Optional, defaults to false. If set to true, createdAt and updatedAt will be created and populated automatically (if not specified by user)
 * @param {Boolean} options.inMemoryOnly Optional, defaults to false
 * @param {Boolean} options.autoload Optional, defaults to false
 * @param {Function} options.onload Optional, if autoload is used this will be called after the load database with the error object as parameter. If you don't pass it the error will be thrown
 * @param {Function} options.afterSerialization/options.beforeDeserialization Optional, serialization hooks
 * @param {Number} options.corruptAlertThreshold Optional, threshold after which an alert is thrown if too much data is corrupt
 * @param {Function} options.compareStrings Optional, string comparison function that overrides default for sorting
 * @param {Object} options.storage Optional, custom storage engine for the database files. Must implement all methods exported by the standard "storage" module included in NestDB
 *
 * @fires Datastore.created
 * @fires Datastore#loaded
 * @fires Datastore#compacted
 * @fires Datastore#inserted
 * @fires Datastore#updated
 * @fires Datastore#removed
 * @fires Datastore#destroyed
 * @fires Datastore.destroyed
 *
 * Event Emitter
 *  - Static Events
 *      - "created": Emitted whenever a Datastore is fully loaded OR newly created
 *          - callback:  function(db) { ... }
 *          - context:   Datastore
 *      - "destroyed": Emitted whenever a Datastore is fully destroyed
 *          - callback:  function(db) { ... }
 *          - context:   Datastore
 *
 *  - Instance Events
 *      - "loaded": Emitted when this Datastore is fully loaded
 *          - callback:  function() { ... }
 *          - context:   this (a.k.a. `db`)
 *      - "compacted": Emitted whenever a compaction operation is completed for this Datastore
 *          - callback:  function() { ... }
 *          - context:   this (a.k.a. `db`)
 *      - "inserted": Emitted whenever a new document is inserted for this Datastore
 *          - callback:  function(newDoc) { ... }
 *          - context:   this (a.k.a. `db`)
 *      - "updated": Emitted whenever an existing document is updated for this Datastore
 *          - callback:  function(newDoc, oldDoc) { ... }
 *          - context:   this (a.k.a. `db`)
 *      - "removed": Emitted whenever an existing document is removed for this Datastore
 *          - callback:  function(oldDoc) { ... }
 *          - context:   this (a.k.a. `db`)
 *      - "destroyed": Emitted when this Datastore is fully destroyed
 *          - callback:  function() { ... }
 *          - context:   this (a.k.a. `db`)
 */
function Datastore(options) {
  if (!(this instanceof Datastore)) {
    return new Datastore(options);
  }

  EventEmitter.call(this);

  options = options || {};
  var filename = options.filename;
  this.inMemoryOnly = options.inMemoryOnly || false;
  this.autoload = options.autoload || false;
  this.timestampData = options.timestampData || false;

  // Determine whether in memory or persistent
  if (!filename || typeof filename !== 'string' || filename.length === 0) {
    this.filename = null;
    this.inMemoryOnly = true;
  } else {
    this.filename = filename;
  }

  // String comparison function
  this.compareStrings = options.compareStrings;

  // Persistence handling
  this.persistence = new Persistence({ db: this
                                      , afterSerialization: options.afterSerialization
                                      , beforeDeserialization: options.beforeDeserialization
                                      , corruptAlertThreshold: options.corruptAlertThreshold
                                      , storage: options.storage
                                      });

  // This new executor is ready if we don't use persistence
  // If we do, it will only be ready once `load` is called
  this.executor = new Executor();
  if (this.inMemoryOnly) { this.executor.ready = true; }

  // Indexed by field name, dot notation can be used
  // _id is always indexed and since _ids are generated randomly the underlying
  // binary is always well-balanced
  this.indexes = {};
  this.indexes._id = new Index({ fieldName: '_id', unique: true });
  this.ttlIndexes = {};

  // Queue a load of the database right away and call the onload handler
  // By default (no onload handler), if there is an error there, no operation will be possible so warn the user by throwing an exception
  if (this.autoload) {
    this.load(options.onload);
  }
}

util.inherits(Datastore, EventEmitter);


//
// Also forcibly alter the Datastore (NeDB) object itself to be a static EventEmitter
//
EventEmitter.call(Datastore);

Object.keys(EventEmitter.prototype)
  .forEach(function (key) {
    if (typeof EventEmitter.prototype[key] === 'function') {
      Datastore[key] = EventEmitter.prototype[key].bind(Datastore);
    }
  });


/**
 * Load the database from the datafile, and trigger the execution of buffered commands if any
 *
 * @fires Datastore.created
 * @fires Datastore#loaded
 * @fires Datastore#compacted
 */
Datastore.prototype.load = function (cb) {
  var self = this
    , callback = cb || function (err) { if (err) { throw err; } }
    , eventedCallback = function (err) {
        async.setImmediate(function () {
          if (!err) {
            // Ensure there are listeners registered before making any unnecessary function calls to `emit`
            if (self.constructor.listeners('created').length > 0) {
              self.constructor.emit('created', self);
            }
            if (self.listeners('loaded').length > 0) {
              self.emit('loaded');
            }
          }
          callback(err);
        });
      };

  this.executor.push({ this: this.persistence, fn: this.persistence.loadDatabase, arguments: [eventedCallback] }, true);
};


/**
 * Backward compatibility with NeDB
 * @deprecated
 */
Datastore.prototype.loadDatabase = Datastore.prototype.load;


/**
 * Get an array of all the data in the database
 */
Datastore.prototype.getAllData = function () {
  return this.indexes._id.getAll();
};


/**
 * Reset all currently defined indexes
 */
Datastore.prototype.resetIndexes = function (newData) {
  var self = this;

  Object.keys(this.indexes).forEach(function (i) {
    self.indexes[i].reset(newData);
  });
};


/**
 * Ensure an index is kept for this field. Same parameters as lib/indexes
 * For now this function is synchronous, we need to test how much time it takes
 * We use an async API for consistency with the rest of the code
 * @param {String} options.fieldName
 * @param {Boolean} options.unique
 * @param {Boolean} options.sparse
 * @param {Number} options.expireAfterSeconds - Optional, if set this index becomes a TTL index (only works on Date fields, not arrays of Date)
 * @param {Function} cb Optional callback, signature: err
 */
Datastore.prototype.ensureIndex = function (options, cb) {
  var err
    , callback = cb || function () {};

  options = options || {};

  if (!options.fieldName) {
    err = new Error("Cannot create an index without a fieldName");
    err.missingFieldName = true;
    return callback(err);
  }
  if (this.indexes[options.fieldName]) { return callback(null); }

  this.indexes[options.fieldName] = new Index(options);
  if (options.expireAfterSeconds !== undefined) { this.ttlIndexes[options.fieldName] = options.expireAfterSeconds; }   // With this implementation index creation is not necessary to ensure TTL but we stick with MongoDB's API here

  try {
    this.indexes[options.fieldName].insert(this.getAllData());
  } catch (e) {
    delete this.indexes[options.fieldName];
    return callback(e);
  }

  // We may want to force all options to be persisted including defaults, not just the ones passed the index creation function
  this.persistence.persistNewState([{ $$indexCreated: options }], function (err) {
    if (err) { return callback(err); }
    return callback(null);
  });
};


/**
 * Remove an index
 * @param {String} fieldName
 * @param {Function} cb Optional callback, signature: err
 */
Datastore.prototype.removeIndex = function (fieldName, cb) {
  var callback = cb || function () {};

  delete this.indexes[fieldName];

  this.persistence.persistNewState([{ $$indexRemoved: fieldName }], function (err) {
    if (err) { return callback(err); }
    return callback(null);
  });
};


/**
 * Add one or several document(s) to all indexes
 */
Datastore.prototype.addToIndexes = function (doc) {
  var i, failingIndex, error
    , keys = Object.keys(this.indexes)
    ;

  for (i = 0; i < keys.length; i += 1) {
    try {
      this.indexes[keys[i]].insert(doc);
    } catch (e) {
      failingIndex = i;
      error = e;
      break;
    }
  }

  // If an error happened, we need to rollback the insert on all other indexes
  if (error) {
    for (i = 0; i < failingIndex; i += 1) {
      this.indexes[keys[i]].remove(doc);
    }

    throw error;
  }
};


/**
 * Remove one or several document(s) from all indexes
 */
Datastore.prototype.removeFromIndexes = function (doc) {
  var self = this;

  Object.keys(this.indexes).forEach(function (i) {
    self.indexes[i].remove(doc);
  });
};


/**
 * Update one or several documents in all indexes
 * To update multiple documents, oldDoc must be an array of { oldDoc, newDoc } pairs
 * If one update violates a constraint, all changes are rolled back
 */
Datastore.prototype.updateIndexes = function (oldDoc, newDoc) {
  var i, failingIndex, error
    , keys = Object.keys(this.indexes)
    ;

  for (i = 0; i < keys.length; i += 1) {
    try {
      this.indexes[keys[i]].update(oldDoc, newDoc);
    } catch (e) {
      failingIndex = i;
      error = e;
      break;
    }
  }

  // If an error happened, we need to rollback the update on all other indexes
  if (error) {
    for (i = 0; i < failingIndex; i += 1) {
      this.indexes[keys[i]].revertUpdate(oldDoc, newDoc);
    }

    throw error;
  }
};


/**
 * Return the list of candidates for a given query
 * Crude implementation for now, we return the candidates given by the first usable index if any
 * We try the following query types, in this order: basic match, $in match, comparison match
 * One way to make it better would be to enable the use of multiple indexes if the first usable index
 * returns too much data. I may do it in the future.
 *
 * Returned candidates will be scanned to find and remove all expired documents
 *
 * @param {Query} query
 * @param {Boolean} dontExpireStaleDocs Optional, defaults to false, if true don't remove stale docs. Useful for the remove function which shouldn't be impacted by expirations
 * @param {Function} callback Signature err, candidates
 */
Datastore.prototype.getCandidates = function (query, dontExpireStaleDocs, callback) {
  var indexNames = Object.keys(this.indexes)
    , self = this
    , usableQueryKeys;

  if (typeof dontExpireStaleDocs === 'function') {
    callback = dontExpireStaleDocs;
    dontExpireStaleDocs = false;
  }


  async.waterfall([
  // STEP 1: get candidates list by checking indexes from most to least frequent usecase
  function (cb) {
    // For a basic match
    usableQueryKeys = [];
    Object.keys(query).forEach(function (k) {
      if (typeof query[k] === 'string' || typeof query[k] === 'number' || typeof query[k] === 'boolean' || util.isDate(query[k]) || query[k] === null) {
        usableQueryKeys.push(k);
      }
    });
    usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
    if (usableQueryKeys.length > 0) {
      return cb(null, self.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]]));
    }

    // For a $in match
    usableQueryKeys = [];
    Object.keys(query).forEach(function (k) {
      if (query[k] && query[k].hasOwnProperty('$in')) {
        usableQueryKeys.push(k);
      }
    });
    usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
    if (usableQueryKeys.length > 0) {
      return cb(null, self.indexes[usableQueryKeys[0]].getMatching(query[usableQueryKeys[0]].$in));
    }

    // For a comparison match
    usableQueryKeys = [];
    Object.keys(query).forEach(function (k) {
      if (query[k] && (query[k].hasOwnProperty('$lt') || query[k].hasOwnProperty('$lte') || query[k].hasOwnProperty('$gt') || query[k].hasOwnProperty('$gte'))) {
        usableQueryKeys.push(k);
      }
    });
    usableQueryKeys = _.intersection(usableQueryKeys, indexNames);
    if (usableQueryKeys.length > 0) {
      return cb(null, self.indexes[usableQueryKeys[0]].getBetweenBounds(query[usableQueryKeys[0]]));
    }

    // By default, return all the DB data
    return cb(null, self.getAllData());
  }
  // STEP 2: remove all expired documents
  , function (docs) {
    if (dontExpireStaleDocs) { return callback(null, docs); }

    var expiredDocsIds = [], validDocs = [], ttlIndexesFieldNames = Object.keys(self.ttlIndexes);

    docs.forEach(function (doc) {
      var valid = true;
      ttlIndexesFieldNames.forEach(function (i) {
        if (doc[i] !== undefined && util.isDate(doc[i]) && Date.now() > doc[i].getTime() + self.ttlIndexes[i] * 1000) {
          valid = false;
        }
      });
      if (valid) { validDocs.push(doc); } else { expiredDocsIds.push(doc._id); }
    });

    async.eachSeries(expiredDocsIds, function (_id, cb) {
      self._remove({ _id: _id }, {}, function (err) {
        if (err) { return callback(err); }
        return cb();
      });
    }, function (err) {
      return callback(null, validDocs);
    });
  }]);
};


/**
 * Insert a new document
 *
 * @private
 * @see Datastore#insert
 */
Datastore.prototype._insert = function (newDoc, cb) {
  var preparedDoc
    , self = this
    , origCallback = cb || function () {}
    , callback = function (err, newDocs) {
        var context = this
          , args = arguments
          ;
        async.setImmediate(function () {
          if (!err) {
            var newDocsArr = util.isArray(newDocs) ? newDocs : [newDocs];

            // Ensure there are listeners registered before making a bunch of unnecessary function calls to `emit`
            if (self.listeners('inserted').length > 0) {
              newDocsArr.forEach(function (newDoc) {
                self.emit('inserted', newDoc);
              });
            }
          }

          origCallback.apply(context, args);
        });
      }
    ;

  try {
    preparedDoc = this.prepareDocumentForInsertion(newDoc)
    this._insertInCache(preparedDoc);
  } catch (e) {
    return callback(e);
  }

  this.persistence.persistNewState(util.isArray(preparedDoc) ? preparedDoc : [preparedDoc], function (err) {
    if (err) { return callback(err); }
    return callback(null, model.deepCopy(preparedDoc));
  });
};


/**
 * Create a new _id that's not already in use
 */
Datastore.prototype.createNewId = function () {
  var tentativeId = customUtils.uid(16);
  // Try as many times as needed to get an unused _id. As explained in customUtils, the probability of this ever happening is extremely small, so this is O(1)
  if (this.indexes._id.getMatching(tentativeId).length > 0) {
    tentativeId = this.createNewId();
  }
  return tentativeId;
};


/**
 * Prepare a document (or array of documents) to be inserted in a database
 * Meaning adds _id and timestamps if necessary on a copy of newDoc to avoid any side effect on user input
 * @private
 */
Datastore.prototype.prepareDocumentForInsertion = function (newDoc) {
  var preparedDoc, self = this;

  if (util.isArray(newDoc)) {
    preparedDoc = [];
    newDoc.forEach(function (doc) { preparedDoc.push(self.prepareDocumentForInsertion(doc)); });
  } else {
    preparedDoc = model.deepCopy(newDoc);
    if (preparedDoc._id === undefined) { preparedDoc._id = this.createNewId(); }
    var now = new Date();
    if (this.timestampData && preparedDoc.createdAt === undefined) { preparedDoc.createdAt = now; }
    if (this.timestampData && preparedDoc.updatedAt === undefined) { preparedDoc.updatedAt = now; }
    model.checkObject(preparedDoc);
  }

  return preparedDoc;
};


/**
 * If newDoc is an array of documents, this will insert all documents in the cache
 * @private
 */
Datastore.prototype._insertInCache = function (preparedDoc) {
  if (util.isArray(preparedDoc)) {
    this._insertMultipleDocsInCache(preparedDoc);
  } else {
    this.addToIndexes(preparedDoc);
  }
};


/**
 * If one insertion fails (e.g. because of a unique constraint), roll back all previous
 * inserts and throws the error
 * @private
 */
Datastore.prototype._insertMultipleDocsInCache = function (preparedDocs) {
  var i, failingI, error;

  for (i = 0; i < preparedDocs.length; i += 1) {
    try {
      this.addToIndexes(preparedDocs[i]);
    } catch (e) {
      error = e;
      failingI = i;
      break;
    }
  }

  if (error) {
    for (i = 0; i < failingI; i += 1) {
      this.removeFromIndexes(preparedDocs[i]);
    }

    throw error;
  }
};


/**
 * Insert a new document
 *
 * @param {Object} newDoc New document to be inserted
 * @param {Function} cb Optional callback, signature: err, insertedDoc
 *
 * @fires Datastore#inserted
 */
Datastore.prototype.insert = function () {
  this.executor.push({ this: this, fn: this._insert, arguments: arguments });
};


/**
 * Count all documents matching the query
 * @param {Object} query MongoDB-style query
 */
Datastore.prototype.count = function(query, callback) {
  var cursor = new Cursor(this, query, function(err, docs, callback) {
    if (err) { return callback(err); }
    return callback(null, docs.length);
  });

  if (typeof callback === 'function') {
    cursor.exec(callback);
  } else {
    return cursor;
  }
};


/**
 * Find all documents matching the query
 * If no callback is passed, we return the cursor so that user can limit, skip and finally exec
 * @param {Object} query MongoDB-style query
 * @param {Object} projection MongoDB-style projection
 */
Datastore.prototype.find = function (query, projection, callback) {
  switch (arguments.length) {
    case 1:
      projection = {};
      // callback is undefined, will return a cursor
      break;
    case 2:
      if (typeof projection === 'function') {
        callback = projection;
        projection = {};
      }   // If not assume projection is an object and callback undefined
      break;
  }

  var cursor = new Cursor(this, query, function(err, docs, callback) {
    var res = [], i;

    if (err) { return callback(err); }

    for (i = 0; i < docs.length; i += 1) {
      res.push(model.deepCopy(docs[i]));
    }
    return callback(null, res);
  });

  cursor.projection(projection);
  if (typeof callback === 'function') {
    cursor.exec(callback);
  } else {
    return cursor;
  }
};


/**
 * Find one document matching the query
 * @param {Object} query MongoDB-style query
 * @param {Object} projection MongoDB-style projection
 */
Datastore.prototype.findOne = function (query, projection, callback) {
  switch (arguments.length) {
    case 1:
      projection = {};
      // callback is undefined, will return a cursor
      break;
    case 2:
      if (typeof projection === 'function') {
        callback = projection;
        projection = {};
      }   // If not assume projection is an object and callback undefined
      break;
  }

  var cursor = new Cursor(this, query, function(err, docs, callback) {
    if (err) { return callback(err); }
    if (docs.length === 1) {
      return callback(null, model.deepCopy(docs[0]));
    } else {
      return callback(null, null);
    }
  });

  cursor.projection(projection).limit(1);
  if (typeof callback === 'function') {
    cursor.exec(callback);
  } else {
    return cursor;
  }
};


/**
 * Update all docs matching query
 *
 * @private
 * @see Datastore#update
 */
Datastore.prototype._update = function (query, updateQuery, options, cb) {
  var callback
    , self = this
    , numReplaced = 0
    , multi
    , upsert
    , i
    ;

  if (typeof options === 'function') { cb = options; options = {}; }
  callback = cb || function () {};
  multi = options.multi !== undefined ? options.multi : false;
  upsert = options.upsert !== undefined ? options.upsert : false;

  async.waterfall([
  function (cb) {   // If upsert option is set, check whether we need to insert the doc
    if (!upsert) { return cb(); }

    // Need to use an internal function not tied to the executor to avoid deadlock
    var cursor = new Cursor(self, query);
    cursor.limit(1)._exec(function (err, docs) {
      if (err) { return callback(err); }
      if (docs.length === 1) {
        return cb();
      } else {
        var toBeInserted;

        try {
          model.checkObject(updateQuery);
          // updateQuery is a simple object with no modifier, use it as the document to insert
          toBeInserted = updateQuery;
        } catch (e) {
          // updateQuery contains modifiers, use the find query as the base,
          // strip it from all operators and update it according to updateQuery
          try {
            toBeInserted = model.modify(model.deepCopy(query, true), updateQuery);
          } catch (err) {
            return callback(err);
          }
        }

        return self._insert(toBeInserted, function (err, newDoc) {
          if (err) { return callback(err); }
          return callback(null, 1, newDoc, true);
        });
      }
    });
  }
  , function () {   // Perform the update
    var modifiedDoc
      , modifications = []
      , createdAt
      , eventedCallback = function(err /*, numReplaced, updatedDocs, upserted */) {
          var context = this
            , args = arguments
            ;
          async.setImmediate(function () {
            if (!err) {
              if (modifications && modifications.length > 0) {
                // Ensure there are listeners registered before making a bunch of unnecessary function calls to `emit`
                if (self.listeners('updated').length > 0) {
                  modifications.forEach(function (mod) {
                    self.emit('updated', mod.newDoc, mod.oldDoc);
                  });
                }
              }
            }

            callback.apply(context, args);
          });
        }
      ;

    self.getCandidates(query, function (err, candidates) {
      if (err) { return eventedCallback(err); }

      // Preparing update (if an error is thrown here neither the datafile nor
      // the in-memory indexes are affected)
      try {
        for (i = 0; i < candidates.length; i += 1) {
          if (model.match(candidates[i], query) && (multi || numReplaced === 0)) {
            numReplaced += 1;
            if (self.timestampData) { createdAt = candidates[i].createdAt; }
            modifiedDoc = model.modify(candidates[i], updateQuery);
            if (self.timestampData) {
              modifiedDoc.createdAt = createdAt;
              modifiedDoc.updatedAt = new Date();
            }
            modifications.push({ oldDoc: candidates[i], newDoc: modifiedDoc });
          }
        }
      } catch (err) {
        return eventedCallback(err);
      }

      // Change the docs in memory
      try {
        self.updateIndexes(modifications);
      } catch (err) {
        return eventedCallback(err);
      }

      // Update the datafile
      var updatedDocs = _.pluck(modifications, 'newDoc');
      self.persistence.persistNewState(updatedDocs, function (err) {
        if (err) { return eventedCallback(err); }
        if (!options.returnUpdatedDocs) {
          return eventedCallback(null, numReplaced);
        } else {
          var updatedDocsDC = [];
          updatedDocs.forEach(function (doc) { updatedDocsDC.push(model.deepCopy(doc)); });
          if (!multi) { updatedDocsDC = updatedDocsDC[0]; }
          return eventedCallback(null, numReplaced, updatedDocsDC);
        }
      });
    });
  }]);
};

/**
 * Update all docs matching query
 *
 * @param {Object} query
 * @param {Object} updateQuery
 * @param {Object} options Optional options
 *                 options.multi If true, can update multiple documents (defaults to false)
 *                 options.upsert If true, document is inserted if the query doesn't match anything
 *                 options.returnUpdatedDocs Defaults to false, if true return as third argument the array of updated matched documents (even if no change actually took place)
 * @param {Function} cb Optional callback, signature: (err, numAffected, affectedDocuments, upsert)
 *                      If update was an upsert, upsert flag is set to `true`
 *                      affectedDocuments can be one of the following:
 *                        * For an upsert, the upserted document
 *                        * For an update with returnUpdatedDocs option false, null
 *                        * For an update with returnUpdatedDocs true and multi false, the updated document
 *                        * For an update with returnUpdatedDocs true and multi true, the array of updated documents
 *
 * @fires Datastore#updated
 * @fires Datastore#inserted
 */
Datastore.prototype.update = function () {
  this.executor.push({ this: this, fn: this._update, arguments: arguments });
};


/**
 * Remove all docs matching the query
 *
 * @private
 * @see Datastore#remove
 */
Datastore.prototype._remove = function (query, options, cb) {
  var origCallback
    , callback
    , self = this
    , numRemoved = 0
    , removalDocs = []
    , removedFullDocs = []
    , multi
    ;

  if (typeof options === 'function') { cb = options; options = {}; }
  origCallback = cb || function () {};
  multi = options.multi !== undefined ? options.multi : false;

  callback = function(err /*, numRemoved */) {
    var context = this
      , args = arguments
      ;
    async.setImmediate(function () {
      if (!err) {
        if (removedFullDocs && removedFullDocs.length > 0) {
          // Ensure there are listeners registered before making a bunch of unnecessary function calls to `emit`
          if (self.listeners('removed').length > 0) {
            removedFullDocs.forEach(function (oldDoc) {
              self.emit('removed', oldDoc);
            });
          }
        }
      }

      origCallback.apply(context, args);
    });
  };

  this.getCandidates(query, true, function (err, candidates) {
    if (err) { return callback(err); }

    try {
      candidates.forEach(function (d) {
        if (model.match(d, query) && (multi || numRemoved === 0)) {
          numRemoved += 1;
          removalDocs.push({ $$deleted: true, _id: d._id });
          removedFullDocs.push(d);
          self.removeFromIndexes(d);
        }
      });
    } catch (err) { return callback(err); }

    self.persistence.persistNewState(removalDocs, function (err) {
      if (err) { return callback(err); }
      return callback(null, numRemoved);
    });
  });
};


/**
 * Remove all docs matching the query
 *
 * @param {Object} query
 * @param {Object} options Optional options
 *                 options.multi If true, can update multiple documents (defaults to false)
 * @param {Function} cb Optional callback, signature: err, numRemoved
 *
 * @fires Datastore#removed
 */

Datastore.prototype.remove = function () {
  this.executor.push({ this: this, fn: this._remove, arguments: arguments });
};


/**
 * Remove all docs and then destroy the database's persistent datafile, if any
 *
 * @param {Function} cb Optional callback, signature: err
 *
 * @private
 * @see Datastore#destroy
 */
Datastore.prototype._destroy = function (cb) {
  var self = this
    , callback = cb || function () {};

  self._remove({}, { multi: true }, function (err /*, numRemoved */) {
    if (err) {
      return callback(err);
    }

    self.persistence.destroyDatabase(callback);
  });
};


/**
 * Remove all docs and then destroy the database's persistent datafile, if any
 *
 * @param {Function} cb Optional callback, signature: err
 *
 * @fires Datastore#destroyed
 * @fires Datastore.destroyed
 */
Datastore.prototype.destroy = function (cb) {
  var self = this
    , callback = cb || function () {}
    , eventedCallback = function (err) {
        async.setImmediate(function () {
          if (!err) {
            // Ensure there are listeners registered before making any unnecessary function calls to `emit`
            if (self.listeners('destroyed').length > 0) {
              self.emit('destroyed');
            }
            if (self.constructor.listeners('destroyed').length > 0) {
              self.constructor.emit('destroyed', self);
            }
          }
          callback(err);
        });
      };

  this.executor.push({ this: this, fn: this._destroy, arguments: [eventedCallback] });
};


/**
 * Augment `Datastore` with user-defined extensions/patches
 * @param {Function|Object} ext Must either be a function that takes one argument of `Datastore`, or a non-empty object
 */
Datastore.plugin = function (ext) {
  if (typeof ext === 'function') { // function style for plugins
    ext(Datastore);
  } else if (typeof ext !== 'object' || !ext || Object.keys(ext).length === 0){
    throw new Error('Invalid plugin: got \"' + ext + '\", expected an object or a function');
  } else {
    Object.keys(ext).forEach(function (id) { // object style for plugins
      Datastore.prototype[id] = ext[id];
    });
  }
  return Datastore;
};



module.exports = Datastore;
