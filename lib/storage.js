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
	, storage = {encryptionEnabled:false}
	, jwe = require('@adorsys/jwe-codec'), jre;
storage.initJRE = async function (e) {
	storage.encryptionEnabled=true;
	if (typeof e == 'string' && e.indexOf('{') === 0) e = JSON.parse(e);
	jre = await jwe(e);
	return jre;
}
storage.decrypt = function (d, n) {
	if (d && typeof d!='string') d = d.toString();
	if(storage.encryptionEnabled && d && d.indexOf('ENC:') != -1){
		if(!jre){
			setTimeout(function(){
				storage.decrypt(d,n);
			},1000);
			return;
		}
		if (d.indexOf('\n') != -1) {
			let out = [];
			async.eachSeries(d.split('\n'), function (l, nl) {
				if (/^ENC:/i.test(l)) {
					jre.decrypt(l.replace(/^ENC:/, '')).then(r => {
						out.push(r.trim());
						nl();
					});
					return;
				}
				out.push(l);
				nl();
			}, function () {
				n(out.join('\n'));
			})
			return;
		}
		jre.decrypt(d.replace(/^ENC:/, '')).then(r => {
			n(r);
		});
		return;
	}
	n(d);
}
storage.encrypt = function (d, n) {
	if(storage.encryptionEnabled && d){
		if(!jre){
			setTimeout(function(){
				storage.encrypt(d,n);
			},1000);
			return;
		}
		jre.encrypt(d).then(r => {
			n('ENC:' + r);
		});
		return;
	}
	n(d);
}

/**
 * Read the file asynchronously, providing the data to a callback
 * @public
 */
storage.read = function (file, callback) {
	fs.readFile(file, { encoding: 'utf8' }, function (err, data) {
		if (err) console.error(err);
		if (!data || err) {
			callback(err, data);
			return;
		}
		data = data.toString().trim().replace(/^\n/, '');
		storage.decrypt(data, function (d) {
			callback(err, d);
		});
	});
};


/**
 * Append new data to the file asynchronously
 * @public
 */
storage.append = function (file, data, callback) {
	storage.encrypt(data, function (r) {
		fs.appendFile(file, r, { encoding: 'utf8' }, callback);
	});
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
 * Fully write or rewrite the datafile, immune to crashes during the write operation (data will not be lost)
 * @param {String} filename
 * @param {String} data
 * @param {Function} cb Optional callback, signature: err
 */
storage.write = function (filename, data, cb) {
	var callback = cb || function () { }
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
			storage.encrypt(data, function (r) {
				fs.writeFile(tempFilename, r, { encoding: 'utf8' }, function (err) { return cb(err); });
			});
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
