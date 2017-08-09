/**
 * Build the browser version of NestDB
 */

var path = require('path')
  , fs = require('fs-extra')
  , child_process = require('child_process')
  , async = require('async')
  , browserify = require('browserify')
  , getStream = require('get-stream')
  , uglify = require('uglify-js')
  , toCopy = ['lib', 'node_modules']
  ;

// Ensuring "node_modules", "src", and "out" directories exist
function ensureDirExists (name) {
  try {
    fs.mkdirSync(path.join(__dirname, name));
  } catch (e) {
    if (e.code !== 'EEXIST') {
      console.log("Error ensuring that node_modules exists");
      process.exit(1);
    }
  }
}

ensureDirExists('../node_modules');
ensureDirExists('out');
ensureDirExists('src');


async.waterfall(
  [
    function (cb) {
      console.log("Installing NPM dependencies if needed");

      child_process.exec('npm install', { cwd: path.join(__dirname, '..') }, function (err) { return cb(err); });
    }
  , function (cb) {
      console.log("Installing Bower dependencies if needed");

      child_process.exec('bower install', { cwd: __dirname }, function(err) { return cb(err); });
    }
  , function (cb) {
      console.log("Removing contents of the src directory");

      async.eachSeries(fs.readdirSync(path.join(__dirname, 'src')), function (item, _cb) {
        fs.remove(path.join(__dirname, 'src', item), _cb);
      }, cb);
    }
  , function (cb) {
      console.log("Copying source files");

      async.eachSeries(toCopy, function (item, _cb) {
        fs.copy(path.join(__dirname, '..', item), path.join(__dirname, 'src', item), _cb);
      }, cb);
    }
  , function (cb) {
      console.log("Copying browser specific files to replace their server-specific counterparts");

      async.eachSeries(fs.readdirSync(path.join(__dirname, 'browser-specific')), function (item, _cb) {
        fs.copy(path.join(__dirname, 'browser-specific', item), path.join(__dirname, 'src', item), _cb);
      }, cb);
    }
  , function (cb) {
      console.log("Browserifying the code");

      var srcPath = path.join(__dirname, 'src/lib/datastore.js')
        , bundleStream = browserify(srcPath, { standalone: 'NestDB' }).bundle()
        ;

        return getStream(bundleStream)
          .then(function (out) {
            fs.writeFile(path.join(__dirname, 'out/nestdb.js'), out, 'utf8', function (err) {
              if (err) {
                return cb(err);
              } else {
                return cb(null, out);
              }
            });
          })
          .catch(cb);
    }
  , function (out, cb) {
      console.log("Creating the minified version");

      var compressedCode = uglify.minify(out);
      fs.writeFile(path.join(__dirname, 'out/nestdb.min.js'), compressedCode.code, 'utf8', cb);
    }
  ],
  function (err) {
    if (err) {
      console.log("Error during build");
      console.log(err);
    } else {
      console.log("Build finished with success");
    }
  }
);
