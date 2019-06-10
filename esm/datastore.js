global.require = require('esm')(module)

const Datastore = global.require('../lib/datastore').default

module.exports = Datastore
