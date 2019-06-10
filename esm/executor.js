global.require = require('esm')(module)

module.exports = global.require('../lib/executor').default
