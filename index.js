const debug = require('debug')('db')
const pretty = require('pretty-hash')

const DB = require('./db')

const { uuid, sink, noop } = require('./lib/util')
const createKvView = require('./views/kv')
const createRecordsView = require('./views/records')
const createIndexView = require('./views/indexes')
const Record = require('./lib/record')
const Schema = require('./lib/schema')

const FEED_TYPE = 'kappa-records'

module.exports = function defaultDatabase (opts = {}) {
  opts.swarmMode = 'rootfeed'
  // opts.swarmMode = 'multifeed'
  const db = new DB(opts)
  db.useMiddleware('db', databaseMiddleware(db))
  db.useMiddleware('sources', sourcesMiddleware(db))

  // Assign for backwards compatibility.
  db.put = db.api.db.put
  db.del = db.api.db.del
  db.putSchema = db.api.db.putSchema
  db.getSchema = db.api.db.getSchema
  db.getSchemas = db.api.db.getSchemas
  db.schemas = db.api.db.schemas
  db.putSource = db.api.sources.putSource

  return db
}

function sourcesMiddleware (db) {
  return {
    open (cb) {
      const qs = db.createQueryStream('records', { schema: 'core/source' }, { live: true })
      qs.once('sync', cb)
      qs.pipe(sink((record, next) => {
        const { alias, key, type } = record.value
        if (type !== FEED_TYPE) return next()
        debug(`[%s] source:add key %s alias %s type %s`, db._name, pretty(key), alias, type)
        db.addFeed({ key })
        next()
      }))
    },
    api: {
      putSource (key, opts = {}, cb) {
        // opts should/can include: { alias }
        if (typeof opts === 'function') {
          cb = opts
          opts = {}
        }
        if (Buffer.isBuffer(key)) key = key.toString('hex')
        const record = {
          schema: 'core/source',
          id: key,
          value: {
            type: 'kappa-records',
            key,
            ...opts
          }
        }
        db.put(record, cb)
      }
    }
  }
}

function databaseMiddleware (db, opts = {}) {
  const schemas = new Schema({ key: db.key })
  const self = this
  self.schemas = schemas
  self.opts = opts
  const databaseMiddleware = {
    open: function (cb) {
      self.schemas.open(db, cb)
    },

    views: {
      kv: createKvView,
      records: createRecordsView,
      index: createIndexView
    },

    onload: function (message, cb) {
      const { key, seq, lseq, value, feedType } = message
      if (feedType !== FEED_TYPE) return cb(null, message)
      const record = Record.decode(value, { key, seq, lseq })
      cb(null, record)
    },

    api: {
      put: function (record, opts, cb) {
        if (typeof opts === 'function') {
          cb = opts
          opts = {}
        }
        if (!opts) opts = {}
        if (!cb) cb = noop

        record.op = Record.PUT
        if (!record.id) record.id = uuid()

        record.schema = self.schemas.resolveName(record.schema)

        let validate = false
        if (self.opts.validate) validate = true
        if (typeof opts.validate !== 'undefined') validate = !!opts.validate

        if (validate) {
          if (!self.schemas.validate(record)) return cb(self.schemas.error)
        }

        db.lock(release => {
          db.writer((err, feed) => {
            if (err) return release(cb, err)
            getLinks(record, (err, links) => {
              if (err && err.status !== 404) return release(cb, err)
              record.links = links || []
              const buf = Record.encode(record)
              feed.append(buf, (err) => {
                if (err) return release(cb, err)
                release(cb, null, record.id)
              })
            })
          })
        })
      },

      del: function (id, opts, cb) {
        if (typeof id === 'object') id = id.id
        const record = {
          id,
          op: Record.DEL
        }
        db.lock(release => {
          db.writer('db', (err, feed) => {
            if (err) return release(cb, err)
            getLinks(record, (err, links) => {
              if (err && err.status !== 404) return release(cb, err)
              record.links = links || []
              const buf = Record.encode(record)
              feed.append(buf, (err) => {
                if (err) return release(cb, err)
                release(cb, null, record.id)
              })
            })
          })
        })
      },

      putSchema: function putSchema (name, schema, cb) {
        db.ready(() => {
          const value = self.schemas.parseSchema(name, schema)
          if (!value) return cb(self.schemas.error)
          const record = {
            schema: 'core/schema',
            value
          }
          self.schemas.put(value)
          db.put(record, cb)
        })
      },

      getSchema: function getSchema (name) {
        return self.schemas.get(name)
      },

      getSchemas: function () {
        return self.schemas.list()
      },
      schemas
    }
  }

  return databaseMiddleware

  function getLinks (record, cb) {
    // TODO: Find out if this potentially can block forever.
    db.kappa.ready('kv', () => {
      db.view.kv.getLinks(record, cb)
    })
  }
}
