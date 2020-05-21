const debug = require('debug')('db')
const pretty = require('pretty-hash')

const Group = require('./group')
const { uuid, sink, noop, defaultTrue } = require('./lib/util')
const createKvView = require('./views/kv')
const createRecordsView = require('./views/records')
const createIndexView = require('./views/indexes')
const Record = require('./lib/record')
const Schema = require('./lib/schema')

const FEED_TYPE = 'kappa-records'
const SCHEMA_SOURCE = 'core/source'

const bind = (obj, fn) => fn.bind(obj)

module.exports = defaultDatabase
module.exports.Group = Group

function defaultDatabase (opts = {}) {
  opts.swarmMode = opts.swarmMode || 'rootfeed'
  // opts.swarmMode = 'multifeed'

  const group = new Group(opts)
  const db = new Database(group, opts)

  group.put = bind(db, db.put)
  group.del = bind(db, db.del)
  group.putSchema = bind(db, db.putSchema)
  group.getSchema = bind(db, db.getSchema)
  group.getSchemas = bind(db, db.getSchemas)
  group.putSource = bind(db, db.putSource)
  group.db = db

  if (opts.swarmMode === 'rootfeed') {
    const sources = new Sources(group, {
      onput: bind(db, db.put),
      onsource: bind(group, group.addFeed)
    })
    group.putSource = bind(sources, sources.put)
    sources.open(noop)
  } else {
    group.putSource = function (key, info = {}, cb) {
      if (typeof info === 'function') {
        cb = info
        info = {}
      }
      group.addFeed({ key, ...info }, cb)
    }
  }

  return group
}

class Database {
  constructor (group, opts) {
    this.group = group
    this.opts = opts
    this.schemas = new Schema()
    this.group.registerFeedType(FEED_TYPE, {
      onload: this._onload.bind(this),
      onappend: this._onappend.bind(this)
    })
    const viewOpts = { schemas: this.schemas }
    this.group.use('kv', createKvView)
    this.group.use('records', createRecordsView, viewOpts)
    this.group.use('index', createIndexView, viewOpts)
  }

  _onopen (cb) {
    this.schemas.open(this.group, cb)
  }

  _onload (message, opts, cb) {
    const { key, seq, lseq, value, feedType } = message
    const record = Record.decode(value, { key, seq, lseq, feedType })
    cb(null, record)
  }

  _onappend (record, opts, cb) {
    if (!record.schema) return cb(new Error('schema is required'))
    if (record.op === undefined) record.op = Record.PUT
    if (record.op === 'put') record.op = Record.PUT
    if (record.op === 'del') record.op = Record.DEL
    if (!record.id) record.id = uuid()

    record.schema = this.schemas.resolveName(record.schema)

    if (record.op === Record.PUT) {
      let validate = false
      if (this.opts.validate) validate = true
      if (typeof opts.validate !== 'undefined') validate = !!opts.validate

      if (validate) {
        if (!this.schemas.validate(record)) return cb(this.schemas.error)
      }
    }

    record.timestamp = Date.now()

    this.group.view.kv.getLinks(record, (err, links) => {
      if (err && err.status !== 404) return cb(err)
      record.links = links || []
      const buf = Record.encode(record)
      cb(null, buf, record.id)
    })
  }

  put (record, opts, cb) {
    record.op = Record.PUT
    this.group.append(record, opts, cb)
  }

  del ({ id, schema }, opts, cb) {
    const record = {
      id,
      schema,
      op: Record.DEL
    }
    this.group.append(record, opts, cb)
  }

  putSchema (name, schema, cb) {
    this.group.ready(() => {
      const value = this.schemas.parseSchema(name, schema)
      if (!value) return cb(this.schemas.error)
      const record = {
        schema: 'core/schema',
        value
      }
      this.schemas.put(value)
      this.group.put(record, cb)
    })
  }

  getSchema (name) {
    return this.schemas.get(name)
  }

  getSchemas () {
    return this.schemas.list()
  }

  putSource (key, info = {}, cb) {
    // opts should/can include: { alias }
    if (typeof info === 'function') {
      cb = info
      info = {}
    }
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    this.put({
      schema: SCHEMA_SOURCE,
      id: key,
      value: {
        type: FEED_TYPE,
        key,
        ...info
      }
    }, cb)
  }
}

class Sources {
  constructor (group, handlers) {
    this.group = group
    this.handlers = handlers
  }

  open (cb) {
    const qs = this.group.createQueryStream('records', { schema: 'core/source' }, { live: true })
    qs.once('sync', cb)
    qs.pipe(sink((record, next) => {
      const { alias, key, type, ...info } = record.value
      if (type !== FEED_TYPE) return next()
      debug('[%s] source:add key %s alias %s type %s', this.group._name, pretty(key), alias, type)
      const opts = { alias, key, type, info }
      this.handlers.onsource(opts)
      next()
    }))
  }

  put (key, opts = {}, cb) {
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
        type: FEED_TYPE,
        key,
        ...opts
      }
    }
    this.handlers.onput(record, cb)
  }
}

module.exports.Database = Database
