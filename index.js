const debug = require('debug')('db')
const pretty = require('pretty-hash')

const Stack = require('./stack')
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
module.exports.Stack = Stack

function defaultDatabase (opts = {}) {
  opts.swarmMode = opts.swarmMode || 'rootfeed'
  // opts.swarmMode = 'multifeed'

  const stack = new Stack(opts)
  const db = new Database(stack, opts)

  stack.put = bind(db, db.put)
  stack.del = bind(db, db.del)
  stack.putSchema = bind(db, db.putSchema)
  stack.getSchema = bind(db, db.getSchema)
  stack.getSchemas = bind(db, db.getSchemas)
  stack.putSource = bind(db, db.putSource)
  stack.db = db

  if (opts.swarmMode === 'rootfeed') {
    const sources = new Sources(stack, {
      onput: bind(db, db.put),
      onsource: bind(stack, stack.addFeed)
    })
    stack.putSource = bind(sources, sources.put)
    sources.open(noop)
  } else {
    stack.putSource = function (key, info = {}, cb) {
      if (typeof info === 'function') {
        cb = info
        info = {}
      }
      stack.addFeed({ key, ...info }, cb)
    }
  }

  return stack
}

class Database {
  constructor (stack, opts) {
    this.stack = stack
    this.opts = opts
    this.schemas = new Schema()

    this.stack.handlers = {
      onload: this._onload.bind(this),
      onappend: this._onappend.bind(this),
      open: this._onopen.bind(this)
    }
    this.stack.use('kv', createKvView)
    this.stack.use('records', createRecordsView, { schemas: this.schemas })
    this.stack.use('index', createIndexView, { schemas: this.schemas })
  }

  _onopen (cb) {
    this.schemas.open(this.stack, cb)
  }

  _onload (message, cb) {
    if (message.feedType !== FEED_TYPE) return cb(null, message)
    const { key, seq, lseq, value, feedType } = message
    const record = Record.decode(value, { key, seq, lseq, feedType })
    cb(null, record)
  }

  _onappend (record, opts, cb) {
    if (opts.feedType !== FEED_TYPE) return cb(null, record)
    if (record.op === undefined) record.op = Record.PUT
    if (record.op === 'put') record.op = Record.PUT
    if (record.op === 'del') record.op = Record.DEL
    if (!record.id) record.id = uuid()

    if (record.op === Record.PUT) {
      record.schema = this.schemas.resolveName(record.schema)
      let validate = false
      if (this.opts.validate) validate = true
      if (typeof opts.validate !== 'undefined') validate = !!opts.validate

      if (validate) {
        if (!this.schemas.validate(record)) return cb(this.schemas.error)
      }
    }

    record.timestamp = Date.now()

    this.stack.view.kv.getLinks(record, (err, links) => {
      if (err && err.status !== 404) return cb(err)
      record.links = links || []
      const buf = Record.encode(record)
      cb(null, buf, record.id)
    })
  }

  put (record, opts, cb) {
    record.op = Record.PUT
    this.stack.append(record, opts, cb)
  }

  del (id, opts, cb) {
    if (typeof id === 'object') id = id.id
    const record = {
      id,
      op: Record.DEL
    }
    this.stack.append(record, opts, cb)
  }

  putSchema (name, schema, cb) {
    this.stack.ready(() => {
      const value = this.schemas.parseSchema(name, schema)
      if (!value) return cb(this.schemas.error)
      const record = {
        schema: 'core/schema',
        value
      }
      this.schemas.put(value)
      this.stack.put(record, cb)
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
  constructor (stack, handlers) {
    this.stack = stack
    this.handlers = handlers
  }

  open (cb) {
    const qs = this.stack.createQueryStream('records', { schema: 'core/source' }, { live: true })
    qs.once('sync', cb)
    qs.pipe(sink((record, next) => {
      const { alias, key, type, ...info } = record.value
      if (type !== FEED_TYPE) return next()
      debug(`[%s] source:add key %s alias %s type %s`, this.stack._name, pretty(key), alias, type)
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
