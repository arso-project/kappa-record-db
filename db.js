const { EventEmitter } = require('events')
const ram = require('random-access-memory')
const sub = require('subleveldown')
const memdb = require('level-mem')
const collect = require('stream-collector')
const { PassThrough, Writable } = require('stream')
const debug = require('debug')('db')
const inspect = require('inspect-custom-symbol')
const pretty = require('pretty-hash')

const Kappa = require('kappa-core')
const Indexer = require('kappa-sparse-indexer')
const Corestore = require('corestore')

const { uuid, through } = require('./lib/util')
const Schema = require('./lib/schema')
const Record = require('./lib/record')

const createKvView = require('./views/kv')
const createRecordsView = require('./views/records')
const createIndexview = require('./views/indexes')

module.exports = function (opts) {
  return new Database(opts)
}
module.exports.uuid = uuid

class Database extends EventEmitter {
  constructor (opts = {}) {
    super()
    const self = this
    this.opts = opts

    this._name = opts.name
    this._validate = defaultTrue(opts.validate)

    this.encoding = Record
    this.lvl = opts.db || memdb()

    this.corestore = opts.corestore || new Corestore(opts.storage || ram)
    this._feeds = {}

    this.indexer = new Indexer({
      db: sub(this.lvl, 'indexer'),
      loadValue (key, seq, cb) {
        self.loadRecord(key, seq, cb)
      }
    })

    this.kappa = new Kappa()
    this.kappa.pause()

    this.use('kv', createKvView)
    this.use('records', createRecordsView)
    this.use('indexes', createIndexview)

    this.opened = false
  }

  get key () {
    return this._primaryFeed && this._primaryFeed.key
  }

  get discoveryKey () {
    return this._primaryFeed && this._primaryFeed.discoveryKey
  }

  get localKey () {
    return this._localWriter && this._localWriter.key
  }

  get view () {
    return this.kappa.view
  }

  get api () {
    return this.kappa.view
  }

  localWriter () {
    return this._localWriter
  }

  close (cb) {
    this.kappa.close(cb)
  }

  use (...args) {
    return this.useRecordView(...args)
  }

  useRecordView (name, createView, opts) {
    const self = this
    const viewdb = sub(this.lvl, 'view.' + name)
    const view = createView(viewdb, self, opts)
    this.kappa.use(name, this.indexer.source(), view)
  }

  replicate (isInitiator, opts) {
    return this.corestore.replicate(isInitiator, null, opts)
  }

  ready (cb) {
    const self = this
    this.corestore.ready(() => {
      this._initPrimaryFeeds(() => {
        this.schemas = this.opts.schemas || new Schema({ key: this.key })
        this._initSchemas(() => {
          this._initSources(() => {
            if (!this._localWriter.length) onfirstinit(finish)
            else finish()
          })
        })
      })
    })

    function finish () {
      self.kappa.resume()
      self.opened = true
      cb()
    }

    function onfirstinit (cb) {
      const sourceOpts = {}
      if (self._name) sourceOpts.name = self._name
      self.putSource(self._localWriter.key, sourceOpts, cb)
    }
  }

  _initPrimaryFeeds (cb) {
    const self = this

    if (this.opts.key) {
      this._primaryFeed = this.feed(this.opts.key)
    } else {
      this._primaryFeed = this.feed(null, { default: true })
    }

    this._primaryFeed.ready(() => {
      if (this._primaryFeed.writable) {
        this._localWriter = this._primaryFeed
      } else {
        this._localWriter = this.feed(null, { default: true })
      }
      this._localWriter.ready(() => {
        self.indexer.add(self._primaryFeed)
        self.indexer.add(self._localWriter)
        cb()
      })
    })
  }

  _initSchemas (cb) {
    const qs = this.createQueryStream('records', { schema: 'core/schema', live: true })
    qs.once('sync', cb)
    qs.pipe(sink((record, next) => {
      this.schemas.put(record.value)
      next()
    }))
  }

  _initSources (cb) {
    const qs = this.createQueryStream('records', { schema: 'core/source', live: true })
    qs.once('sync', cb)
    qs.pipe(sink((record, next) => {
      const feed = this.feed(record.value.key)
      this.indexer.add(feed)
      next()
    }))
  }

  put (record, opts = {}, cb) {
    if (typeof opts === 'function') return this.put(record, {}, opts)
    record.op = Record.PUT
    record.schema = this.schemas.resolveName(record.schema)

    let validate = false
    if (this._validate) validate = true
    if (typeof opts.validate !== 'undefined') validate = !!opts.validate

    debug(`put: id %s schema %s value %s`, record.id || '<>', record.schema, JSON.stringify(record.value).substring(0, 40))

    if (validate) {
      if (!this.schemas.validate(record)) return cb(this.schemas.error)
    }

    if (!record.id) record.id = uuid()
    this._putRecord(record, err => err ? cb(err) : cb(null, record.id))
  }

  del (id, cb) {
    if (typeof id === 'object') id = id.id
    const record = {
      id,
      op: Record.DEL
    }
    this._putRecord(record, cb)
  }

  // TODO: Make this actual batching ops to the underyling feed.
  batch (ops, cb) {
    let pending = 1
    let ids = []
    let errs = []
    for (let op of ops) {
      if (op.op === 'put') ++pending && this.put(op, done)
      if (op.op === 'del') ++pending && this.put(op, done)
    }
    done()
    function done (err, id) {
      if (err) errs.push(err)
      if (id) ids.push(id)
      if (--pending === 0) {
        if (errs.length) {
          err = new Error('Batch failed')
          err.errors = errs
          cb(err)
        } else {
          cb(null, ids)
        }
      }
    }
  }

  // TODO.
  createBatchStream () {
  }

  _putRecord (record, cb) {
    const feed = this.localWriter()
    record.timestamp = Date.now()
    this.getLinks(record, (err, links) => {
      if (err && err.status !== 404) return cb(err)
      record.links = links
      const buf = Record.encode(record)
      feed.append(buf, cb)
    })
  }

  get (req, cb) {
    this.loadStream(this.kappa.view.records.get(req), cb)
  }

  getLinks (record, cb) {
    this.kappa.view.kv.getLinks(record, cb)
  }

  loadRecord (key, seq, cb) {
    const feed = this.feed(key)
    feed.get(seq, (err, record) => {
      record = Record.decode(record, { key, seq })
      if (err) return cb(err)
      cb(null, record)
    })
  }

  feed (key, opts = {}) {
    if (this._feeds[key]) return this._feeds[key]
    opts.valueEncoding = Record
    opts.parent = this.key
    this._feeds[key] = this.corestore.get({ key, parent: this.key, ...opts })
    return this._feeds[key]
  }

  // TODO: Deduplicate / error if exists?
  putSchema (name, schema, cb) {
    this.ready(() => {
      const value = this.schemas.parseSchema(name, schema)
      if (!value) return cb(this.schemas.error)
      const record = {
        schema: 'core/schema',
        value
      }
      this.schemas.put(value)
      this.put(record, cb)
    })
  }

  getSchema (name) {
    return this.schemas.get(name)
  }

  getSchemas () {
    return this.schemas.list()
  }

  putSource (key, opts = {}, cb) {
    if (typeof opts === 'function') return this.putSource(key, {}, opts)
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const record = {
      schema: 'core/source',
      id: key,
      value: {
        type: 'kappa-records',
        key,
        name: opts.name,
        description: opts.description
      }
    }
    this.put(record, cb)
  }

  createLoadStream () {
    const self = this
    const transform = through(function (req, enc, next) {
      self.loadRecord(req.key, req.seq, (err, record) => {
        if (err) this.emit('error', err)
        if (record) {
          if (req.meta) record.meta = req.meta
          this.push(record)
        }
        next()
      })
    })
    return transform
  }

  loadStream (stream, cb) {
    if (typeof stream === 'function') return this.loadStream(null, stream)
    const transform = this.createLoadStream()
    if (stream) stream.pipe(transform)
    if (cb) return collect(transform, cb)
    else return transform
  }

  loadLink (link, cb) {
    if (typeof link === 'string') {
      var [key, seq] = link.split('@')
    } else {
      key = link.key
      seq = link.seq
    }
    this.loadRecord(key, seq, cb)
  }

  createQueryStream (name, args, opts = {}) {
    if (typeof opts.load === 'undefined') opts.load = true

    let proxy = new PassThrough({ objectMode: true })
    if (!this.view[name] || !this.view[name].query) {
      proxy.destroy(new Error('Invalid query name: ' + name))
      return proxy
    }

    const qs = this.view[name].query(args, opts)
    qs.once('sync', () => proxy.emit('sync'))
    if (opts.load !== false) qs.pipe(this.createLoadStream()).pipe(proxy)
    else qs.pipe(proxy)
    return proxy
  }

  query (name, args, opts = {}, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    const qs = this.createQueryStream(name, args, opts)
    if (cb && opts.live) {
      return cb(new Error('Cannot use live mode with callbacks'))
    }
    if (cb) {
      return collect(qs, cb)
    } else {
      return qs
    }
  }

  [inspect] (depth, opts) {
    const { stylize } = opts
    var indent = ''
    if (typeof opts.indentationLvl === 'number') {
      while (indent.length < opts.indentationLvl) indent += ' '
    }

    return 'Database(\n' +
          indent + '  key         : ' + stylize((this.key && pretty(this.key)), 'string') + '\n' +
          indent + '  discoveryKey: ' + stylize((this.discoveryKey && pretty(this.discoveryKey)), 'string') + '\n' +
          indent + '  primaryFeed : ' + fmtFeed(this._primaryFeed) + '\n' +
          indent + '  localFeed   : ' + fmtFeed(this._localWriter) + '\n' +
          indent + '  feeds:      : ' + Object.values(this.indexer._feeds).length + '\n' +
          indent + '  opened      : ' + stylize(this.opened, 'boolean') + '\n' +
          indent + '  name        : ' + stylize(this._name, 'string') + '\n' +
          // indent + '  peers: ' + stylize(this.peers.length, 'number') + '\n' +
          indent + ')'

    function fmtFeed (feed) {
      if (!feed) return '(undefined)'
      return stylize(pretty(feed.key), 'string') + ' @ ' + feed.length + ' ' +
        (feed.writable ? '*' : '')
    }
  }
}

function defaultTrue (val) {
  if (typeof val === 'undefined') return true
  return !!val
}

function formatKey (key) {
  if (Buffer.isBuffer(key)) key = key.toString('hex')
  return key.substring(0, 6) + '..'
}

function sink (fn) {
  return new Writable({
    objectMode: true,
    write (msg, enc, next) {
      fn(msg, next)
    }
  })
}
