const { EventEmitter } = require('events')
const ram = require('random-access-memory')
const sub = require('subleveldown')
const memdb = require('level-mem')
const collect = require('stream-collector')
const crypto = require('crypto')
const { Transform, PassThrough } = require('stream')
const debug = require('debug')('db')

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
    this.key = opts.key

    this._name = opts.name

    if (!this.key) this.key = crypto.randomBytes(32)

    this._validate = defaultTrue(opts.validate)

    this.encoding = Record
    this.schemas = opts.schemas || new Schema({ key: this.key })
    this.lvl = opts.db || memdb()

    this.corestore = opts.corestore || new Corestore(opts.storage || ram)

    this.indexer = new Indexer({
      db: sub(this.lvl, 'indexer'),
      transform (msgs, next) {
        let res = []
        load()
        function load (err, record) {
          if (!err && record) res.push(record)
          let msg = msgs.pop()
          if (!msg) return next(res)
          process.nextTick(() => self.loadRecord(msg.key, msg.seq, load))
        }
      }
    })

    this.kappa = new Kappa()
    this.kappa.pause()

    this.useRecordView('kv', createKvView)
    this.useRecordView('records', createRecordsView)
    this.useRecordView('indexes', createIndexview)

    this.useRecordView('schema', () => ({
      // TODO: Moving the filterering from map to filter breaks everytihng.
      // The values are not yet decoded there? This should not be the case.
      // filter (msgs, next) {
      //   next(msgs.filter(msg => msg.schema === 'core/schema'))
      // },
      map (msgs, next) {
        for (const msg of msgs) {
          if (msg.schema === 'core/schema') self.schemas.put(msg.value)
          if (!self.schemas.has(msg)) self.schemas.fake(msg)
        }
        next()
      }
    }))

    this.useRecordView('source', () => ({
      // filter (msgs, next) {
      //   next(msgs.filter(msg => msg.schema === 'core/source'))
      // },
      map (msgs, next) {
        msgs = msgs.filter(msg => msg.schema === 'core/source')
        msgs.forEach(msg => {
          const key = msg.value.key
          const feed = self.corestore.get({ key, parent: self.key })
          self.indexer.add(feed, { scan: true })
        })
        next()
      }
    }))

    this._opened = false
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
    this.corestore.ready(() => {
      this._initLocalWriter(err => {
        if (err) return cb(err)
        this._initSchemas(() => {
          this.kappa.resume()
          this._opened = true
          cb()
        })
      })
    })
  }

  get localKey () {
    return this.localWriter().key
  }

  get view () {
    return this.kappa.view
  }

  localWriter () {
    return this._localWriter
  }

  _initLocalWriter (cb) {
    const self = this
    this._localWriter = this.corestore.get({
      default: true,
      _name: 'localwriter'
    })

    this._localWriter.ready(() => {
      this.indexer.add(this._localWriter)
      if (!this._localWriter.length) onfirstinit(cb)
      else cb()
    })

    function onfirstinit (cb) {
      const sourceOpts = {}
      if (self._name) sourceOpts.name = self._name
      self.putSource(self._localWriter.key, sourceOpts, cb)
    }
  }

  _initSchemas (cb) {
    const qs = this.api.records.get({ schema: 'core/schema' })
    this.loadStream(qs, (err, schemas) => {
      if (err) return cb(err)
      schemas.forEach(msg => this.schemas.put(msg.value))
      cb()
    })
  }

  get api () {
    return this.kappa.view
  }

  put (record, opts, cb) {
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

  // get (id, cb) {
  //   this.kappa.api.kv.getLinks(id, (err, links) => {
  //     if (err) cb(err)
  //     else this.loadAll(links, cb)
  //   })
  // }

  getLinks (record, cb) {
    this.kappa.view.kv.getLinks(record, cb)
  }

  loadRecord (key, seq, cb) {
    const feed = this.corestore.get({ key })
    feed.get(seq, (err, record) => {
      record = Record.decode(record, { key, seq })
      if (err) return cb(err)
      cb(null, record)
    })
  }

  // TODO: Deduplicate / error if exists?
  putSchema (name, schema, cb) {
    this.ready(() => {
      schema = this.schemas.parseSchema(name, schema)
      if (!schema) return cb(this.schemas.error)
      const record = {
        schema: 'core/schema',
        value: schema
      }
      this.put(record, cb)
    })
  }

  getSchema (name) {
    return this.schemas.get(name)
  }

  getSchemas () {
    return this.schemas.list()
  }

  putSource (key, opts, cb) {
    if (typeof opts === 'function') return this.putSource(key, {}, opts)
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    const record = {
      schema: 'core/source',
      id: key,
      value: {
        type: 'kappa-records',
        key,
        name: opts.source,
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

  // loadAll (links, cb) {
  //   let pending = links.length
  //   let res = []
  //   let errs = []
  //   links.forEach(link => this.loadLink(link, (err, link) => {
  //     if (err) errs.push(err)
  //     else res.push(link)
  //     if (--pending === 0) cb(errs.length ? errs : null, links)
  //   }))
  // }

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
    if (opts.load) qs.pipe(this.createLoadStream()).pipe(proxy)
    else qs.pipe(proxy)
    return proxy
  }

  query (name, args, opts, cb) {
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
}

function defaultTrue (val) {
  if (typeof val === 'undefined') return true
  return !!val
}
