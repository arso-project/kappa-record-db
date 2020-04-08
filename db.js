const { EventEmitter } = require('events')
const ram = require('random-access-memory')
const sub = require('subleveldown')
const memdb = require('level-mem')
const collect = require('stream-collector')
const { PassThrough } = require('stream')
const debug = require('debug')('db')
const inspect = require('inspect-custom-symbol')
const pretty = require('pretty-hash')
const pump = require('pump')
const mutex = require('mutexify')
const LRU = require('lru-cache')
const Bitfield = require('fast-bitfield')
const thunky = require('thunky')

const Kappa = require('kappa-core')
const Indexer = require('kappa-sparse-indexer')
const Corestore = require('corestore')

const { uuid, through, noop, defaultTrue } = require('./lib/util')
const { Header } = require('./lib/messages')

const LEN = Symbol('record-size')

const MAX_CACHE_SIZE = 16777216 // 16M
const DEFAULT_MAX_BATCH = 500
const FEED_TYPE = 'kappa-records'

module.exports = class Database extends EventEmitter {
  static uuid () {
    return uuid()
  }

  constructor (opts = {}) {
    super()
    const self = this
    this.opts = opts

    this._name = opts.name
    this._alias = opts.alias

    this._validate = defaultTrue(opts.validate)

    this.lvl = opts.db || memdb()

    this._feeddb = sub(this.lvl, 'feeds')

    this.corestore = opts.corestore || new Corestore(opts.storage || ram)
    this._feeds = {}

    this.indexer = new Indexer({
      name: this._name,
      db: sub(this.lvl, 'indexer'),
      // Load and decode value.
      loadValue (message, next) {
        // Skip first message (header)
        if (message.seq === 0) return next(null)
        self.loadRecord(message, (err, record) => {
          if (err) return next(message)
          next(record)
        })
      }
    })

    this.kappa = new Kappa()

    this.lock = mutex()

    this.defaultFeedType = opts.defaultFeedType || FEED_TYPE

    // Cache for records. Max cache size can be set as an option.
    // The length for each record is the buffer length of the serialized record,
    // so the actual cache size will be a bit higher.
    this._recordCache = new LRU({
      max: opts.maxCacheSize || MAX_CACHE_SIZE,
      length (record) {
        return record[LEN] || 64
      }
    })
    // Cache for query bitfields. This will usually take 4KB per bitfield.
    // We cache max 4096 bitfields, amounting to max 16MB bitfield cache size.
    this._queryBitfields = new LRU({
      max: 4096
    })

    this.opened = false
    this.middlewares = []
    this._api = {}

    this._feedNames = {}
    this._feeds = [null]

    this.open = thunky(this._open.bind(this))
    this.close = thunky(this._close.bind(this))
    this.ready = this.open
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
    return { ...this.kappa.view, ...this._api }
  }

  useMiddleware (name, handlers) {
    this.middlewares.push({ name, handlers })
    this._api[name] = handlers.api
    if (handlers.views) {
      for (const [name, createView] of Object.entries(handlers.views)) {
        this.use(name, createView)
      }
    }
  }

  applyMiddleware (op, args, cb) {
    if (typeof args === 'function') {
      cb = args
      args = []
    }
    let [state, ...other] = args
    let hasState = true
    if (state === undefined) hasState = false

    let middlewares = this.middlewares.filter(m => m.handlers[op])
    if (!middlewares.length) return cb(null, state)

    next(state)

    function next (state) {
      let middleware = middlewares.shift()
      if (!middleware) return cb(null, state)
      if (hasState) middleware.handlers[op](state, ...other, done)
      else middleware.handlers[op](done)
      function done (err, state) {
        if (err) return cb(err)
        process.nextTick(next, state)
      }
    }
  }

  use (name, createView, opts = {}) {
    const self = this
    const viewdb = sub(this.lvl, 'view.' + name)
    const view = createView(viewdb, self, opts)
    const sourceOpts = {
      maxBatch: opts.maxBatch || view.maxBatch || DEFAULT_MAX_BATCH,
      filter (messages, next) {
        next(messages.filter(msg => msg.seq !== 0))
      }
    }
    this.kappa.use(name, this.indexer.source(sourceOpts), view)
  }

  replicate (isInitiator, opts) {
    return this.corestore.replicate(isInitiator, opts)
  }

  _close (cb) {
    this.kappa.close(() => {
      this.corestore.close(cb)
    })
  }

  _open (cb) {
    const self = this
    this.corestore.ready(() => {
      this._initFeeds(() => {
        this._primaryFeed = this.getFeed('primary')
        this._localWriter = this.getFeed('localwriter')
        this.applyMiddleware('open', finish)
      })
    })

    function finish () {
      self.kappa.resume()
      self.opened = true
      cb()
    }
  }

  _initFeeds (cb) {
    this._openFeeds(() => {
      this.addFeed({ name: 'primary', key: this.opts.key }, (err, feed) => {
        if (err) return cb(err)
        if (feed.writable) {
          this.addFeed({ name: 'localwriter', key: feed.key }, cb)
        } else {
          this.addFeed({ name: 'localwriter' }, cb)
        }
      })
    })
  }

  _openFeeds (cb) {
    const rs = this._feeddb.createReadStream({ gt: 'key/', lt: 'key/z' })
    rs.on('data', ({ value }) => {
      value = JSON.parse(value)
      const { name, key, type } = value
      this._addFeedInternally(key, name, type)
    })
    rs.on('end', cb)
  }

  _addFeedInternally (key, name, type) {
    const feed = this.corestore.get({ key })
    let id = this._feeds.length
    const info = { feed, key, name, type, id }
    this._feeds.push(info)
    this._feedNames[name] = id
    this._feedNames[key] = id
    this.indexer.add(feed, { scan: true })
    this.emit('feed', feed)
    debug('[%s] add feed key %s name %s type %s', this._name, pretty(feed.key), name, type)

    return info
  }

  // Write header to feed.
  // TODO: Delegate this to a feed type handler.
  _initFeed (info, cb) {
    const { feed, type } = info
    const header = Header.encode({
      type,
      metadata: Buffer.from(JSON.stringify({ encodingVersion: 1 }))
    })
    feed.append(header, cb)
  }

  getFeedInfo (keyOrName) {
    if (Buffer.isBuffer(keyOrName)) keyOrName = keyOrName.toString('hex')
    if (this._feedNames[keyOrName] !== undefined) {
      return this._feeds[this._feedNames[keyOrName]]
    }
    return null
  }

  getFeed (keyOrName) {
    const info = this.getFeedInfo(keyOrName)
    if (info) return info.feed
    return null
  }

  hasFeed (keyOrName) {
    if (Buffer.isBuffer(keyOrName)) keyOrName = keyOrName.toString('hex')
    if (this._feedNames[keyOrName] !== undefined) return true
    return false
  }

  addFeed (opts, cb = noop) {
    let { name, key, type } = opts
    if (!name && !key) return cb(new Error('Either key or name is required'))
    if (key && Buffer.isBuffer(key)) key = key.toString('hex')
    if (this.hasFeed(name)) {
      let info = this.getFeedInfo(name)
      if (key && info.key !== key) return cb(new Error('Invalid key for name'))
      return cb(null, info)
    }
    if (this.hasFeed(key)) {
      let info = this.getFeedInfo(key)
      if (info.name !== name) {
        this._feedNames[name] = info.id
      }
      return cb(null, info.feed)
    }

    if (!type) type = this.defaultFeedType
    if (!name) name = uuid()
    if (!key) key = this.corestore.get().key.toString('hex')

    const data = { name, key, type }
    const ops = [
      { type: 'put', key: 'name/' + name, value: key },
      { type: 'put', key: 'key/' + key, value: JSON.stringify(data) }
    ]
    this._feeddb.batch(ops, err => {
      if (err) return cb(err)
      const info = this._addFeedInternally(key, name, type)
      const feed = info.feed
      feed.ready(() => {
        if (feed.writable && !feed.length) {
          this._initFeed(info, err => cb(err, feed))
        } else {
          cb(null, feed)
        }
      })
    })
  }

  writer (name, opts, cb) {
    if (typeof name === 'function') {
      cb = name
      opts = {}
      name = 'localwriter'
    }
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    opts.name = name
    this.addFeed(opts, (err, info) => {
      if (err) return cb(err)
      cb(null, info.feed)
    })
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
          err = new Error(`Batch failed with ${errs.length} errors. First error: ${errs[0].message}`)
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

  get (req, opts, cb) {
    if (typeof opts === 'function') return this.get(req, {}, opts)
    this.query('records', req, opts, cb)
  }

  loadLseq (req, cb) {
    if (req.lseq && req.seq && req.key) {
      cb(null, req)
      return
    }
    if (req.lseq && (!req.key || !req.seq)) {
      this.indexer.lseqToKeyseq(req.lseq, (err, keyseq) => {
        if (!err && keyseq) {
          req.key = keyseq.key
          req.seq = keyseq.seq
        }
        cb(null, req)
      })
      return
    }
    if (!req.lseq && req.key && typeof req.seq !== 'undefined') {
      this.indexer.keyseqToLseq(req.key, req.seq, (err, lseq) => {
        if (!err && lseq) req.lseq = lseq
        cb(null, req)
      })
      return
    }
    cb(null, req)
  }

  loadRecord (req, cb) {
    const self = this
    this.loadLseq(req, (err, req) => {
      // TODO: Error out?
      if (err) return cb(err)
      let { key, seq, lseq } = req
      if (!key) return cb(new Error('key is required'))
      if (Buffer.isBuffer(key)) key = key.toString('hex')
      seq = parseInt(seq)
      if (seq === 0) return cb(new Error('seq 0 is the header, not a record'))
      if (typeof seq !== 'number') return cb(new Error('seq is not a number'))

      const cachekey = key + '@' + seq
      if (this._recordCache.has(cachekey)) {
        return cb(null, this._recordCache.get(cachekey))
      }

      const feed = this.getFeed(key)
      if (!feed) return cb(new Error('feed not found'))

      let len
      feed.get(seq, { wait: false }, onget)

      function onget (err, buf) {
        if (err) return cb(err)
        if (buf.length) len = buf.length

        const feedType = 'db'

        const message = { key, seq, lseq, value: buf, feedType }

        self.applyMiddleware('onload', [message], finish)
      }

      function finish (err, message) {
        if (err) return cb(err)
        if (len) message[LEN] = len
        self._recordCache.set(cachekey, message)
        cb(null, message)
      }
    })
  }

  createLoadStream (opts = {}) {
    const self = this

    const { cacheid } = opts

    let bitfield = null
    if (cacheid) {
      if (!this._queryBitfields.has(cacheid)) {
        this._queryBitfields.set(cacheid, Bitfield())
      }
      bitfield = this._queryBitfields.get(cacheid)
    }

    const transform = through(function (req, _enc, next) {
      self.loadLseq(req, (err, req) => {
        if (err) this.emit('error', err)
        if (bitfield && bitfield.get(req.lseq)) {
          this.push({ lseq: req.lseq, meta: req.meta })
          next()
          return
        }
        self.loadRecord(req, (err, record) => {
          if (err) this.emit('error', err)
          if (!record) return next()
          const recordClone = Object.assign({}, record)
          if (req.meta) recordClone.meta = req.meta
          if (bitfield) {
            bitfield.set(req.lseq, 1)
          }
          this.push(recordClone)
          next()
        })
      })
    })
    return transform
  }

  createQueryStream (name, args, opts = {}) {
    const self = this
    if (typeof opts.load === 'undefined') opts.load = true

    const proxy = new PassThrough({ objectMode: true })
    const flow = this.kappa.flows[name]

    if (!flow || !flow.view.query) {
      proxy.destroy(new Error('Invalid query name: ' + name))
      return proxy
    }

    if (opts.waitForSync) {
      this.sync(createStream)
    } else {
      createStream()
    }

    return proxy

    function createStream () {
      const qs = flow.view.query(args, opts)
      qs.once('sync', () => proxy.emit('sync'))
      if (opts.load !== false) pump(qs, self.createLoadStream(opts), proxy)
      else pump(qs, proxy)
    }
  }

  sync (cb) {
    process.nextTick(() => {
      this.lock(release => {
        this.kappa.ready(cb)
        release()
      })
    })
  }

  query (name, args, opts = {}, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }

    if (cb && opts.live) {
      return cb(new Error('Cannot use live mode with callbacks'))
    }

    const qs = this.createQueryStream(name, args, opts)
    return collect(qs, cb)
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
