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
const crypto = require('hypercore-crypto')

const Kappa = require('kappa-core')
const Indexer = require('kappa-sparse-indexer')
const Corestore = require('corestore')

const { uuid, through, noop } = require('./lib/util')
const { Header } = require('./lib/messages')
const mux = require('./lib/mux')

const LEN = Symbol('record-size')
const INFO = Symbol('feed-info')

const MAX_CACHE_SIZE = 16777216 // 16M
const DEFAULT_MAX_BATCH = 500
const FEED_TYPE = 'kappa-records'

const LOCAL_WRITER_NAME = '_localwriter'
const ROOT_FEED_NAME = '_root'

const Mode = {
  MULTIFEED: 'multifeed',
  ROOTFEED: 'rootfeed'
}

module.exports = class Database extends EventEmitter {
  static uuid () {
    return uuid()
  }

  constructor (opts = {}) {
    super()
    const self = this
    this.opts = opts

    if (opts.swarmMode && Object.values(Mode).indexOf(opts.swarmMode) === -1) {
      throw new Error('Invalid swarm mode')
    }

    this._name = opts.name
    this._alias = opts.alias
    this._id = opts.id || uuid()

    this._level = opts.db || memdb()
    this._feeddb = sub(this._level, 'feeds')

    if (opts.key) {
      this._address = Buffer.isBuffer(opts.key) ? opts.key : Buffer.from(opts.key, 'hex')
    }

    this.kappa = new Kappa()
    this.corestore = opts.corestore || new Corestore(opts.storage || ram)
    this.indexer = new Indexer({
      name: this._name,
      db: sub(this._level, 'indexer'),
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

    this._swarmMode = opts.swarmMode || Mode.ROOTFEED
    if (this._swarmMode === Mode.MULTIFEED) {
      this.on('feed', (feed, info) => {
        mux.forwardLiveFeedAnnouncements(this, feed, info)
      })
    }

    this._middlewares = []
    this._api = {}
    this._feedNames = {}
    this._feeds = []
    this._streams = []

    this.open = thunky(this._open.bind(this))
    this.close = thunky(this._close.bind(this))
    this.ready = this.open
    this.opened = false
  }

  get view () {
    return this.kappa.view
  }

  get api () {
    return { ...this.kappa.view, ...this._api }
  }

  useMiddleware (name, handlers) {
    this._middlewares.push({ name, handlers })
    if (handlers.api) {
      this._api[name] = {}
      for (let [key, value] of Object.entries(handlers.api)) {
        if (typeof value === 'function') value = value.bind(handlers.api)
        this._api[name][key] = value
      }
    }
    if (handlers.views) {
      for (const [name, createView] of Object.entries(handlers.views)) {
        this.use(name, createView)
      }
    }
  }

  applyMiddleware (op, args, cb) {
    if (typeof args === 'function') {
      cb = args
      args = undefined
    }
    let state, other
    let hasState = true
    if (Array.isArray(args)) {
      state = args[0]
      other = args.slice(1)
    } else if (args !== undefined) {
      state = args
      other = null
    } else {
      hasState = false
    }

    let middlewares = this._middlewares.filter(m => m.handlers[op])
    if (!middlewares.length) return cb(null, state)

    next(state)

    function next (state) {
      let middleware = middlewares.shift()
      if (!middleware) return cb(null, state)
      if (hasState && other) middleware.handlers[op](state, ...other, done)
      else if (hasState) middleware.handlers[op](state, done)
      else middleware.handlers[op](done)
      function done (err, state) {
        if (err) return cb(err)
        process.nextTick(next, state)
      }
    }
  }

  use (name, createView, opts = {}) {
    const self = this
    const viewdb = sub(this._level, 'view.' + name)
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
    if (this._swarmMode === Mode.MULTIFEED) {
      return mux.replicate(this, isInitiator, opts)
    } else {
      return this.corestore.replicate(isInitiator, opts)
    }
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
    const self = this
    this._openFeeds(() => {
      if (this._swarmMode === Mode.ROOTFEED) {
        this._address = this._address || this.corestore.get().key
        initRootFeed(this._address)
      } else {
        this._address = this._address || crypto.keyPair().publicKey
        finish()
      }
    })

    function initRootFeed (key) {
      self.addFeed({ name: ROOT_FEED_NAME, key }, (err, feed) => {
        if (err) return finish(err)
        feed.ready(() => {
          if (feed.writable) {
            self.addFeed({ name: LOCAL_WRITER_NAME, key }, finish)
          } else {
            self.addFeed({ name: LOCAL_WRITER_NAME }, finish)
          }
        })
      })
    }

    function finish (err) {
      self.key = self._address
      self.discoveryKey = crypto.discoveryKey(self.key)
      cb(err)
    }
  }

  _openFeeds (cb) {
    const rs = this._feeddb.createReadStream({ gt: 'key/', lt: 'key/z' })
    rs.on('data', ({ value }) => {
      value = JSON.parse(value)
      const { name, key, type } = value
      console.log('openfeed', value)
      this._addFeedInternally(key, name, type)
    })
    rs.on('end', cb)
  }

  _addFeedInternally (key, name, type) {
    const feed = this.corestore.get({ key })
    let id = this._feeds.length
    feed[INFO] = { name, type, id, key }
    this._feeds.push(feed)
    this._feedNames[name] = id
    this._feedNames[key] = id
    this.indexer.add(feed, { scan: true })
    this.emit('feed', feed, { ...feed[INFO] })
    debug('[%s] add feed key %s name %s type %s', this._name, pretty(feed.key), name, type)

    return feed
  }

  // Write header to feed.
  // TODO: Delegate this to a feed type handler.
  _initFeed (feed, cb) {
    if (!feed[INFO]) return cb(new Error('Invalid feed: has no info'))
    const { type } = feed[INFO]
    const header = Header.encode({
      type,
      metadata: Buffer.from(JSON.stringify({ encodingVersion: 1 }))
    })
    feed.append(header, cb)
  }

  getFeedInfo (keyOrName) {
    const feed = this.getFeed(keyOrName)
    if (feed && feed[INFO]) return feed[INFO]
    return null
  }

  getFeed (keyOrName) {
    if (!keyOrName) keyOrName = LOCAL_WRITER_NAME
    if (Buffer.isBuffer(keyOrName)) keyOrName = keyOrName.toString('hex')
    if (this._feedNames[keyOrName] !== undefined) {
      return this._feeds[this._feedNames[keyOrName]]
    }
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
      return cb(null, this.getFeed(name))
    }
    if (this.hasFeed(key)) {
      let info = this.getFeedInfo(key)
      if (info.name !== name) {
        this._feedNames[name] = info.id
      }
      return cb(null, this.getFeed(key))
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
      const feed = this._addFeedInternally(key, name, type)
      feed.ready(() => {
        if (feed.writable && !feed.length) {
          this._initFeed(feed, err => cb(err, feed))
        } else {
          cb(null, feed)
        }
      })
    })
  }

  stats (cb) {
    const stats = { feeds: [] }
    for (const feed of this._feeds) {
      stats.feeds.push({
        key: feed.key.toString('hex'),
        length: feed.length,
        byteLength: feed.byteLength,
        writable: feed.writable,
        ...feed[INFO],
        stats: feed.stats
      })
    }
    cb(null, stats)
    // let pending = Object.values(this.kappa.flows).length
    // for (const flow of Object.values(this.kappa.flows)) {
    //   flow._source.subscription.getState((_err, state) => {
    //     stats.kappa[flow.name] = state
    //     if (--pending === 0) cb(null, stats)
    //   })
    // }
  }

  writer (name, cb) {
    if (typeof name === 'function') {
      cb = name
      name = LOCAL_WRITER_NAME
    } else if (name === null) {
      name = LOCAL_WRITER_NAME
    }
    let opts
    if (name && typeof name === 'object') {
      opts = name
    } else {
      opts = { name }
    }
    this.addFeed(opts, (err, feed) => {
      if (err) return cb(err)
      cb(null, feed)
    })
  }

  append (writer, message, cb) {
    this.writer(writer, (err, feed) => {
      if (err) return cb(err)
      feed.append(message, cb)
    })
  }

  // TODO.
  createBatchStream () {
  }

  get (req, opts, cb) {
    if (typeof opts === 'function') return this.get(req, {}, opts)
    this.query('records', req, opts, cb)
  }

  _loadLseq (req, cb) {
    if (req.lseq !== undefined && req.seq !== undefined && req.key !== undefined) {
      cb(null, req)
      return
    }
    if (req.lseq !== undefined) {
      this.indexer.lseqToKeyseq(req.lseq, (err, keyseq) => {
        if (!err && keyseq) {
          req.key = keyseq.key
          req.seq = keyseq.seq
        }
        cb(null, req)
      })
      return
    }
    if (req.lseq === undefined && req.seq !== undefined && req.key) {
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
    this._loadLseq(req, (err, req) => {
      if (err) return cb(err)
      let { key, seq, lseq } = req
      if (!key) return cb(new Error('Key is required'))
      if (Buffer.isBuffer(key)) key = key.toString('hex')
      seq = parseInt(seq)
      lseq = parseInt(lseq)
      if (seq === 0) return cb(new Error('seq 0 is the header, not a record'))

      if (this._recordCache.has(lseq)) {
        return cb(null, this._recordCache.get(lseq))
      }

      const feed = this.getFeed(key)
      if (!feed) return cb(new Error('feed not found'))

      let len
      feed.get(seq, { wait: false }, onget)

      function onget (err, buf) {
        if (err) return cb(err)
        if (buf && buf.length) len = buf.length

        const feedType = feed[INFO].type
        const message = { key, seq, lseq, value: buf, feedType }

        self.applyMiddleware('onload', message, finish)
      }

      function finish (err, message) {
        if (err) return cb(err)
        if (len) message[LEN] = len
        self._recordCache.set(lseq, message)
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
      self._loadLseq(req, (err, req) => {
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
          indent + '  address     : ' + stylize((this.key && pretty(this.key)), 'string') + '\n' +
          indent + '  discoveryKey: ' + stylize((this.discoveryKey && pretty(this.discoveryKey)), 'string') + '\n' +
          indent + '  swarmMode:    ' + stylize(this._swarmMode) + '\n' +
          indent + '  feeds:      : ' + stylize(this._feeds.length) + '\n' +
          indent + '  opened      : ' + stylize(this.opened, 'boolean') + '\n' +
          indent + '  name        : ' + stylize(this._name, 'string') + '\n' +
          // indent + '  peers: ' + stylize(this.peers.length, 'number') + '\n' +
          indent + ')'

    // function fmtFeed (feed) {
    //   if (!feed) return '(undefined)'
    //   return stylize(pretty(feed.key), 'string') + ' @ ' + feed.length + ' ' +
    //     (feed.writable ? '*' : '')
    // }
  }
}
