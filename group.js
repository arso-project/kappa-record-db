const { EventEmitter } = require('events')
const ram = require('random-access-memory')
const sub = require('subleveldown')
const memdb = require('level-mem')
const collect = require('stream-collector')
const { Transform } = require('stream')
const hypercore = require('hypercore')
const debug = require('debug')('db')
const inspect = require('inspect-custom-symbol')
const pretty = require('pretty-hash')
const pump = require('pump')
const mutex = require('mutexify')
const LRU = require('lru-cache')
const Bitfield = require('fast-bitfield')
const thunky = require('thunky')
const crypto = require('hypercore-crypto')
const Nanoresource = require('nanoresource/emitter')

const Kappa = require('kappa-core')
const Indexer = require('kappa-sparse-indexer')
const Corestore = require('corestore')

const { uuid, through, noop } = require('./lib/util')
const { Header } = require('./lib/messages')
const mux = require('./lib/mux')
const SyncMap = require('./lib/sync-map')

const LEN = Symbol('record-size')
const INFO = Symbol('feed-info')

const MAX_CACHE_SIZE = 16777216 // 16M
const DEFAULT_MAX_BATCH = 500
const DEFAULT_FEED_TYPE = 'kappa-records'
const DEFAULT_NAMESPACE = 'kappa-group'

const LOCAL_WRITER_NAME = '_localwriter'
const ROOT_FEED_NAME = '_root'

const Mode = {
  MULTIFEED: 'multifeed',
  ROOTFEED: 'rootfeed'
}

module.exports = class Group extends Nanoresource {
  static uuid () {
    return uuid()
  }

  constructor (opts = {}) {
    super()
    const self = this
    this.opts = opts
    this.handlers = opts.handlers

    if (opts.swarmMode && Object.values(Mode).indexOf(opts.swarmMode) === -1) {
      throw new Error('Invalid swarm mode')
    }

    this._name = opts.name
    this._alias = opts.alias
    this._id = opts.id || uuid()

    this._level = opts.db || memdb()
    this._store = new SyncMap(sub(this._level, 's'), {
      valueEncoding: 'json'
    })

    if (opts.key) {
      this.address = Buffer.isBuffer(opts.key) ? opts.key : Buffer.from(opts.key, 'hex')
    }

    this.kappa = opts.kappa || new Kappa()
    this.corestore = opts.corestore || new Corestore(opts.storage || ram)
    // Patch in a recursive namespace method if we got a namespaced corestore.
    if (!this.corestore.namespace && this.corestore.store) {
      this.corestore.namespace = name => this.corestore.store.namespace(this.corestore.name + ':' + name)
    }
    this.indexer = opts.indexer || new Indexer({
      name: this._name,
      db: sub(this._level, 'indexer'),
      // Load and decode value.
      loadValue (req, next) {
        self.load(req, (err, message) => {
          if (err) return next(null)
          next(message)
        })
      }
    })
    this.lock = mutex()

    this.defaultFeedType = opts.defaultFeedType || DEFAULT_FEED_TYPE

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

    this._api = {}
    this._feedNames = {}
    this._feeds = []
    this._streams = []

    this.ready = this.open.bind(this)
  }

  get view () {
    return this.kappa.view
  }

  get api () {
    return { ...this.kappa.view, ...this._api }
  }

  use (name, createView, opts = {}) {
    const self = this
    const viewdb = sub(this._level, 'view.' + name)
    const view = createView(viewdb, opts.context || self, opts)
    const sourceOpts = {
      maxBatch: opts.maxBatch || view.maxBatch || DEFAULT_MAX_BATCH,
      filter (messages, next) {
        next(messages.filter(msg => msg.seq !== 0))
      }
    }
    this.kappa.use(name, this.indexer.source(sourceOpts), view, opts)
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
      this._store.open(() => {
        this._initFeeds(() => {
          if (this.handlers.open) this.handlers.open(finish)
          else finish()
        })
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
    for (const [key, info] of this._store.entries()) {
      this._addFeedInternally(key, info)
    }

    if (this._swarmMode === Mode.ROOTFEED) {
      initRootFeed(this.address, (err, feed) => {
        if (err) finish(err)
        else finish(null, feed.key, feed.discoveryKey)
      })
    } else {
      finish(null, this.address || crypto.keyPair().publicKey)
    }

    function initRootFeed (key, cb) {
      self.addFeed({ name: ROOT_FEED_NAME, key }, (err, rootfeed) => {
        if (err) return cb(err)
        if (rootfeed.writable) {
          self.addFeed({ name: LOCAL_WRITER_NAME, key: rootfeed.key }, cb)
        } else {
          self.addFeed({ name: LOCAL_WRITER_NAME }, err => cb(err, rootfeed))
        }
      })
    }

    function finish (err, key, discoveryKey) {
      if (err) return cb(err)
      self.address = key
      self.key = key
      self.discoveryKey = discoveryKey || crypto.discoveryKey(key)
      cb()
    }
  }

  _createFeed (key, opts) {
    const { name, persist } = opts
    let feed
    if (persist === false) {
      if (!key) {
        const keyPair = crypto.keyPair()
        key = keyPair.key
        opts.secretKey = keyPair.secretKey
      }
      feed = hypercore(ram, key, opts)
    } else if (!key) {
      // No key was given, create new feed.
      feed = this.corestore.namespace(DEFAULT_NAMESPACE + ':' + name).default(opts)
      key = feed.key
      this.corestore.get({ ...opts, key })
    } else {
      // Key was given, get from corestore.
      feed = this.corestore.get({ ...opts, key })
    }
    return feed
  }

  _addFeedInternally (key, opts) {
    const feed = this._createFeed(key, opts)
    feed.on('remote-update', () => this.emit('remote-update'))

    if (!key && feed.key) key = feed.key.toString('hex')
    if (!key) throw new Error('Missing key for feed')
    const { name, type } = opts
    let id = this._feeds.length
    feed[INFO] = { name, type, id, key, ...opts.info || {} }
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

  feedInfo (keyOrName) {
    const feed = this.feed(keyOrName)
    if (feed && feed[INFO]) return feed[INFO]
    return null
  }

  feed (keyOrName) {
    if (Buffer.isBuffer(keyOrName)) keyOrName = keyOrName.toString('hex')
    if (this._feedNames[keyOrName] !== undefined) {
      return this._feeds[this._feedNames[keyOrName]]
    }
    return null
  }

  getRootFeed () {
    return this.feed(ROOT_FEED_NAME)
  }

  getDefaultWriter () {
    return this.feed(LOCAL_WRITER_NAME)
  }

  addFeed (opts, cb = noop) {
    const self = this
    let { name, key } = opts
    if (!name && !key) return cb(new Error('Either key or name is required'))
    if (key && Buffer.isBuffer(key)) key = key.toString('hex')
    if (this.feed(name)) {
      let info = this.feedInfo(name)
      if (key && info.key !== key) return cb(new Error('Invalid key for name'))
      return cb(null, this.feed(name))
    }
    if (this.feed(key)) {
      let info = this.feedInfo(key)
      if (info && info.name !== name) {
        this._feedNames[name] = info.id
      }
      return cb(null, this.feed(key))
    }

    if (!opts.type) opts.type = this.defaultFeedType
    if (!opts.name) opts.name = uuid()
    const feed = this._addFeedInternally(key, opts)

    feed.ready(() => {
      if (feed.writable && !feed.length) {
        this._initFeed(feed, finish)
      } else {
        finish()
      }
    })

    function finish (err) {
      if (err) return cb(err)
      const info = {
        key: feed.key.toString('hex'),
        name: opts.name,
        type: opts.type,
        ...opts.info || {}
      }
      self._store.setFlush(info.key, info, err => {
        cb(err, feed)
      })
    }
  }

  stats (cb) {
    return this.status(cb)
  }

  status (cb = noop) {
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
    return stats
    // let pending = Object.values(this.kappa.flows).length
    // for (const flow of Object.values(this.kappa.flows)) {
    //   flow._source.subscription.getState((_err, state) => {
    //     stats.kappa[flow.name] = state
    //     if (--pending === 0) cb(null, stats)
    //   })
    // }
  }

  writer (opts, cb) {
    if (typeof opts === 'string') {
      opts = { name: opts }
    } else if (typeof opts === 'function') {
      cb = opts
      opts = null
    }
    if (!opts) {
      opts = { name: LOCAL_WRITER_NAME }
    }
    this.addFeed(opts, (err, feed) => {
      if (err) return cb(err)
      cb(null, feed)
    })
  }

  append (message, opts, cb) {
    const self = this
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    if (!opts) opts = {}
    if (!cb) cb = noop

    this.lock(release => {
      self.writer(opts.feed, (err, feed) => {
        if (err) return release(cb, err)
        opts.feedType = feed[INFO].type
        if (this.handlers.onappend) this.handlers.onappend(message, opts, append)
        else append(null, message, {})

        function append (err, buf, result) {
          if (err) return release(cb, err)
          feed.append(buf, err => {
            if (err) return release(cb, err)
            release(cb, err, result)
          })
        }
      })
    })
  }

  batch (messages, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    const self = this
    this.lock(release => {
      let batch = []
      let errs = []
      let results = []
      let pending = messages.length
      self.writer(opts.feed, (err, feed) => {
        if (err) return release(cb, err)
        opts.feedType = feed[INFO].type
        for (let message of messages) {
          process.nextTick(() => {
            if (this.handlers.onappend) this.handlers.onappend(message, opts, done)
            else done(null, message, {})
          })
        }
        function done (err, buf, result) {
          if (err) errs.push(err)
          else {
            batch.push(buf)
            results.push(result)
          }
          if (--pending !== 0) return

          if (errs.length) {
            let err = new Error(`Batch failed with ${errs.length} errors. First error: ${errs[0].message}`)
            err.errors = errs
            release(cb, err)
            return
          }

          feed.append(batch, err => release(cb, err, results))
        }
      })
    })
  }

  get (keyOrName, seq, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    if (opts.wait === undefined) opts.wait = false
    const feed = this.feed(keyOrName)
    if (!feed) return cb(new Error('Feed does not exist'))
    feed.get(seq, opts, (err, value) => {
      if (err) return cb(err)
      const { key, type: feedType } = feed[INFO]
      const message = { key, seq, value, feedType }
      if (this.handlers.onload) this.handlers.onload(message, cb)
      else cb(null, message)
    })
  }

  load (req, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }
    const self = this
    this._resolveRequest(req, (err, req) => {
      if (err) return cb(err)
      // TODO: Keep this?
      if (req.seq === 0) return cb(new Error('seq 0 is the header, not a record'))

      if (this._recordCache.has(req.lseq)) {
        return cb(null, this._recordCache.get(req.lseq))
      }

      this.get(req.key, req.seq, opts, finish)

      function finish (err, message) {
        if (err) return cb(err)
        message.lseq = req.lseq
        self._recordCache.set(req.lseq, message)
        if (req.meta) {
          message = { ...message, meta: req.meta }
        }
        cb(null, message)
      }
    })
  }

  _resolveRequest (req, cb) {
    if (!empty(req.lseq) && empty(req.seq)) {
      this.indexer.lseqToKeyseq(req.lseq, (err, keyseq) => {
        if (!err && keyseq) {
          req.key = keyseq.key
          req.seq = keyseq.seq
        }
        finish(req)
      })
    } else if (empty(req.lseq)) {
      this.indexer.keyseqToLseq(req.key, req.seq, (err, lseq) => {
        if (!err && lseq) req.lseq = lseq
        finish(req)
      })
    } else finish(req)

    function finish (req) {
      if (empty(req.key) || empty(req.seq)) return cb(new Error('Invalid get request'))
      req.seq = parseInt(req.seq)
      if (!empty(req.lseq)) req.lseq = parseInt(req.lseq)
      if (Buffer.isBuffer(req.key)) req.key = req.key.toString('hex')
      cb(null, req)
    }
  }

  loadRecord (req, cb) {
    this.load(req, cb)
  }

  createLoadStream (opts = {}) {
    const self = this

    const { cacheid } = opts

    let bitfield
    if (cacheid) {
      if (!this._queryBitfields.has(cacheid)) {
        this._queryBitfields.set(cacheid, Bitfield())
      }
      bitfield = this._queryBitfields.get(cacheid)
    }

    const transform = through(function (req, _enc, next) {
      self._resolveRequest(req, (err, req) => {
        if (err) return next()
        if (bitfield && bitfield.get(req.lseq)) {
          this.push({ lseq: req.lseq, meta: req.meta })
          return next()
        }
        self.load(req, (err, message) => {
          if (err) return next()
          if (bitfield) bitfield.set(req.lseq, 1)
          this.push(message)
          next()
        })
      })
    })
    return transform
  }

  createQueryStream (name, args, opts = {}) {
    const self = this
    if (typeof opts.load === 'undefined') opts.load = true

    const proxy = new Transform({
      objectMode: true,
      transform (chunk, enc, next) {
        this.push(chunk)
        next()
      }
    })
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
      qs.on('error', err => proxy.emit('error', err))
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
          indent + ')'
  }
}

function empty (value) {
  return value === undefined || value === null
}
