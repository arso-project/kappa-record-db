const codecs = require('codecs')
const Nanoresource = require('nanoresource')
const mutex = require('mutexify')
const { createPromiseProxy } = require('./util')

const CALLBACK_METHODS = [
  'open',
  'close',
  'flush',
  'getFlush',
  'setFlush',
  'deleteFlush',
  'batchFlush'
]

module.exports = class SyncMap extends Nanoresource {
  constructor (db, opts = {}) {
    super()
    this.db = db
    this._data = {}
    this._queue = {}
    this._valueEncoding = codecs(opts.valueEncoding || 'utf8')
    this._condition = opts.condition
    this._lock = mutex()

    this.promise = createPromiseProxy(this, CALLBACK_METHODS)
  }

  _open (cb) {
    const rs = this.db.createReadStream()
    rs.on('data', ({ key, value }) => {
      this._data[key] = this._valueEncoding.decode(value)
    })
    rs.on('end', () => {
      this.opened = true
      cb()
    })
  }

  _close (cb) {
    this.flush(cb)
  }

  entries () {
    return Object.entries(this._data)
  }

  get (key) {
    return this._data[key]
  }

  has (key) {
    return this._data[key] !== undefined
  }

  set (key, value, flush = true) {
    this._data[key] = value
    this._queue[key] = { value, type: 'put' }
    if (flush) this._queueFlush()
  }

  setFlush (key, value, cb) {
    this.set(key, value)
    this.flush(cb)
  }

  batch (data, flush = true) {
    Object.entries(data).map(([key, value]) => {
      this.set(key, value, { flush: false })
    })
    if (flush) this._queueFlush()
  }

  batchFlush (data, cb) {
    this.batch(data)
    this.flush(cb)
  }

  delete (key, cb, flush = true) {
    this._data[key] = undefined
    this._queue[key] = { type: 'del' }
    if (flush) this._queueFlush()
  }

  deleteFlush (key, cb) {
    this.delete(key)
    this.flush(cb)
  }

  _queueFlush (opts, cb) {
    if (this._flushQueued) return
    this._flushQueued = true
    process.nextTick(() => {
      this._flushQueued = false
      this.flush()
    })
  }

  flush (cb = noop) {
    this._lock(release => {
      if (!Object.keys(this._queue).length) return release(cb)
      const queue = Object.entries(this._queue).map(([key, { value, type }]) => {
        if (value) value = this._valueEncoding.encode(value)
        return { key, value, type }
      })
      this.queue = {}
      this.db.batch(queue, release.bind(null, cb))
    })
  }
}

function noop () {}
