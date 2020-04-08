const multiplexer = require('multifeed/mux')
const through = require('through2')
const debug = require('debug')('mux')
const pretty = require('pretty-hash')

module.exports = { replicate, init }
// Taken and adapted from multifeed/index.js
function replicate (self, isInitiator, opts) {
  if (!self.opened) {
    var tmp = through()
    process.nextTick(function () {
      tmp.emit('error', new Error('tried to use "replicate" before multifeed is ready'))
    })
    return tmp
  }

  if (!opts) opts = {}
  self.writerLock = self.lock

  var rootKey = self._address

  var mux = multiplexer(isInitiator, rootKey, Object.assign({}, opts, { _id: self._id }))

  // Add key exchange listener
  var onManifest = function (m) {
    mux.requestFeeds(m.keys)
  }
  mux.on('manifest', onManifest)

  // Add replication listener
  var onReplicate = function (keys, repl) {
    console.log(self._id, 'RECV', keys.map(pretty))
    addMissingKeys(keys, function (err) {
      if (err) return mux.stream.destroy(err)

      // Create a look up table with feed-keys as keys
      // (since not all keys in self._feeds are actual feed-keys)
      var key2feed = values(self._feeds).reduce(function (h, feed) {
        h[feed.key.toString('hex')] = feed
        return h
      }, {})

      // Select feeds by key from LUT
      var feeds = keys.map(function (k) { return key2feed[k] })
      // console.log(self._id, 'REPL', feeds.map(f => pretty(f.key)))
      repl(feeds)
    })
  }
  mux.on('replicate', onReplicate)

  // Start streaming
  self.ready(function (err) {
    if (err) return mux.stream.destroy(err)
    if (mux.stream.destroyed) return
    mux.ready(function () {
      var keys = values(self._feeds).map(function (feed) { return feed.key.toString('hex') })
      console.log(self._id, 'ANNO', keys.map(pretty))
      mux.offerFeeds(keys)
    })

    // Push session to _streams array
    self._streams.push(mux)

    // Register removal
    var cleanup = function (err) {
      mux.removeListener('manifest', onManifest)
      mux.removeListener('replicate', onReplicate)
      self._streams.splice(self._streams.indexOf(mux), 1)
      debug('[REPLICATION] Client connection destroyed', err)
    }
    mux.stream.once('end', cleanup)
    mux.stream.once('error', cleanup)
  })

  return mux.stream

  // Helper functions

  function addMissingKeys (keys, cb) {
    self.ready(function (err) {
      if (err) return cb(err)
      self.writerLock(function (release) {
        addMissingKeysLocked(keys, function (err) {
          release(cb, err)
        })
      })
    })
  }

  function addMissingKeysLocked (keys, cb) {
    var pending = 0
    debug(self._id + ' [REPLICATION] recv\'d ' + keys.length + ' keys')
    var filtered = keys.filter(function (key) {
      return !Number.isNaN(parseInt(key, 16)) && key.length === 64
    })
    filtered.forEach(function (key) {
      var feeds = values(self._feeds).filter(function (feed) {
        return feed.key.toString('hex') === key
      })
      if (!feeds.length) {
        pending++
        debug(self._id + ' [REPLICATION] trying to create new local hypercore, key=' + key.toString('hex'))
        // self._createFeedFromKey(key, null, function (err) {
        self.addFeed({ key }, function (err) {
          if (err) {
            debug(self._id + ' [REPLICATION] failed to create new local hypercore, key=' + key.toString('hex'))
            debug(self._id + err.toString())
          } else {
            debug(self._id + ' [REPLICATION] succeeded in creating new local hypercore, key=' + key.toString('hex'))
          }
          if (!--pending) cb()
        })
      }
    })
    if (!pending) cb()
  }
}

function init (self) {
  self._streams = []
  self.on('feed', (feed, info) => {
    forwardLiveFeedAnnouncements(feed, info.name)
  })
  function forwardLiveFeedAnnouncements (feed, name) {
    if (!self._streams.length) return // no-op if no live-connections
    var hexKey = feed.key.toString('hex')
    console.log(self._id, 'FWD', pretty(hexKey))
    // Tell each remote that we have a new key available unless
    // it's already being replicated
    self._streams.forEach(function (mux) {
      if (mux.knownFeeds().indexOf(hexKey) === -1) {
        debug('Forwarding new feed to existing peer:', hexKey)
        mux.offerFeeds([hexKey])
      }
    })
  }
}

function values (obj) {
  return Object.keys(obj).map(function (k) { return obj[k] })
}
