const crypto = require('crypto')
const base32 = require('base32')
const { Writable, Transform } = require('stream')

exports.keyseq = function (record) {
  return record.key + '@' + record.seq
}

exports.uuid = function () {
  return base32.encode(crypto.randomBytes(16))
}

exports.through = function (transform) {
  return new Transform({
    objectMode: true,
    transform
  })
}

exports.sink = function (fn) {
  return new Writable({
    objectMode: true,
    write (msg, enc, next) {
      fn(msg, next)
    }
  })
}

exports.once = function (fn) {
  let called = false
  return (...args) => {
    if (!called) fn(...args)
    called = true
  }
}

exports.defaultTrue = function (val) {
  if (typeof val === 'undefined') return true
  return !!val
}

exports.noop = function () {}
