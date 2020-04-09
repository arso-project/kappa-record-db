const tape = require('tape')
const { runAll, replicate } = require('./lib/util')
const Database = require('..')

const MAX = 10000

tape('insert many', t => {
  const db = new Database({ name: 'db1', alias: 'w1', validate: false })
  const timer = clock()
  runAll([
    cb => db.ready(cb),
    cb => logtime(cb, timer, 'ready'),

    // cb => {
    //   let pending = 0
    //   for (let i = 1; i <= MAX; i++) {
    //     ++pending
    //     db.put({ schema: 'doc', value: 'bar' }, done)
    //   }
    //   function done () {
    //     if (--pending === 0) cb()
    //   }
    // },

    cb => {
      let batch = []
      for (let i = 1; i <= MAX; i++) {
        batch.push({ schema: 'doc', value: 'bar' })
      }
      db.api.db.batch(batch, cb)
    },

    cb => logtime(cb, timer, 'inserted ' + MAX),
    cb => db.query('records', { schema: 'doc' }, { waitForSync: true }, (err, records) => {
      t.error(err)
      t.equal(records.length, MAX)
      cb()
    }),
    cb => logtime(cb, timer, 'queried'),
    cb => db.query('records', { schema: 'doc' }, { waitForSync: true }, (err, records) => {
      t.error(err)
      t.equal(records.length, MAX)
      cb()
    }),
    cb => logtime(cb, timer, 'queried'),
    cb => logtime(cb, timer.total, 'total'),

    cb => rundb2(cb),
    cb => t.end()
  ])

  function rundb2 (finish) {
    const timer = clock()
    const db2 = new Database({ name: 'db2', key: db.key })
    console.log()
    console.log('now sync to db2')
    runAll([
      cb => db2.ready(cb),
      cb => logtime(cb, timer, 'ready'),
      cb => replicate(db, db2, cb),
      cb => db2.query('records', { schema: 'doc' }, { waitForSync: true }, (err, records) => {
        t.error(err)
        t.equal(records.length, MAX)
        cb()
      }),
      cb => logtime(cb, timer, 'queried'),
      cb => db2.query('records', { schema: 'doc' }, { waitForSync: true }, (err, records) => {
        t.error(err)
        t.equal(records.length, MAX)
        cb()
      }),
      cb => logtime(cb, timer, 'queried'),
      cb => logtime(cb, timer.total, 'total'),
      cb => finish()
    ])
  }
})

function logtime (cb, timer, message) {
  console.log(timer(), message)
  cb()
}

function clock () {
  let start = process.hrtime()
  let last = start
  function measure () {
    const interval = process.hrtime(last)
    last = process.hrtime()
    return fmt(interval)
  }
  measure.total = function () {
    const interval = process.hrtime(start)
    return fmt(interval)
  }
  return measure
}

function fmt (time) {
  const ds = time[0]
  const dn = time[1]
  const ns = (ds * 1e9) + dn
  const ms = round(ns / 1e6)
  const s = round(ms / 1e3)
  if (s >= 1) return s + 's'
  if (ms >= 0.01) return ms + 'ms'
  if (ns) return ns + 'ns'
}

function round (num, decimals = 2) {
  return Math.round(num * Math.pow(10, decimals)) / Math.pow(10, decimals)
}
