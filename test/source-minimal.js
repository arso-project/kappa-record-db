const tape = require('tape')
const { runAll, replicate } = require('./lib/util')
const Database = require('..')
tape('minimal kv test', t => {
  const db = new Database({ name: 'db1', alias: 'w1', validate: false })
  let id
  runAll([
    cb => db.ready(cb),
    cb => db.put({ schema: 'doc', value: 'foo' }, (err, _id) => {
      t.error(err)
      id = _id
      cb()
    }),
    cb => db.put({ schema: 'doc', value: 'bar', id }, cb),
    cb => db.query('records', { schema: 'doc' }, { waitForSync: true }, (err, records) => {
      t.error(err)
      cb()
    }),
    cb => t.end()
  ])
})

function doc (value, id) {
  return { schema: 'doc', value, id }
}

tape('minimal kv test with two sourcees', t => {
  const db = new Database({ name: 'db1', alias: 'w1', validate: false })
  let db2
  let id
  runAll([
    cb => db.ready(cb),
    cb => db.put(doc('1rev1'), (err, _id) => {
      t.error(err)
      id = _id
      cb()
    }),
    cb => db.put(doc('1rev2', id), cb),
    cb => {
      db2 = new Database({ key: db.key, name: 'db2', alias: 'w2', validate: false })
      db2.ready(cb)
    },
    cb => {
      const db2localfeed = db2.getFeed('localwriter')
      const db2localkey = db2localfeed.key
      db.putSource(db2localkey, { alias: 'w2' }, cb)
    },
    cb => replicate(db, db2, cb),
    cb => setTimeout(cb, 400),
    cb => db2.put(doc('2rev1', id), cb),
    cb => checkOne(t, db, { schema: 'doc' }, '2rev1', 'db1 ok', cb),
    cb => checkOne(t, db2, { schema: 'doc' }, '2rev1', 'db2 ok', cb),
    cb => t.end()
  ])
})

function checkOne (t, db, query, value, msg, cb) {
  db.query('records', query, { waitForSync: true }, (err, records) => {
    t.error(err, msg + ' (no err)')
    t.equal(records.length, 1, msg + ' (result len)')
    t.equal(records[0].value, value, msg + '(value)')
    cb()
  })
}
