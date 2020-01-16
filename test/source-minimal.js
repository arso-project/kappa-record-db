const tape = require('tape')
const { runAll, replicate } = require('./lib/util')
const Database = require('../db')
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
      console.log('records', records)
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
    cb => db.put(doc('foo'), (err, _id) => {
      t.error(err)
      id = _id
      cb()
    }),
    cb => db.put(doc('bar', id), cb),
    cb => {
      db2 = new Database({ key: db.key, name: 'db2', alias: 'w2', validate: false })
      db2.ready(cb)
    },
    cb => replicate(db, db2, cb),
    cb => db.putSource(db2.localKey, { alias: 'w2' }, cb),
    cb => db2.put(doc('baz', id), cb),
    cb => checkOne(t, db, { schema: 'doc' }, 'baz', cb),
    cb => checkOne(t, db2, { schema: 'doc' }, 'baz', cb),
    cb => t.end()
  ])
})

function checkOne (t, db, query, value, cb) {
  db.query('records', query, { waitForSync: true }, (err, records) => {
    t.error(err)
    t.equal(records.length, 1)
    t.equal(records[0].value, value)
    cb()
  })
}
