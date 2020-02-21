const { runAll } = require('./lib/util')
const tape = require('tape')
const collect = require('stream-collector')
const Database = require('../db')

tape('query bitfield', t => {
  const db = new Database({ validate: false })
  runAll([
    cb => db.ready(cb),
    cb => {
      const ops = ['a', 'b', 'c', 'd'].map(title => (
        { op: 'put', schema: 'doc', value: { title } }
      ))
      db.batch(ops, cb)
    },
    cb => db.sync(cb),
    cb => {
      db.query('records', { schema: 'doc' }, { cacheid: 'test' }, (err, records) => {
        t.error(err)
        t.equal(records.length, 4)
        t.deepEqual(records.map(r => r.value.title).sort(), ['a', 'b', 'c', 'd'])
        cb()
      })
    },
    cb => {
      db.query('records', { schema: 'doc' }, { cacheid: 'test' }, (err, records) => {
        t.error(err)
        t.equal(records.length, 4)
        t.deepEqual(records.map(r => r.value), [undefined, undefined, undefined, undefined])
        cb()
      })
    },
    cb => t.end()
  ])
})
