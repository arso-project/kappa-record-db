const { runAll } = require('./lib/util')
const tape = require('tape')
const collect = require('stream-collector')
const Database = require('../db')

const DOC = {
  properties: {
    title: { type: 'string' },
    body: { type: 'string' },
    tags: { type: 'array', index: true, items: { type: 'string' } }
  }
}

const GROUP = {
  properties: {
    name: { type: 'string' },
    docs: {
      type: 'array', index: true, items: { type: 'string' }
    }
  }
}

const DOC1 = {
  schema: 'doc',
  value: { title: 'hello', body: 'world', tags: ['red'] }
}

tape('replication', async t => {
  const db = new Database()
  let db2, id1, docIds
  await runAll([
    cb => db.ready(cb),
    cb => {
      db2 = new Database({ key: db.key })
      cb()
    },
    cb => db2.ready(cb),
    cb => {
      console.log('DB', db)
      console.log('DB2', db2)
      cb()
    },
    cb => db.putSchema('doc', DOC, cb),
    cb => db.put(DOC1, (err, id) => {
      t.error(err)
      id1 = id
      process.nextTick(cb)
    }),
    // cb => {
    //   db.putSource(db2.localKey, cb)
    //   db2.putSource(db.localKey, cb)
    // },
    // cb => {
    //   setTimeout(() => {
    //     // t.end()
    //   }, 200)
    // },
    cb => {
      const stream = db.replicate(true, { live: true })
      stream.pipe(db2.replicate(false, { live: true })).pipe(stream)
      setTimeout(cb, 200)
    },
    cb => {
      console.log('After replication')
      console.log('DB', db)
      console.log('DB2', db2)
      db2.query('records', { schema: 'doc' }, (err, records) => {
        t.error(err)
        t.equal(records.length, 1)
        t.equal(records[0].id, id1)
        cb()
      })
    },
    cb => {
      db2.put({ schema: 'doc', value: { title: 'hi', body: 'mars', tags: ['green'] } }, cb)
    },
    cb => {
      db2.put({ schema: 'doc', value: { title: 'hello', body: 'moon', tags: ['green'] }, id: id1 }, cb)
    },
    cb => {
      db.putSource(db2.localKey, cb)
    },
    cb => setTimeout(cb, 100),
    cb => {
      console.log('After putSource')
      console.log('DB', db)
      console.log('DB2', db2)
      db.query('records', { schema: 'doc' }, (err, records) => {
        t.error(err)
        t.equal(records.length, 2, 'records get len')
        t.deepEqual(records.map(r => r.value.title).sort(), ['hello', 'hi'], 'record get vals')
        docIds = records.map(r => r.id)
        cb()
      })
    },
    cb => {
      db.query('indexes', { schema: 'doc', prop: 'tags', value: 'green' }, (err, records) => {
        t.error(err)
        t.deepEqual(records.map(r => r.value.body).sort(), ['mars', 'moon'], 'query')
        cb()
      })
    },
    cb => {
      db.putSchema('group', GROUP, cb)
    },
    cb => setTimeout(cb, 100),
    cb => {
      db.put({
        schema: 'group',
        value: {
          name: 'stories',
          docs: docIds
        }
      }, cb)
    },
    cb => setTimeout(cb, 100),
    cb => {
      db2.kappa.ready('kv', () => {
        // collect(db.loadStream(db.api.records.get({ schema: 'group' }), (err, records) => {

        db2.query('records', { schema: 'group' }, { load: true }, (err, records) => {
          t.error(err)
          t.equal(records.length, 1)
          t.equal(records[0].value.name, 'stories')
          t.end()
        })
      })
    }
  ])
  t.end()
})
