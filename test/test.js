const { runAll } = require('./lib/util')
const tape = require('tape')
const collect = require('stream-collector')
const Database = require('../db')

const docSchema = {
  properties: {
    title: { type: 'string' },
    body: { type: 'string' },
    tags: { type: 'array', index: true, items: { type: 'string' } }
  }
}

const groupSchema = {
  properties: {
    name: { type: 'string' },
    docs: {
      type: 'array', index: true, items: { type: 'string' }
    }
  }
}

tape('basics', async t => {
  const db = new Database()
  let id1
  let docIds
  await runAll([
    cb => db.ready(cb),
    cb => db.putSchema('doc', docSchema, cb),
    cb => db.put({ schema: 'doc', value: { title: 'hello', body: 'world', tags: ['red'] } }, (err, id) => {
      t.error(err)
      console.log('put', id)
      id1 = id
      process.nextTick(cb)
    }),
    cb => {
      db.put({ schema: 'doc', value: { title: 'hi', body: 'mars', tags: ['green'] } }, cb)
    },
    cb => {
      db.put({ schema: 'doc', value: { title: 'hello', body: 'moon', tags: ['green'] }, id: id1 }, cb)
    },
    cb => {
      // db.kappa.ready('records', () => {
      setTimeout(() => {
        db.loadStream(db.api.records.get({ schema: 'doc' }), (err, records) => {
          console.log('oi')
          t.error(err)
          t.equal(records.length, 2, 'records get len')
          t.deepEqual(records.map(r => r.value.title).sort(), ['hello', 'hi'], 'record get vals')
          docIds = records.map(r => r.id)
          cb()
        })
      }, 100)
    },
    cb => {
      db.loadStream(db.api.index.query({ schema: 'doc', prop: 'tags', value: 'green' }), (err, records) => {
        t.deepEqual(records.map(r => r.value.body).sort(), ['mars', 'moon'], 'query')
        cb()
      })
    },
    cb => {
      db.putSchema('group', groupSchema, cb)
    },
    cb => setTimeout(cb, 100),
    cb => {
      db.put({
        schema: 'group',
        value: {
          name: 'stories',
          docs: docIds
        }
      }, (err, id) => {
        t.error(err)
        t.ok(id)
        db.get({ id }, { waitForSync: true }, (err, res) => {
          t.error(err)
          t.equal(res.length, 1)
          t.equal(res[0].value.name, 'stories')
          cb()
        })
      })
    },
    cb => setTimeout(() => cb(), 200),
    cb => {
      db.kappa.ready(() => {
        collect(db.loadStream(db.api.records.get({ schema: 'group' }), (err, records) => {
          t.error(err)
          t.equal(records.length, 1)
          t.equal(records[0].value.name, 'stories')
          cb()
        }))
      })
    },
    cb => {
      db.put({
        schema: 'doc',
        value: {
          title: 'foosaturn',
          tags: ['saturn']
        }
      })
      let pending = 2
      const query = { schema: 'doc', prop: 'tags', value: 'saturn' }
      db.query('index', query, { waitForSync: true }, (err, res) => {
        t.error(err)
        t.equal(res.length, 1)
        t.equal(res[0].value.title, 'foosaturn', 'waitforsync true has result')
        if (--pending === 0) cb()
      })
      db.query('index', query, { waitForSync: false }, (err, res) => {
        t.error(err)
        t.equal(res.length, 0, 'waitforsync false no result')
        if (--pending === 0) cb()
      })
    }

  ])
  t.end()
})
