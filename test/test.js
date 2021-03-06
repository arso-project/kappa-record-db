const { runAll } = require('./lib/util')
const tape = require('tape')
const collect = require('stream-collector')
const Database = require('..')

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

tape('minimal', t => {
  const db = new Database()
  runAll([
    cb => db.putSchema('doc', docSchema, cb),
    cb => db.put({ schema: 'doc', value: { title: 'hello', body: 'world' } }, cb),
    cb => db.query('records', { schema: 'doc' }, { waitForSync: true }, (err, records) => {
      t.error(err)
      t.equal(records.length, 1)
      t.equal(records[0].value.title, 'hello')
      db.put({ id: records[0].id, schema: 'doc', value: { title: 'hello2', body: 'world' } }, cb)
    }),
    cb => db.query('records', { schema: 'doc' }, { waitForSync: true }, (err, records) => {
      t.error(err)
      t.equal(records.length, 1)
      t.equal(records[0].value.title, 'hello2')
      cb()
    }),
    () => t.end()
  ])
})

tape('delete', t => {
  const db = new Database()
  const schema = 'doc'
  let id
  runAll([
    cb => db.putSchema(schema, docSchema, cb),
    cb => db.put({ schema, value: { title: 'hello', body: 'world' } }, (err, id1) => {
      if (err) return cb(err)
      id = id1
      cb()
    }),
    cb => db.sync(cb),
    cb => db.query('records', { id }, (err, records) => {
      t.error(err)
      t.equal(records.length, 1, 'queried one record')
      cb()
    }),
    cb => db.del({ id, schema }, cb),
    cb => db.sync(cb),
    cb => db.query('records', { id }, (err, records) => {
      t.error(err)
      t.equal(records.length, 0, 'queried zero records')
      cb()
    }),
    cb => t.end()
  ])
})

tape('kitchen sink', async t => {
  const db = new Database()
  let id1
  let docIds
  await runAll([
    cb => db.ready(cb),
    cb => db.putSchema('doc', docSchema, cb),
    cb => db.put({ schema: 'doc', value: { title: 'hello', body: 'world', tags: ['red'] } }, (err, id) => {
      t.error(err)
      // console.log('put', id)
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
        db.query('records', { schema: 'doc' }, (err, records) => {
          // console.log('oi')
          t.error(err)
          t.equal(records.length, 2, 'records get len')
          t.deepEqual(records.map(r => r.value.title).sort(), ['hello', 'hi'], 'record get vals')
          docIds = records.map(r => r.id)
          cb()
        })
      }, 100)
    },
    cb => {
      db.query('index', { schema: 'doc', prop: 'tags', value: 'green' }, (err, records) => {
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
        db.query('records', { id }, { waitForSync: true }, (err, res) => {
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
        db.query('records', { schema: 'group' }, (err, records) => {
          t.error(err)
          t.equal(records.length, 1)
          t.equal(records[0].value.name, 'stories')
          cb()
        })
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
