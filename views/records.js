const through = require('through2')
const keyEncoding = require('charwise')
const { opsForRecords } = require('./helpers')
const Live = require('level-live')
const debug = require('debug')('view:records')
// const collect = require('stream-collector')

const INDEXES = {
  is: ['id', 'schema', 'key'],
  si: ['schema', 'id', 'key']
}

module.exports = function recordView (lvl, db) {
  return {
    map (msgs, next) {
      opsForRecords(db, msgs, mapToPutOp, (err, ops) => {
        debug('map: msgs %s, ops %s err %s', msgs.length, ops.length, err)
        if (err) return next(err)
        lvl.batch(ops, () => {
          next()
        })
      })
    },

    api: {
      query (kappa, req, opts = {}) {
        if (!req) return this.view.all(opts)
        if (typeof req === 'string') req = { id: req }
        let { schema, id, key, seq } = req

        if (schema) schema = db.schemas.resolveName(schema)

        let filter
        if (schema && !id) {
          filter = includerange(['si', schema])
        } else if (!schema && id) {
          filter = includerange(['is', id])
        } else {
          filter = includerange(['is', id, schema])
        }

        let rs = query(lvl, { ...opts, ...filter })
        if (key) rs = rs.pipe(filterSource(key, seq))
        return rs
      },

      get (kappa, req, opts) {
        return this.view.query(req, opts)
      },

      all (kappa, cb, opts) {
        return query(lvl, includerange(['is']), opts)
      },

      bySchema (kappa, schema, opts) {
        return this.view.query({ schema }, opts)
      },

      byId (kappa, id, opts) {
        return this.view.query({ id }, opts)
      },

      byIdAndSchema (kappa, id, schema, opts) {
        return this.view.query({ id, schema }, opts)
      }
    }
  }
}

function query (db, opts) {
  opts.keyEncoding = keyEncoding
  const transform = parseRow()
  let rs
  if (opts.live) {
    rs = new Live(db, opts)
    rs.once('sync', () => transform.emit('sync'))
  } else {
    rs = db.createReadStream(opts)
  }
  return rs.pipe(transform)
}

function validate (msg) {
  const result = msg.id && msg.schema && msg.key && typeof msg.seq !== 'undefined'
  return result
}

function mapToPutOp (msg, db) {
  const ops = []
  if (!validate(msg)) return ops
  const value = msg.seq || 0
  const shared = { value, keyEncoding }
  Object.entries(INDEXES).forEach(([key, fields]) => {
    fields = fields.map(field => msg[field])
    ops.push({
      key: [key, ...fields],
      ...shared
    })
  })
  return ops
}

function parseRow () {
  return through.obj(function (row, enc, next) {
    const { key, value: seq, type } = row
    const idx = key.shift()
    const index = INDEXES[idx]
    if (!index) return next()
    const record = { seq: Number(seq), type }
    for (let i = 0; i < key.length; i++) {
      record[index[i]] = key[i]
    }
    this.push(record)
    next()
  })
}

function filterSource (key, seq) {
  return through.obj(function (row, enc, next) {
    if (row.key === key) {
      if (!seq || seq === row.seq) this.push(row)
    }
    next()
  })
}

function includerange (key) {
  return {
    gte: [...key],
    lte: [...key, undefined]
  }
}
