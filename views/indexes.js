const through = require('through2')
const pump = require('pump')

const { mapRecordsIntoLevelDB } = require('./helpers')

const CHAR_END = '\uffff'
const CHAR_SPLIT = '\u0000'
const CHAR_START = '\u0001'

module.exports = function indexedView (lvl, db) {
  return {
    name: 'indexes',
    map (records, next) {
      mapRecordsIntoLevelDB({
        db, records, map: mapToIndex, level: lvl
      }, next)
    },
    api: {
      query (kappa, opts, cb) {
        // const { schema, prop, value, gt, lt, gte, lte, reverse, limit } = opts
        const proxy = transform()
        if (!opts.schema || !opts.prop) {
          proxy.destroy(new Error('schema and prop are required.'))
        } else {
          opts.schema = db.schemas.resolveName(opts.schema)
          if (!opts.schema) return proxy.destroy(new Error('Invalid schema name.'))
          const lvlopts = queryOptsToLevelOpts(opts)
          lvl.createReadStream(lvlopts).pipe(proxy)
        }
        return proxy
      }
    }
  }
}

function mapToIndex (msg, db) {
  const schema = db.getSchema(msg)
  const ops = []
  const { id, key: source, seq, schema: schemaName, value, lseq } = msg
  if (!schema || !schema.properties) return ops
  // TODO: Recursive?
  for (const [field, def] of Object.entries(schema.properties)) {
    // Only care for fields that want to be indexed and are not undefined.
    if (!def.index) continue
    if (typeof value[field] === 'undefined') continue

    if (def.type === 'array') var values = value[field]
    else values = [value[field]]
    values.forEach(val => {
      ops.push({
        key: [schemaName, field, val, lseq].join(CHAR_SPLIT),
        value: ''
      })
    })
  }
  return ops
}

function queryOptsToLevelOpts (opts) {
  const { schema, prop, reverse, limit, value, gt, gte, lt, lte } = opts
  const lvlopts = { reverse, limit }
  const key = schema + CHAR_SPLIT + prop + CHAR_SPLIT
  lvlopts.gt = key + CHAR_SPLIT
  lvlopts.lt = key + CHAR_END
  if (value) {
    lvlopts.gt = key + value + CHAR_SPLIT
    lvlopts.lt = key + value + CHAR_SPLIT + CHAR_END
  } else if (gt) {
    lvlopts.gt = key + gt + CHAR_SPLIT
    lvlopts.lt = key + gt + CHAR_END
  } else if (gte) {
    lvlopts.gte = key + gte + CHAR_SPLIT
    lvlopts.lt = key + gte + CHAR_END
  }
  if (lt) {
    lvlopts.lt = key + lt + CHAR_START
  } else if (lte) {
    lvlopts.lt = undefined
    lvlopts.lte = key + lte + CHAR_END
  }
  return lvlopts
}

function transform () {
  return through.obj(function (row, enc, next) {
    const decoded = decodeNode(row)
    this.push(decoded)
    next()
  })
}

function decodeNode (node) {
  const { key, value: _ } = node
  const [schema, prop, value, lseq] = key.split(CHAR_SPLIT)
  // return { schema, id, key: source, seq, params: { prop, value } }
  return { lseq }
}
