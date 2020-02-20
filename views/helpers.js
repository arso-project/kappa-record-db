const Record = require('../lib/record')
module.exports = { mapRecordsIntoLevelDB }

function mapRecordsIntoLevelDB (opts, next) {
  const { records, map, level, db } = opts
  let pending = records.length
  const ops = []

  records.forEach(record => collectOps(db, record, map, done))

  function done (err, curOps) {
    if (!err) ops.push(...curOps)
    if (--pending === 0) finish()
  }

  function finish () {
    level.batch(ops, next)
  }
}

function collectOps (db, record, mapFn, cb) {
  const ops = []
  // check if the current record is already outdated,
  // this means do nothing.
  db.api.kv.isLinked(record, (err, isLinked) => {
    if (err) return cb(err)
    if (isLinked) return cb(null, [])

    // check if we have to delete other records because they are
    // linked to by this record.
    collectLinkedRecords(db, record, (err, linkedRecords) => {
      if (err) return cb(null, [])

      // map linked records to delete ops
      for (const linkedRecord of linkedRecords) {
        ops.push(...mapToDel(db, linkedRecord, mapFn))
      }

      // map the current record itself
      if (record.op === Record.PUT) {
        ops.push(...mapToPut(db, record, mapFn))
      } else if (record.op === Record.DEL) {
        ops.push(...mapToDel(db, record, mapFn))
      }

      cb(err, ops)
    })
  })
}

function collectLinkedRecords (db, record, cb) {
  if (!record.links.length) return cb(null, [])
  const records = []
  let pending = record.links.length
  record.links.forEach(link => {
    db.loadLink(link, (err, record) => {
      if (!err && record) records.push(record)
      if (--pending === 0) cb(null, records)
    })
  })
}

function mapToPut (db, record, mapFn) {
  return mapFn(record, db).map(op => {
    return { ...op, type: 'put' }
  })
}

function mapToDel (db, record, mapFn) {
  return mapFn(record, db).map(op => {
    return { ...op, type: 'del' }
  })
}
