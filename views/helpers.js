module.exports = { opsForRecords }

function opsForRecords (db, records, map, cb) {
  let pending = records.length
  const ops = []
  records.forEach(record => collectOps(db, record, map, done))
  function done (err, curOps) {
    if (!err) ops.push(...curOps)
    if (--pending === 0) cb(null, ops)
  }
}

function collectOps (db, record, map, cb) {
  db.api.kv.isLinked(record, (err, isOutdated) => {
    if (err || isOutdated) return cb(null, [])
    // check if we have to delete other records.
    collectDeletes(db, record, map, (err, ops = []) => {
      if (err) return cb(err)
      ops = mapToDel(ops)
      // finally, add the put itself.
      if (!record.delete) ops.push(...mapToPut(map(record, db)))
      cb(err, ops)
    })
  })
}

function mapToPut (ops) {
  return ops.map(op => {
    return { ...op, type: 'put' }
  })
}

function mapToDel (ops) {
  return ops.map(op => {
    return { ...op, type: 'del' }
  })
}

function collectDeletes (db, record, map, cb) {
  let ops = []
  if (record.delete) {
    ops = map(record, db)
  }

  let pending = record.links.length + 1
  record.links.forEach(link => {
    db.loadLink(link, (err, record) => {
      if (err || !record) return done()
      ops.push(...map(record, db))
      done()
    })
  })
  done()

  function done () {
    if (--pending === 0) {
      ops = mapToDel(ops)
      cb(null, ops)
    }
  }
}
