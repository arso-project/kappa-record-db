const { Record: RecordEncoding } = require('./messages')

module.exports = class Record {
  static get PUT () { return RecordEncoding.Type.PUT }
  static get DEL () { return RecordEncoding.Type.DEL }

  static decode (buf, props = {}) {
    let record = RecordEncoding.decode(buf)
    record = { ...record, ...props }
    if (Buffer.isBuffer(record.key)) record.key = record.key.toString('hex')
    if (record.seq) record.seq = Number(record.seq)
    if (record.value) record.value = JSON.parse(record.value)
    return record
  }

  static decodeValue (msg) {
    const value = msg.value
    delete msg.value
    return Record.decode(value, msg)
  }

  static encode (record) {
    record = Record.encodeValue(record)
    const buf = RecordEncoding.encode(record)
    return buf
  }

  static encodeValue (record) {
    if (record.value) record.value = JSON.stringify(record.value)
    return record
  }
}

