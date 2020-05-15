const Ajv = require('ajv')
const debug = require('debug')('db:schema')
const SCHEMAS = require('./schemas')
const { sink } = require('./util')

module.exports = class SchemaStore {
  constructor (opts = {}) {
    this.key = opts.key
    this.schemas = {}
    this.ajv = new Ajv()

    for (const schema of SCHEMAS) {
      this.put(schema)
    }
  }

  setKey (key) {
    this.key = key
  }

  open (db, cb) {
    this.key = db.key
    const qs = db.createQueryStream('records', { schema: 'core/schema' }, { live: true })
    qs.once('sync', cb)
    qs.pipe(sink((record, next) => {
      this.put(record.value)
      next()
    }))
  }

  put (schema) {
    const name = schema.name || schema.$id
    // TODO: Handle error.
    if (!name) return false
    schema = this.parseSchema(name, schema)
    // TODO: Handle error
    try {
      const valid = this.ajv.addSchema(schema, name)
      if (!valid) this._lastError = new ValidationError(this.ajv.errorsText(), this.ajv.errors)
      if (!valid) return false
      this.schemas[name] = schema
      return true
    } catch (err) {
      this._lastError = err
      return false
    }
  }

  get (name) {
    if (typeof name === 'object') {
      let { schema, key } = name
      name = this.resolveName(schema, key)
    } else {
      name = this.resolveName(name)
    }
    return this.schemas[name]
  }

  delete (name) {
    const basicSchemas = ['core/schema', 'core/source', 'core/any', 'sonar/resource']
    if (typeof name === 'object') {
      let { schema, key } = name
      name = this.resolveName(schema, key)
    } else {
      name = this.resolveName(name)
    }
    if (this.has(name)) {
      if (basicSchemas.includes(name)) {
        const err =  new Error('Can not delete basic schema.')
        return err
      }
      delete this.schemas[name]
      return(null)
    }
    else {
      const err = new Error('Schema not found')
      return err
    }
  }

  has (name) {
    return !!this.get(name)
  }

  list () {
    return { ...this.schemas }
  }

  fake (msg) {
    const schema = {
      $id: msg.schema,
      type: 'object'
    }
    this.put(schema)
  }

  validate (record) {
    const name = this.resolveName(record.schema)
    let result = false
    try {
      result = this.ajv.validate(name, record.value)
      if (!result) this._lastError = new ValidationError(this.ajv.errorsText(), this.ajv.errors)
    } catch (err) {
      this._lastError = err
    }
    return result
  }

  get error () {
    return this._lastError
  }

  resolveName (name, key) {
    console.log("RESOLVED NAME: ", name)
    if (!key) key = this.key
    if (Buffer.isBuffer(key)) key = key.toString('hex')
    if (name.indexOf('/') === -1) name = key + '/' + name
    // if (name.indexOf('@') === -1) {
    // TODO: Support versions
    // name = name + '@0'
    // }
    console.log("KEY: ", key)
    console.log("NAME: ", name)
    return name
  }

  parseSchema (name, schema, key) {
    name = this.resolveName(name, key)
    return {
      properties: {},
      type: 'object',
      ...schema,
      $id: name,
      name
    }
  }
}

class ValidationError extends Error {
  constructor (message, errors) {
    super(message)
    this.errors = errors
  }
}
