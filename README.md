# kappa-record-db

A peer-to-peer database based on hypercores and kappa-core. Successor to [hyper-content-db](https://github.com/arso-project/hyper-content-db/).

## Installation

#### `npm install kappa-record-db`

## API

`const Database = require('kappa-record-db')`

#### `const db = new Database(opts)`

opts are:
* `key`: A unique key for this database (optional)
* `db`: A [levelup](https://github.com/Level/levelup) instance (optional, defaults to inmemory level-mem)
* `storage`: either a string to a file system path or a [random-access-storage](https://github.com/random-access-storage/) instance.
* `corestore`: A [corestore](https://github.com/andrewosh/corestore)
* `validate`: Enable strict validation of all records being put into the database (default: true)

#### `db.ready(cb)`

`cb` is called after the database is fully initialized.

#### `db.replicate(opts)`

Create a hypercore-protocol replication stream. 

#### `db.putSource(key, cb)`

Add an additional source feed.

#### `db.put(record, cb)`

Put a record into the database. 

`record` is a plain js object:
```javascript
{
  id: 'string',
  schema: 'string'
  value: someObject,
}
```

* `schema` is required. All records have a schema name. Schemas are identified by strings. Schemas can either be local or well-defined. Local schemas are identifiers that should be unique in the context of the database, their names may not contain slashes (`/`). Well-defined schemas are identified by a domain, followed by an identifier (e.g. `arso.xyz/event`). They have to include exactly one slash.

* `id` identifies a record uniquely within the database. When creating new recors, leave `id` undefined. When updating existing records, `id` is required.

* `value` is the record's value. It has to be a JavaScript object that is serializable to JSON. If the record's schema has its definition stored in the database, the value has to conform to the schema.

* `cb` is a callback that will be called with `(err, id)`.

#### `db.get(req, [opts], cb)`

Get a record from the database. `req` should look like this:

```javascript
{
  id: 'string' // required,
  schema: 'string' // required,
  source: 'string' // optional,
  seq: int // optional
}
```

#### `db.putSchema(name, schema, cb)`

Save a schema into the database. The schema declaration follows the [JSON Schema](https://json-schema.org), with some additional properties.

```javascript
const schema = {
  properties: {
    title: {
      type: 'string',
      index: true
    },
    date: {
      type: 'string',
      index: true
    }
  }
}
db.putSchema('event', schema, (err) => {
  if (!err) console.log('schema saved.')
})
```

Supported properties in addition to the JSON Schema spec are:

* `index`: Set on a top-level simple field to index values of that field in the database.

The top-level JSON schema declaration can be omitted and will be filled in automatically. 

> TODO: Also support putting full JSON schemas (including the outer section)

#### `db.getSchema(name, cb)`

Load a schema declaration from the database.

#### `db.useRecordView(name, makeView, [opts])`

Register a new database view. Views are functions that will be called whenever records are being put or deleted. The database maintains the state of each view so that they catch up on updates automatically. See [kappa-core](https://github.com/kappa-db/kappa-core) for good introduction on how to work with kappa views.

`name` is the name of the view. It has to be unique per database.

`makeView` is a constructor function. It will be called with `(level, db, opts)`:

* `level`: an [LevelUp](https://github.com/Level/levelup)-compatible LevelDB instance for this view
* `db`: this db
* `opts`: optional opts passed into `useRecordView`

The constructor function should return a view object with the following keys:

* `map: function (records, next) {}`
    This function will be called with a batch of records. Process the entries (e.g. by inserting rows into the leveldb). Call `next()` when done.
* `api`: An object of query functions that this view exposes to the outside world. They should be safe to call (may not modify data) as they may be called from the client side.
