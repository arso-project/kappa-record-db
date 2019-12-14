# kappa-record-db

A peer-to-peer database built on [hypercores](https://github.com/mafintosh/hypercore) and [kappa-core@experimental](https://github.com/Frando/kappa-core#experimental).

* Index a set of hypercores efficiently into materialized-view style secondary indexes
* A simple and universal *record* data model: Each record has an *id*, a *schema*, and a *value*. 

Basicaly this means: put in json, optionally specify its JSON schema (for validation and uniformness), and sync effortlessly with other peers. 

A *database* refers to a particular set of feeds. The current model starts with a single feed, and that feed then can add other feeds to the set of authorized sources. An second model, where all feeds that swarm under a shared key are considered authorized, will be added soon.

Internally, the database uses [unordered-materialized-kv](https://github.com/digidem/unordered-materialized-kv/) to have a shared notion of the latest versions of a record.

## Installation

#### `npm install kappa-record-db`

## API

`const Database = require('kappa-record-db')`

#### `const db = new Database(opts)`

required opts are:
  * either `corestore`: A [corestore](https://github.com/andrewosh/corestore) instance.
  * or `storage`: A string to a file system path or a [random-access-storage](https://github.com/random-access-storage/) instance.
opts are:

optional opts are:
* `key`: A unique key for this database. Will be a random key otherwise.
* `db`: A [levelup](https://github.com/Level/levelup) instance (optional, defaults to inmemory level-mem).
* `validate`: Enable strict validation of all records being put into the database (default: true).

#### `db.ready(cb)`

`cb` is called after the database is fully initialized.

#### `db.replicate(opts)`

Create a hypercore-protocol replication stream. If piped into the replicate method of another database with the same key, the databases will exchange their updates.

#### `db.putSource(key, cb)`

Add an additional source feed. The key of the database's local writable feed is available at `db.localKey`.

#### `db.put(record, cb)`

Put a record into the database. 

* `record` is a plain js object: `{ id, schema, value }`.
  * `id` is the record's unique id. Leave empty when you want to create a new record.
  * `schema` is a string that sets the record's type. If the `validate` opt is true, the put only succeeds if a schema by that name is in the database and if the record correctly validates with this schema.
  * `value` is a JavaScript object. It has to be serializable to JSON. If the record's schema has its definition stored in the database, the value has to conform to the schema.
* `cb` is a callback that will be called with `(err, id)`.


#### `db.get(req, [opts], cb)`

Get a record from the database. `req` is a plain js object with:

```javascript
{
  id: 'string' 
  schema: 'string' 
  key: 'string' 
  seq: int 
}
```
Either `id` or both `key` and `seq` are required. 

#### `db.putSchema(name, schema, cb)`

Save a schema into the database. The schema has to be valid [JSON Schema](https://json-schema.org), with additional optional properties. The top level properties of the JSON schema will be filled automatically.

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

Additional optional properties per property:

* `index`: Set on a top-level simple field to index values of that field in the database.

#### `db.getSchema(id, cb)`

Get a schema definition from the database.

#### `db.query(name, args, [opts], [cb])`

Query the database. Queries are defined by views (see below).

Returns a readable stream of results. If `cb` is a function, it will be called with `(err, results)` instead of returning a readable stream.

#### `db.use(name, createView, [opts])`

Register a new database view. Views are functions that will be called whenever records are being put or deleted. The database maintains the state of each view so that they catch up on updates automatically. See [kappa-core](https://github.com/kappa-db/kappa-core) for good introduction on how to work with kappa views.

`name` is the name of the view. It has to be unique per database.

`createView` is a constructor function. It will be called with `(level, db, opts)`:

* `level`: an [LevelUp](https://github.com/Level/levelup)-compatible LevelDB instance for this view
* `db`: the database
* `opts`: optional opts passed into `useRecordView`

The constructor function has to return a view object with the following keys:

* `map: function (records, next) {}`
    This function will be called with a batch of records. Process the entries (e.g. by inserting rows into the leveldb). Call `next()` when done.
* `api`: An object of query functions that this view exposes to the outside world. They should be safe to call (may not modify data) as they may be called from the client side.

If the view provides a `query` function on its api object, this function will be callable as a query on the main database. The query function has to return a readable stream of objects with either `{ id }` or `{ key, seq }` properties. Result objects may also include a `meta` property.
