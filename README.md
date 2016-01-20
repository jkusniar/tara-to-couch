Tara to couchdb migration
==

## Prerequisites

* couchdb server on local or remote host
* postgresql server on local or remote host

## Bootstrap
```
npm install
```

## Run

* clear target couch DB:

```
$ curl -X DELETE http://127.0.0.1:5984/lara
```

* create target couch DB:

```
$ curl -X PUT http://127.0.0.1:5984/lara
```

* run migration

```
$ node migration.js
```
