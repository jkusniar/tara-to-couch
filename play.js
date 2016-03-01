var PouchDB = require('pouchdb');
var couchString = 'http://localhost:5984/lara';

function searchByClientName() {
    var db = new PouchDB(couchString);
    db.allDocs({
        include_docs: true,
        startkey: 'record/abrah',
        endkey: 'record/abrah\uffff',
        limit: 100,
    }).then(function (result) {
        console.log(result);
    }).catch(function (err) {
        console.log(err);
    });
}

searchByClientName();
