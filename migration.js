var getSlug = require('speakingurl');

var pg = require('pg');
var conString = "postgres://postgres:postgres@localhost/tara";

var PouchDB = require('pouchdb');
var couchString = 'http://localhost:5984/lara';

function getPqClient() {
    var pqClient = new pg.Client(conString);
    pqClient.on('drain', pqClient.end.bind(pqClient)); //disconnect client when all queries are finished
    pqClient.connect();

    return pqClient;
}

function generateClientId(clientRec) {
    var id = 'record/' + getSlug(clientRec.lastName, { lang: 'sk' }) + '/';

    var firstName = getSlug(clientRec.firstName, { lang: 'sk' });
    if (firstName !== '') {
        id += firstName + '/';
    }

    id += clientRec.legacyId;

    return id;
}

function migration() {
    console.log('$ Deleting empty clients with no patient records');
    var err = false;
    var pqClient = getPqClient();
    var query = pqClient.query('DELETE FROM client WHERE id IN ' +
        '(SELECT c.id FROM client c LEFT JOIN patient p ON c.id = p.client_id ' +
        'WHERE p.id IS NULL)');
    query.on('error', function (error) {
        err = true;
        console.log(' -> Error deleting empty client records: ' + error)
    });
    query.on('end', function (result) {
        console.log(' -> ' + result.rowCount + ' rows were deleted');
        if (!err) {
            // TODO trim empty characters off first and last name 
            migrateClients();
        }
    });
}

function migrateClients() {
    console.log('$ Migrating clients');
    var pqClient = getPqClient();

    var clients = [];
    var i = 1;

    var query = pqClient.query('SELECT c.id as legacy_id, c.first_name, c.last_name, t.name as title, ' +
        'c.phone_1, c.phone_2, c.email, c.note, c.ic, c.dic, c.icdph, ' +
        'ci.city, ci.psc as city_zip, s.street, s.psc as street_psc, c.house_no ' +
        'FROM client c ' +
        'LEFT JOIN lov_title t on t.id = c.title_id ' +
        'LEFT JOIN lov_city ci on ci.id = c.city_id ' +
        'LEFT JOIN lov_street s on s.id = c.street_id ');
        //' where last_name like \'% %\''); // TODO: REMOVE DEBUG CONDITION!!
    query.on('row', function (row) {
        var zip = row.city_zip;
        if (row.street_psc != null) {
            zip = row.street_psc;
        }

        var clientRec = {
            legacyId: row.legacy_id,
            firstName: row.first_name,
            lastName: row.last_name,
            title: row.title,
            phone1: row.phone_1,
            phone2: row.phone_2,
            email: row.email,
            note: row.note,
            ic: row.ic,
            dic: row.dic,
            icdph: row.icdph,
            city: row.city,
            street: row.street,
            zipcode: zip,
            houseNo: row.house_no,
            created: new Date().toJSON(),
            type: 'client'
        };
        
        // FIXME: might be better to use https://github.com/pouchdb/collate/ for ID generation
        // if search by last name is to be implemented using primary index (allDocs() and _id)
        clientRec._id = generateClientId(clientRec);

        clients.push(clientRec);
        
        // dump to pouch!
        if (i++ % 100 == 0) {
            console.log(' -> pushing ' + clients.length + ' to pouch');
            dumpToPouch(clients);
            clients = [];
        }
    });
    query.on('end', function (result) {
        // process remaining records
        if (clients.length != 0) {
            console.log(' -> pushing remaining ' + clients.length + ' to pouch');
            dumpToPouch(clients);
        }
        console.log(' -> rows received: ' + result.rowCount);
        console.log(' -> records processed: ' + (i - 1));
    });
}

function dumpToPouch(docs) {
    var db = new PouchDB(couchString);
    db.bulkDocs(docs).then(function (result) {
        for (var i = 0; i < result.length; i++) {
            var res = result[i];
            if (res.error) {
                console.log(' -> failed to insert record ' + res);
            }
        }
    }).catch(function (err) {
        console.log(' -> error pushing records to couch ' + err);
    });
}


// migration procedure
migration();


