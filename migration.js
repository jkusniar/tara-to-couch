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

function getClientId(firstName, lastName, legacyId) {
    var id = 'record/' + getSlug(lastName, { lang: 'sk' }) + '/';

    var fn = getSlug(firstName, { lang: 'sk' });
    if (fn !== '') {
        id += fn + '/';
    }

    id += legacyId;

    return id;
}

function getRecordId(clientId, patientName, recordDate) {
    return clientId + '/' + getSlug(patientName, { lang: 'sk' }) + '/' + recordDate;
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
            migrateRecords();
        }
    });
}

function migrateClients() {
    console.log('$ Migrating clients');
    var pqClient = getPqClient();

    var i = 1;

    var clients = [];
    var prevClientId = 'ShowMeYourGenitals';
    var clientRec = null;
    var query = pqClient.query('SELECT c.id as legacy_id, c.first_name, c.last_name, t.name as title, ' +
        'c.phone_1, c.phone_2, c.email, c.note, c.ic, c.dic, c.icdph, ' +
        'ci.city, ci.psc as city_zip, s.street, s.psc as street_psc, c.house_no, ' +
        'p.name as pname, p.birth_date, p.note as pnote, p.is_dead, p.lysset, ' +
        'sp.name as species, b.name as breed, sx.name as sex ' +
        'FROM client c ' +
        'JOIN patient p on p.client_id = c.id ' +
        'LEFT JOIN lov_title t on t.id = c.title_id ' +
        'LEFT JOIN lov_city ci on ci.id = c.city_id ' +
        'LEFT JOIN lov_street s on s.id = c.street_id ' +
        'LEFT JOIN lov_species sp on sp.id = p.species_id ' +
        'LEFT JOIN lov_breed b on b.id = p.breed_id ' +
        'LEFT JOIN lov_sex sx on sx.id = p.sex_id ' +
        'ORDER BY c.id, p.id');
    query.on('row', function (row) {
        var timestamp = new Date().toJSON();
        
        // ZIPcode logic
        var zip = row.city_zip;
        if (row.street_psc != null) {
            zip = row.street_psc;
        }

        // is_dead transform        
        var dead = false;
        if (row.is_dead != null && row.is_dead === '1') {
            dead = true;
        }

        var patient = {
            name: row.pname,
            birthDate: row.birth_date,
            note: row.pnote,
            dead: dead,
            lysset: row.lysset,
            species: row.species,
            breed: row.breed,
            sex: row.sex,
            created: timestamp
        };

        
        // FIXME: might be better to use https://github.com/pouchdb/collate/ for ID generation
        // if search by last name is to be implemented using primary index (allDocs() and _id)
        var clientId = getClientId(row.first_name, row.last_name, row.legacy_id);
        
        // process client
        if (prevClientId !== clientId) {
            // put old one to list
            if (clientRec) {
                clients.push(clientRec);
            }

            clientRec = {
                _id: clientId,
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
                created: timestamp,
                type: 'owner',
                pets: [patient]
            };

            // dump to pouch!
            if (i++ % 100 == 0) {
                console.log(' -> pushing ' + clients.length + ' clients to pouch');
                dumpToPouch(clients);
                clients = [];
            }
        } else {
            clientRec.pets.push(patient);
        }

        prevClientId = clientRec._id;
    });
    query.on('end', function (result) {
        // last one missing!
        clients.push(clientRec);

        if (clients.length != 0) {
            console.log(' -> pushing remaining ' + clients.length + ' clients to pouch');
            dumpToPouch(clients);
        }
        console.log(' -> patients processed: ' + result.rowCount);
        console.log(' -> clients processed: ' + (i - 1));
    });
}

function migrateRecords() {
    console.log('$ Migrating records');
    var pqClient = getPqClient();

    var i = 1;

    var records = [];
    var prevRecordId = 'ShowMeYourGenitals';
    var record = null;

    var query = pqClient.query('SELECT c.id as legacy_id, c.first_name, c.last_name,  p.name as pname, r.rec_date, ' +
        'i.item_price ' +
        'FROM client c ' +
        'JOIN patient p on p.client_id = c.id ' +
        'JOIN record r on r.patient_id = p.id ' +
        'LEFT JOIN record_item i on i.record_id = r.id ' +
        'ORDER BY c.id, p.id, r.id, i.id');
    query.on('row', function (row) {
        var item = null
        if (row.item_price != null) {
            item = {
                itemPrice: row.item_price
            };
        }

        var date = row.rec_date.toJSON();

        var recordId = getRecordId(getClientId(row.first_name, row.last_name, row.legacy_id), row.pname, date);
        if (prevRecordId !== recordId) {
            // put old one to list
            if (record) {
                records.push(record);
            }

            record = {
                _id: recordId,
                date: date,
                type: 'record'
            };

            if (item) {
                record.items = [item];
            } else {
                record.items = [];
            }
            
            // dump to pouch!
            if (i++ % 1000 == 0) {
                console.log(' -> pushing ' + records.length + ' records to pouch');
                dumpToPouch(records);
                records = [];
            }
        } else {
            record.items.push(item);
        }

        prevRecordId = recordId;
    });
    query.on('end', function (result) {
        // last one missing!
        records.push(record);

        if (records.length != 0) {
            console.log(' -> pushing remaining ' + records.length + ' records to pouch');
            dumpToPouch(records);
        }
        console.log(' -> record items processed: ' + result.rowCount);
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
