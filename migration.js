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

function getProductId(productName, unitName) {
    return 'product/' + getSlug(productName, { lang: 'sk' }) +
        '/' + getSlug(unitName, { lang: 'sk' });
}

function getAddressId(city, street, zip) {
    var id = 'address/' + getSlug(city, { lang: 'sk' })

    if (street) {
        id += '/' + getSlug(street, { lang: 'sk' });
    }

    id += '/' + getSlug(zip, { lang: 'sk' });

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
            migrateRecords();
            migrateProducts();
            migrateAddresses();
            migrateTitles();
            migrateSex();
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
        'JOIN patient p ON p.client_id = c.id ' +
        'LEFT JOIN lov_title t ON t.id = c.title_id ' +
        'LEFT JOIN lov_city ci ON ci.id = c.city_id ' +
        'LEFT JOIN lov_street s ON s.id = c.street_id ' +
        'LEFT JOIN lov_species sp ON sp.id = p.species_id ' +
        'LEFT JOIN lov_breed b ON b.id = p.breed_id ' +
        'LEFT JOIN lov_sex sx ON sx.id = p.sex_id ' +
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
            tags: [],
            species: row.species,
            breed: row.breed,
            sex: row.sex,
            created: timestamp
        };

        if (row.lysset != null) {
            patient.tags.push(row.lysset);
        }

        
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
                type: 'client',
                patients: [patient]
            };

            // dump to pouch!
            if (i++ % 100 == 0) {
                //console.log(' -> pushing ' + clients.length + ' clients to pouch');
                dumpToPouch(clients, 'client');
                clients = [];
            }
        } else {
            clientRec.patients.push(patient);
        }

        prevClientId = clientRec._id;
    });
    query.on('end', function (result) {
        // last one missing!
        clients.push(clientRec);

        if (clients.length != 0) {
            //console.log(' -> pushing remaining ' + clients.length + ' clients to pouch');
            dumpToPouch(clients, 'client');
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
        'r.data, i.amount, i.prod_price, i.item_price, i.item_type, ' +
        'pr.name as prod_name, pr.plu, u.name as unit_name ' +
        'FROM client c ' +
        'JOIN patient p ON p.client_id = c.id ' +
        'JOIN record r ON r.patient_id = p.id ' +
        'LEFT JOIN record_item i ON i.record_id = r.id ' +
        'LEFT JOIN lov_product pr ON pr.id = i.prod_id ' +
        'LEFT JOIN lov_unit u ON u.id = pr.unit_id ' +
        'ORDER BY c.id, p.id, r.id, i.id');
    query.on('row', function (row) {
        var item = null
        if (row.item_price != null) {
            var type = 'L'; //LABOUR (==0)
            if (row.item_type === 1) {
                type = 'M'; // MATERIAL (==1)
            } 
            
            // TODO consider dividing labour and material items to 2 lists
            
            item = {
                description: row.prod_name,
                unitPrice: row.prod_price,
                quantity: row.amount,
                unit: row.unit_name,
                price: row.item_price,
                itemType: type,
                plu: row.plu
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
                log: row.data,
                type: 'record'
            };

            if (item) {
                record.items = [item];
            } else {
                record.items = [];
            }
            
            // dump to pouch!
            if (i++ % 1000 == 0) {
                //console.log(' -> pushing ' + records.length + ' records to pouch');
                dumpToPouch(records, 'record');
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
            //console.log(' -> pushing remaining ' + records.length + ' records to pouch');
            dumpToPouch(records, 'record');
        }
        console.log(' -> record items processed: ' + result.rowCount);
        console.log(' -> records processed: ' + (i - 1));
    });
}

/*
    remove duplicate addresses from source before running this. e.g.:
    select (c.city || '.' || s.street || '.' ||  s.psc) as agg, count(*) 
        from lov_city c 
        left join lov_street s on s.city_id = c.id 
        group by agg having count(*) > 1;
*/
function migrateAddresses() {
    console.log('$ Migrating addresses');
    var pqClient = getPqClient();

    var i = 0;
    var addresses = [];
    var query = pqClient.query('SELECT c.city, s.street, c.psc as city_zip, s.psc street_zip ' +
        'FROM lov_city c LEFT JOIN lov_street s ON s.city_id = c.id ' +
        'ORDER BY c.id, s.id');
    query.on('row', function (row) {
        var zip = row.city_zip;
        if (row.street) {
            zip = row.street_zip;
        }

        addresses.push({
            _id: getAddressId(row.city, row.street, zip),
            city: row.city,
            street: row.street,
            zipcode: zip,
            type: 'address',
            created: new Date().toJSON()
        });
        
        // dump to pouch!
        if (i++ % 1000 == 0) {
            //console.log(' -> pushing ' + addresses.length + ' addresses to pouch');
            dumpToPouch(addresses, 'address');
            addresses = [];
        }
    });
    query.on('end', function (result) {
        if (addresses.length != 0) {
            //console.log(' -> pushing remaining ' + addresses.length + ' addresses to pouch');
            dumpToPouch(addresses, 'address');
        }
        console.log(' -> addresses processed: ' + result.rowCount);
    });
}

function migrateGenericRecords(sqlQuery, recordType, recordObjFromSqlRow) {
    console.log('$ Migrating ' + recordType);
    var pqClient = getPqClient();

    var records = [];
    var query = pqClient.query(sqlQuery);
    query.on('row', function (row) {
        records.push(recordObjFromSqlRow(row));
    });
    query.on('end', function (result) {
        dumpToPouch(records, recordType);
        console.log(' -> ' + recordType + ' processed: ' + result.rowCount);
    });
}

/*
    remove duplicate products from source before running this. e.g.:
    select (p.name || '-' || u.name) as aggname, count(*) 
        from lov_product p 
        join lov_unit u on u.id = p.unit_id 
        group by aggname having count(*) > 1;
        
    update record_item set prod_id = 512 where prod_id = 318;
    delete from lov_product where id = 318;
    delete from lov_product where id = 932;

*/
function migrateProducts() {
    var sql = 'SELECT pr.name as pname, pr.price, pr.valid_to, pr.plu, u.name as uname ' +
        'FROM lov_product pr ' +
        'LEFT JOIN lov_unit u ON u.id = pr.unit_id ' +
        'ORDER BY pr.id';
    migrateGenericRecords(sql, 'product', function (row) {
        return {
            _id: getProductId(row.pname, row.uname),
            description: row.pname,
            unitPrice: row.price,
            unit: row.uname,
            validTo: row.valid_to,
            plu: row.plu,
            type: 'product',
            created: new Date().toJSON()
        };
    });
}

function migrateTitles() {
    var sql = 'SELECT name FROM lov_title ORDER BY name';
    migrateGenericRecords(sql, 'title', function (row) {
        return {
            _id: 'title/' + getSlug(row.name, { lang: 'sk' }),
            title: row.name,
            type: 'title',
            created: new Date().toJSON()
        };
    });
}

function migrateSex() {
    var sql = 'SELECT name FROM lov_sex ORDER BY name';
    migrateGenericRecords(sql, 'sex', function (row) {
        return {
            _id: 'sex/' + getSlug(row.name, { lang: 'sk' }),
            title: row.name,
            type: 'sex',
            created: new Date().toJSON()
        };
    });
}

function dumpToPouch(docs, docType) {
    var db = new PouchDB(couchString);
    db.bulkDocs(docs).then(function (result) {
        for (var i = 0; i < result.length; i++) {
            var res = result[i];
            if (res.error) {
                console.log(' -> failed to insert ' + docType + ' -> ' + res + ';; id:' + res.id);
            }
        }
    }).catch(function (err) {
        console.log(' -> error pushing records to couch ' + err);
    });
}

// RUN
migration();
