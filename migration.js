var getSlug = require('speakingurl');

var docUri = require('docuri');
var clientIdGenerator = docUri.route('record/:lastName/:firstName/0');

var pg = require('pg');
var conString = "postgres://postgres:postgres@localhost/tara";

var client = new pg.Client(conString);
client.on('drain', client.end.bind(client)); //disconnect client when all queries are finished
client.connect();

// delete 'empty' clients
var err = false;
console.log('$ Deleting empty clients with no patient records');
var query = client.query('DELETE FROM client WHERE id IN ' +
    '(SELECT c.id FROM client c LEFT JOIN patient p ON c.id = p.client_id ' +
    'WHERE p.id IS NULL)');
query.on('error', function (error) {
    err = true;
    console.log(' -> Error deleting empty client records: ' + error)
});
query.on('end', function (result) {
    console.log(' -> ' + result.rowCount + ' rows were deleted');
});

// TODO trim empty characters off first and last name 

// migrate clients
if (!err) {
    query = client.query('SELECT c.id as legacy_id, c.first_name, c.last_name, t.name as title, ' +
        'c.phone_1, c.phone_2, c.email, c.note, c.ic, c.dic, c.icdph, ' +
        'ci.city, ci.psc as city_zip, s.street, s.psc as street_psc, c.house_no ' +
        'FROM client c ' +
        'LEFT JOIN lov_title t on t.id = c.title_id ' +
        'LEFT JOIN lov_city ci on ci.id = c.city_id ' +
        'LEFT JOIN lov_street s on s.id = c.street_id '
        + ' where last_name like \'% %\''); // TODO: REMOVE CONDITION!!
    query.on('row', function (row) {

        var zip = row.city_zip;
        if (row.street_psc != null) {
            zip = row.street_psc;
        }

        var clientRec = {
            legacy_id: row.legacy_id,
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
            created: new Date().toJSON()
        };
        // FIXME: might be better to use https://github.com/pouchdb/collate/ for ID generation
        // if search by last name is to be implemented using primary index (allDocs() and _id)
        // DocUri only
        //clientRec._id = clientIdGenerator(clientRec);
        // speakingurl first:
        clientRec._id = clientIdGenerator({
            firstName: getSlug(clientRec.firstName),
            lastName: getSlug(clientRec.lastName)
        });

        console.log(clientRec);
    });
}

