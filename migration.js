var pg = require('pg');
var conString = "postgres://postgres:postgres@localhost/tara";

var client = new pg.Client(conString);
client.on('drain', client.end.bind(client)); //disconnect client when all queries are finished
client.connect();

var query = client.query('SELECT id, name FROM lov_sex order by id LIMIT 2 OFFSET 1');
query.on('row', function (row) {
    console.log('id "%d" is %s', row.id, row.name);
});

/*
possible couch _id
select c.first_name || '_' || c.last_name  as name, count(*)
from client c
group by name having count(*) >1 order by count(*) desc;
*/