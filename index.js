

let mysql = require('mysql2');


let con = mysql.createConnection({
  host: SOURCE_DB_HOST,
  user: SOURCE_DB_USER,
    password: SOURCE_DB_PASSWORD,
  database: SOURCE_DB_NAME
});

con.connect(function(err) {
  if (err) throw err;
    console.log("Connected!");
    con.query("SELECT * FROM actor", function(err, res) {
        if (err) throw err;
        console.log(res);
    })
});