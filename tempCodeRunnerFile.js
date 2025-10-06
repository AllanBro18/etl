let con = mysql.createConnection({
  host: process.env.DB_HOST,
  user: process.env.DB_USERNAM,
    password: process.env.DB_PASSWORD,
  database: "sakila"
});

con.connect(function(err) {
  if (err) throw err;
    console.log("Connected!");
    con.query("SELECT * FROM actor", function(err, res) {
        if (err) throw err;
        console.log(res);
    })
});