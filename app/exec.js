var sqlite3 = require('sqlite3').verbose();
var fs = require("fs");
var file = "../db/read.sqlite";
var exists = fs.existsSync(file);

var db = new sqlite3.Database(file);

db.serialize(function() {
	console.log(process.argv[2]);
	db.each(process.argv[2], function(err, row){
		if(err) {
			console.log(err);
			return;
		}
		console.log(row);
	});
});

db.close();