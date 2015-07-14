var sqlite3 = require('sqlite3').verbose();
var fs = require("fs");
var liner = require('./liner');

var dataFile = "./data/Keyv2.all";
var dbFile = "./db/read.sqlite";

var DB = {};

var createTables = function(){
    DB.db.run("DROP TABLE IF EXISTS initial");
    DB.db.run("CREATE TABLE initial (code TEXT, rubric TEXT)");

    DB.db.run("DROP TABLE IF EXISTS ref_codes");
    DB.db.run("CREATE TABLE ref_codes (id INTEGER PRIMARY KEY, code TEXT)");

    DB.db.run("DROP TABLE IF EXISTS ref_rubrics");
    DB.db.run("CREATE TABLE ref_rubrics (id INTEGER PRIMARY KEY, rubric TEXT)");

    DB.db.run("DROP TABLE IF EXISTS code_rubric_link");
    DB.db.run("CREATE TABLE code_rubric_link (id INTEGER PRIMARY KEY, codeId INTEGER, rubricId INTEGER)");

    DB.db.run("DROP TABLE IF EXISTS RubricSearch");
    DB.db.run("CREATE VIRTUAL TABLE RubricSearch USING fts4(code, rubric)");
};

var insertData = function(done){
    //BEGIN TRANSACTION must occur before prepared statements
    DB.db.run("BEGIN TRANSACTION");

    var stmt = DB.db.prepare("INSERT INTO initial (code, rubric) VALUES (?, ?)");
    var i = 1;

    var source = fs.createReadStream(dataFile);

    source.pipe(liner);
    console.log("Loading data:");
    var buffer = [], lastRC = "-1";

    var processBuffer = function(code){

        //Sort in order of decreasing length
        buffer = buffer.sort(function(a,b){
            return b.length - a.length;
        });

        //keep all previous in string
        var sofar = "";

        //console.log("Loading data:");
        for(var j=0; j< buffer.length; j++){
            //only add if not already wholly contained
            if(sofar.indexOf(buffer[j].toLowerCase())===-1) {
                stmt.run(code, buffer[j]);
                sofar += "|||" + buffer[j].toLowerCase();
            }
        }
        buffer = [];
    };

    liner.on('readable', function() {
        var line, arr, synonym, sm, md="", lg="", rc;
        if(i%5000===0) process.stdout.write("=");
        i++;
        while(line = liner.read()){
            arr = line.substr(1).split('","');
            rc = arr[7];
            synonym = arr[0];

            sm = arr[2];
            md = arr[3];
            lg = arr[4];

            if(rc !== lastRC) {
                processBuffer(lastRC);
                lastRC = rc;

                if(sm === "" || rc === "") {
                    console.log("ERROR: " + line);
                } else {
                    buffer.push(sm);

                    if(synonym !== "") {
                        buffer.push(synonym);
                    }

                    if(md !== "") {
                        buffer.push(md);
                    }

                    if(lg !== "") {
                        buffer.push(lg);
                    }
                }
            } else {
                if(synonym !== "") {
                    buffer.push(synonym);
                }

                if(sm !== "") {
                    buffer.push(sm);
                }

                if(md !== "") {
                    buffer.push(md);
                }

                if(lg !== "") {
                    buffer.push(lg);
                }
            }
        }
    });

    liner.on('end', function(){
        processBuffer();
        stmt.finalize();
        DB.db.run("END TRANSACTION", function(){
            if(done && typeof done === "function") done();
        });
        console.log("DONE");
    });

    liner.on('error', function(err){
        console.log('An error occurred:');
        console.log(err);
    });
};

var populateRefTables = function() {
    DB.db.parallelize(function(){
        console.time("Populated code ref table");
        DB.db.run("INSERT INTO ref_codes (code) SELECT code FROM initial GROUP BY code", function(a,b,c,d){
            console.timeEnd("Populated code ref table");
            console.log("Unique codes:" + this.lastID);
        });

        console.time("Populated rubric ref table");
        DB.db.run("INSERT INTO ref_rubrics (rubric) SELECT rubric FROM initial GROUP BY rubric", function(){
            console.timeEnd("Populated rubric ref table");
            console.log("Unique rubrics:" + this.lastID);
        });
    });
};

var populateLinkTable = function(done){
    DB.db.run("INSERT INTO code_rubric_link (codeId, rubricId) SELECT ref_codes.id, ref_rubrics.id FROM initial INNER JOIN ref_codes on ref_codes.code = initial.code INNER JOIN ref_rubrics on ref_rubrics.rubric = initial.rubric", function(err){
        if(err) console.log(err);
        console.log("Link table rows:" + this.lastID);

        if(done && typeof done === "function") done();
    });
};

var createFullTextSearch = function(done){
    DB.db.run("INSERT INTO RubricSearch SELECT code, rubric FROM initial", function(err){
        if(err) console.log(err);
        console.log("createFullTextSearch:" + this.lastID);

        if(done && typeof done === "function") done();
    });
};

DB.init = function(){
    DB.db = new sqlite3.Database(dbFile);
    //var db = new sqlite3.Database(':memory:'); //doesn't increase speed for this volume of data

    DB.db.run("PRAGMA synchronous = OFF");
    //db.run("PRAGMA journal_mode = MEMORY"); //doesn't increase speed for this volume of data
    DB.db.serialize(function() {

        createTables();

        console.time("Inserted");
        insertData(function(){
            console.timeEnd("Inserted");
        });

        populateRefTables();

        populateLinkTable();

        createFullTextSearch(function(){
            DB.db.close();
        })
    });
};

module.exports = DB;