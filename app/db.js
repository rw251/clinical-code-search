var sqlite3 = require('sqlite3').verbose();
var fs = require("fs");
var liner = require('./liner');

var dataFile = "./data/Keyv2.all";
var dbFile = "./db/read.sqlite";

var DB = {};

var createTables = function(db){

    db.run("DROP TABLE IF EXISTS PreferredTerm");
    db.run("CREATE TABLE PreferredTerm (code TEXT, rubric TEXT, children INTEGER)");

    db.run("DROP TABLE IF EXISTS temp");
    db.run("CREATE TABLE temp (code TEXT, rubric TEXT)");

    db.run("DROP TABLE IF EXISTS Synonym");
    db.run("CREATE TABLE Synonym (code TEXT, rubric TEXT)");
/*
    db.run("DROP TABLE IF EXISTS ref_codes");
    db.run("CREATE TABLE ref_codes (id INTEGER PRIMARY KEY, code TEXT)");

    db.run("DROP TABLE IF EXISTS ref_rubrics");
    db.run("CREATE TABLE ref_rubrics (id INTEGER PRIMARY KEY, rubric TEXT)");

    db.run("DROP TABLE IF EXISTS code_rubric_link");
    db.run("CREATE TABLE code_rubric_link (id INTEGER PRIMARY KEY, codeId INTEGER, rubricId INTEGER)");*/

    db.run("DROP TABLE IF EXISTS RubricSearch");
    db.run("CREATE VIRTUAL TABLE RubricSearch USING fts4(code, full)");

    return db;
};

var tidyUp = function(db) {
    db.run("DROP TABLE IF EXISTS temp");
};

var addIndices = function() {

};

var insertData = function(db, done){
    //BEGIN TRANSACTION must occur before prepared statements
    db.run("BEGIN TRANSACTION");

    var stmtPref = db.prepare("INSERT INTO temp (code, rubric) VALUES (?, ?)");
    var stmtSynonym = db.prepare("INSERT INTO Synonym (code, rubric) VALUES (?, ?)");
    var stmtSearch = db.prepare("INSERT INTO RubricSearch (code, full) VALUES (?, ?)");

    var i = 1;

    var source = fs.createReadStream(dataFile);

    source.pipe(liner);
    console.log("Loading data:");
    var buffer = [], preferred="", lastRC = "-1";

    var processBuffer = function(code, preferred){

        //Sort in order of decreasing length
        buffer = buffer.sort(function(a,b){
            return b.length - a.length;
        });

        //keep all previous in string
        var sofar = "";

        for(var j=0; j< buffer.length; j++){
            //only add if not already wholly contained
            if(sofar.indexOf(buffer[j].toLowerCase())===-1) {
                sofar += " " + buffer[j].toLowerCase();
                stmtSynonym.run(code, buffer[j]);
            }
        }
        if(buffer.length>0) {
            stmtPref.run(code, preferred);
            stmtSearch.run(code, sofar);
        }
        buffer = [];
    };

    liner.on('readable', function() {
        var line, arr, synonym, sm, md="", lg="", rc, push=function(vals){vals.forEach(function(val){if(val!=="")buffer.push(val);})};
        if(i%5000===0) process.stdout.write("=");
        i++;
        while(line = liner.read()){
            arr = line.substr(1).split('","');
            rc = arr[7];
            synonym = arr[0];

            sm = arr[2]; md = arr[3]; lg = arr[4];

            if(rc !== lastRC) {
                processBuffer(lastRC, preferred);
                lastRC = rc;
                if(lg !== "") preferred = lg;
                else if(md !== "") preferred = md;
                else preferred = sm;

                if(sm === "" || rc === "") {
                    console.log("ERROR: " + line);
                } else {
                    push([sm, synonym, md, lg]);
                }
            } else {
                push([sm, synonym, md, lg]);
            }
        }
    });

    liner.on('end', function(){
        processBuffer(lastRC, preferred);
        stmtPref.finalize();
        stmtSynonym.finalize();
        stmtSearch.finalize();
        db.run("END TRANSACTION", function(){
            if(done && typeof done === "function") done();
        });
        console.log("DONE");
    });

    liner.on('error', function(err){
        console.log('An error occurred:');
        console.log(err);
    });

    return db;
};

var populateRefTables = function(db) {
    db.parallelize(function(){
        console.time("Populated code ref table");
        db.run("INSERT INTO ref_codes (code) SELECT code FROM initial GROUP BY code", function(a,b,c,d){
            console.timeEnd("Populated code ref table");
            console.log("Unique codes:" + this.lastID);
        });

        console.time("Populated rubric ref table");
        db.run("INSERT INTO ref_rubrics (rubric) SELECT rubric FROM initial GROUP BY rubric", function(){
            console.timeEnd("Populated rubric ref table");
            console.log("Unique rubrics:" + this.lastID);
        });
    });

    return db;
};

var populateLinkTable = function(db, done){
    db.run("INSERT INTO code_rubric_link (codeId, rubricId) SELECT ref_codes.id, ref_rubrics.id FROM initial INNER JOIN ref_codes on ref_codes.code = initial.code INNER JOIN ref_rubrics on ref_rubrics.rubric = initial.rubric", function(err){
        if(err) console.log(err);
        console.log("Link table rows:" + this.lastID);

        if(done && typeof done === "function") done();
    });
};

var createFullTextSearch = function(db, done){
    db.run("INSERT INTO RubricSearch SELECT code, rubric, full FROM initial", function(err){
        if(err) console.log(err);
        console.log("createFullTextSearch:" + this.lastID);

        if(done && typeof done === "function") done();
    });
};

var getQueryForFixedLength = function(i){
    var q = "select t.code, t.rubric, sub.cnt from temp t inner join (select substr(code,1," + i + ") || '" + Array(6-i).join('.') + "' as code, count(*)-1 as cnt from temp group by substr(code,1," + i + ")) sub on sub.code = t.code";
    return q;
};

var updateChildCount = function(db, done){
    var query = "INSERT INTO PreferredTerm SELECT code, rubric, max(cnt) FROM ( ";
    var qArr = [];
    for(var i = 1; i < 6; i++ ){
     qArr.push(getQueryForFixedLength(i));
    }
    query += qArr.join(' UNION ');
    query += " ) GROUP BY code, rubric";
    db.run(query, function(err){
        console.log(query);
        if(err) console.log(err);
        if(done && typeof done === "function") done();
    });
};

var init = function(){
    var db = new sqlite3.Database(dbFile);
    //var db = new sqlite3.Database(':memory:'); //doesn't increase speed for this volume of data

    db.run("PRAGMA synchronous = OFF");
    //db.run("PRAGMA journal_mode = MEMORY"); //doesn't increase speed for this volume of data
    db.serialize(function() {

        createTables(db);

        console.time("Inserted");
        insertData(db, function(){
            console.timeEnd("Inserted");
        });


        updateChildCount(db);


        tidyUp(db, function(){
            db.close();
        });
        /*populateRefTables(db);

        populateLinkTable(db);

        createFullTextSearch(db, function(){
            db.close();
        })*/
    });
};

var sortCodeList = function(codeList){
    //Sort
    var i, last, rtn=[];
    if(!codeList || codeList.length === 0) return codeList;

    codeList.sort();

    rtn.push(codeList[0]);
    last = codeList[0].replace(/\.*$/g,''); //Trim trailing '.'s
    for(i=1; i<codeList.length;i++){
        if(codeList[i].indexOf(last)!==0){
            rtn.push(codeList[i]);
            last = codeList[i].replace(/\.*$/g,''); //Trim trailing '.'s
        }
    }

    return rtn;
};

var searchByText = function(searchString) {
    var db = new sqlite3.Database(dbFile);

    /*
        Matching a phrase MATCH '"match th* phr*"'
        Matching proximate words NEAR 'near me'
        Matching boolean MATCH 'thi* and that' Hmm AND not working - something to do with the extended query syntax:
         http://stackoverflow.com/a/14666515/596639
     */
    var query = "SELECT code, full FROM RubricSearch WHERE full MATCH '" + searchString.replace("'","''").split(' ').join('* ') + "*'";
    db.each(query, function(err, row){
        console.log(query);
        if(err){
            console.log(err);
        }
       console.log(row);
    });
    db.close();
};

var getParentCodes = function(searchString, done) {
    var db = new sqlite3.Database(dbFile);

    var query = "SELECT code FROM RubricSearch WHERE full MATCH '" + searchString.replace("'","''").split(' ').join('* ') + "*'";
    var codes = [];

    db.each(query, function (err, row) {
        if (err) {
            console.log(err);
        }
        codes.push(row.code);
    }, function(err){
        console.log(query);
        if(err) console.log(err);
        if(done && typeof done === "function") done(sortCodeList(codes));
    });

    db.close();
};

var getRubrics = function(codes, done){
    var db = new sqlite3.Database(dbFile);

    var query = "SELECT code, rubric, children FROM PreferredTerm WHERE code in ('" + codes.join("','") + "')";

    db.all(query, function (err, rows) {
        console.log(query);
        if (err) {
            console.log(err);
        }
        if(done && typeof done === "function") done(rows);
    });

    db.close();
};

var getChildren = function(code, done){
    var db = new sqlite3.Database(dbFile);

    if(code.indexOf('.')===-1) {
        if(done && typeof done === "function") done([]);
        return;
    }

    var qcode = code.replace('.','_');
    var query = "SELECT code, rubric, children FROM PreferredTerm WHERE code like '" + qcode + "' and code != '" + code + "'";

    db.all(query, function (err, rows) {
        console.log(query);
        if (err) {
            console.log(err);
        }
        if(done && typeof done === "function") done(rows);
    });

    db.close();
};

/* Private functions for testing */
DB._sortCodeList = sortCodeList;
/*  */

/* Public functions*/
DB.init = init;
DB.searchByText = searchByText;
DB.getParentCodes = getParentCodes;
DB.getRubrics = getRubrics;
DB.getChildren = getChildren;

module.exports = DB;