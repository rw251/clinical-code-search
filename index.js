var db = require('./app/db');

//Initialise database from data file
//db.init();

//code, preferred (longest string in first row), full (all text joined)

db.getParentCodes("diabetes mellitus", function(codes){
    db.getRubrics(codes, function(rows){
       console.log(rows);
        db.getChildren(rows[0].code, function(rows){
            console.log(rows);
        });
    });
});

//db.searchByText("bmi");