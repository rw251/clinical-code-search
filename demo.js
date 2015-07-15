var express = require('express');
var app = express();
var db = require('./app/db');

module.exports = app;

app.use(express.static(__dirname + '/web'));

app.get('/data/search/:search', function (req, res){
    db.getParentCodes(req.params.search, function(codes){
        db.getRubrics(codes, function(rows) {
            res.json(rows);
        });
    });
});

app.get('/data/tree/search/:search', function (req, res){
    db.getParentCodes(req.params.search, function(codes){
        db.getRubrics(codes, function(rows) {
            var rtn = rows.map(function(val){
                val.title = val.rubric;
                val.key = val.code;
                if(val.children>0)  val.lazy = true;
                return val;
            });
            res.json(rtn);
        });
    });
});

app.get('/data/children/:code', function (req, res){
    db.getChildren(req.params.code, function(rows){
        res.json(rows);
    });
});

app.get('/data/tree/children/:code', function (req, res){
    db.getChildren(req.params.code, function(rows){
        var rtn = rows.map(function(val){
            val.title = val.rubric;
            val.key = val.code;
            if(val.children>0)  val.lazy = true;
            if(req.query.selected) val.selected = req.query.selected === "true";
            return val;
        });
        res.json(rtn);
    });
});

var server = app.listen(3000, function () {
    var host = server.address().address;
    var port = server.address().port;

    console.log('Example app listening at http://%s:%s', host, port);
});