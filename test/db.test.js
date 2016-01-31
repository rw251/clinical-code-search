var assert = require('chai').assert,
    db = require('../app/db.js');

describe('SortCodeList', function(){
    it('should return empty if given empty', function(){
        var input = [];
        assert.equal(input, db._sortCodeList(input));
    });

    it('should return undefined if given undefined', function(){
        assert.equal(undefined, db._sortCodeList());
    });

    it('should return correct result - fake', function(){
        var input = ['K0512', 'K06..','K05..','K053.'];
        var output = db._sortCodeList(input);
        assert.include(output, 'K05..');
        assert.include(output, 'K06..');
        assert.notInclude(output, 'K0512');
        assert.notInclude(output, 'K053.');
        assert.equal(2, output.length);
    });

    it('should return correct result - aki', function(){
        var input = ['7K165','7K165','F13z3','F2503','F4836','K04..','K04C.','K04D.','K04E.'];
        var output = db._sortCodeList(input);
        assert.include(output, '7K165');
        assert.include(output, 'F13z3');
        assert.include(output, 'F2503');
        assert.include(output, 'F4836');
        assert.include(output, 'K04..');
        assert.equal(5, output.length);
    });

    it('should return correct result - bmi', function(){
        var input = ['22K..','22K4.','22K7.','22K90','22K91','22KC.','22KD.','22KE.','22KE.','38DB1','38DB1','38DC1','38DC1'];
        var output = db._sortCodeList(input);
        assert.include(output, '22K..');
        assert.include(output, '38DB1');
        assert.include(output, '38DC1');
        assert.equal(3, output.length);
    });

});

//Following assumes database is setup
describe('DB', function() {
    it('C10EC has 0 direct ancestors', function (donea) {
        db.getChildren('C10EC', function(rows){
            assert.equal(0, rows.length);
            donea();
        });
    });

    it('C10F. has 27 direct ancestors', function (doneb) {
        db.getChildren('C10F.', function(rows){
            assert.equal(27, rows.length);
            doneb();
        });
    });

    it('C10.. has 26 direct ancestors', function (donec) {
        db.getChildren('C10..', function(rows){
            assert.equal(26, rows.length);
            donec();
        });
    });

    it('13D.. does not include 13d* in it\'s children', function (donec) {
        db.getChildren('13D..', function(rows){
            var codes = rows.map(function(val){
                return val.code;
            });
            assert.equal(9, rows.length);
            assert.notInclude(codes, '13d..');
            assert.notInclude(codes, '13dT.');
            assert.notInclude(codes, '13dh.');
            assert.notInclude(codes, '13d1.');
            donec();
        });
    });
});