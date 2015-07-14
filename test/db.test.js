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