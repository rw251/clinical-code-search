(function () {
    'use strict';

    var local = {};

    var showPage = function(id){
        //Hide all pages
        $('.page').hide();

        //Show page
        $('#'+id).show();

        if(id === "new"){
            $('#searchText').focus();
        }
    };

    var initialize = function(){
        $('a.page-link').on('click', function(){
            showPage(this.href.split('#')[1]);

            return false;
        });

        $('#searchText').on('keyup', function(key){
            if(key.which === 13) $('#searchButton').click();
        });

        $('#searchButton').on('click', function(){
           search($('#searchText').val());
        });

        showPage('start');
    };

    var search = function(searchString){
        if($('#tree').children().length > 0) {
            $('#tree').fancytree('destroy');
        }
        $('#tree').fancytree({
            //extensions: ["table"],
            icons: false,
            source: {
                url: '/data/tree/search/' + searchString,
                cache: false
            },
            /*table: {
                indentation: 20,
                nodeColumnIdx: 2,
                checkboxColumnIdx: 0
            },*/
            checkbox: true,
            selectMode: 3,
            lazyLoad: function(event, data){
                data.result = {
                    url: '/data/tree/children/' + data.node.key,
                    data: {selected: data.node.selected}
                };
            },
            beforeActivate: function(event, data){
                data.node.setSelected(!data.node.selected); //Select node with click
                return false; //prevent highlighting of rows
            },
            //Attempt to allow single click on row to select AND double click to still expand
            /*beforeActivate: function(event, data){
                console.log("beforeActivate:",event,data);

                if(data.node.children || data.node.lazy) {
                    setTimeout(function () {
                        if (local.prev) {
                            local.prev = false;
                            return false;
                        }
                        if (!local.dbl) data.node.setSelected(!data.node.selected);
                        else local.prev = true;
                        local.dbl = false;
                    }, 150);
                } else {
                    data.node.setSelected(!data.node.selected);
                }
                return false;
            },
            dblclick: function(event, data){
                local.dbl=true;
            },*/
            /*select: function(event, data) {
                var node = data.node;

                if (node.isSelected()) {
                    if (node.isUndefined()) {
                        // Load and select all child nodes
                        node.load().done(function() {
                            node.visit(function(childNode) {
                                childNode.setSelected(true);
                            });
                        });
                    } else {
                        // Select all child nodes
                        node.visit(function(childNode) {
                            childNode.setSelected(true);
                        });
                    }
                }
            },*/
            renderNode: function(event, data) {
                // Optionally tweak data.node.span
                var node = data.node;
                //if(node.data.cstrender){
                    var $span = $(node.span);
                    $span.find("> span.fancytree-title").html("<span class='fixed-width-span'>" + node.key + "</span>" + node.title);
                    /*$span.find("> span.fancytree-icon").css({
//                      border: "1px solid green",
                        backgroundImage: "url(skin-custom/customDoc2.gif)",
                        backgroundPosition: "0 0"
                    });*/
                //}
            }
            /*
            renderColumns: function(event, data) {
                var node = data.node,
                    $tdList = $(node.tr).find(">td");
                // (index #0 is rendered by fancytree by adding the checkbox)
                $tdList.eq(1).text(node.key);
                // (index #2 is rendered by fancytree)
                $tdList.eq(3).html("<input type='checkbox' name='like' value='" + node.key + "'>");
            }*/
        });
    };

    window.codelist = {
        init : initialize
    };
})();

/******************************************
 *** This happens when the page is ready ***
 ******************************************/
$(document).on('ready', function () {
    codelist.init();
});