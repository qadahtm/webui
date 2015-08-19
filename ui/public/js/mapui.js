
var clientState = undefined;



if (typeof (Storage) !== "undefined") {
    // Code for localStorage/sessionStorage.

    if (localStorage.testarr) {
        console.log("found test arr");
    }
    else {
        console.log("nothing found");
        var o = {"content": [{"val": 1}, {"val": 2}]};
        localStorage.testarr = $.toJSON(o);
    }

} else {
    // Sorry! No Web Storage support..
}

var uiState = {
    catalog:{
        queries:{
            liststate:[],
            keys:[],
            vals:[],
            poller:null
        },
        datasources:{
            poller:null
        }
    },
    knnList: [],
    rangeList: [],
    getRandomColor: function () {
        return randomColor({luminosity: 'dark'});
    },    
    getRandomColorLight: function () {
        return randomColor({luminosity: 'light'});
    },  
    resolveRangePredVar: function (v) {
        var i = 0;
        while (i < uiState.rangeList.length) {
            if (v == uiState.rangeList[i].name) {
                console.log(uiState.rangeList[i].name);
                return createMBR(uiState.rangeList[i].rect);
            }
            i++;
        }
        return null;
    },
    resolveKNNPredVar: function (v) {
        var i = 0;
        console.log(v);
        while (i < uiState.knnList.length) {
            if (v == uiState.knnList[i].name) {
                // console.log(uiState.knnList[i].marker.getPosition());
//        var p = uiState.knnList[i].marker.getPosition();
                var marker = uiState.knnList[i].marker;

                return createLatLng(marker);
            }
            i++;
        }
        return null;
    }
};

/**
 flags
 **/

var queryvis = true;
var load200 = false;
var load800 = false;
///////////////////

var queries = [];
var inputpoints = [];
var trips = [];
var lines = [];

var eventsources = [];

var nodes = {};
var edges = {};
var nodeSet = [];

/////////////////
var gmap = null;
var pservice = null;

var beforeColor = '#CF5656';
var afterColor = '#9BC150';

var polylines = [];

var drawingContext = {
    rectangles: [],
    rectangleMaxCount: 3
};
var drawingManager = null;

var beforeRectOptions = {
    clickable: true,
    draggable: true,
    editable: true,
    fillColor: beforeColor,
    fillOpacity: 0.5,
    strokeWeight: 5
};

// place holder object for the request
var request = {
    mbr: null
};




var drawingOptions = {
    drawingMode: null, //google.maps.drawing.OverlayType.RECTANGLE,
    drawingControl: true,
    drawingControlOptions: {
        position: google.maps.ControlPosition.TOP_CENTER,
        drawingModes: [
            // google.maps.drawing.OverlayType.CIRCLE,
            // google.maps.drawing.OverlayType.POLYGON,
            google.maps.drawing.OverlayType.MARKER,
            google.maps.drawing.OverlayType.RECTANGLE
        ]
    },
    markerOptions: {draggable: true},
    rectangleOptions: beforeRectOptions

};


var es; // event stream handle

function syslogAddStatus(smsg){
    if (smsg.status === "success"){
        $("#syslog").append(createPanelElement("panel-success",smsg.msg));
    }
    else if (smsg.status === "error"){
        $("#syslog").append(createPanelElement("panel-danger",smsg.msg));
    }
}

function createPanelElement(panelClass, bodyContent){
    return $("<div class='panel "+panelClass+"'></div>").append("<div class='panel-body'>"+bodyContent+"</div>");
}


function initialize() {

    //state listeners
    // uiState.catalog.queryStateListener = setInterval(function(){
    //     refreshRegiesteredQueryList();
    // },500);

    refreshRegiesteredQueryList();

    $.ajax("tornado/config").done(function(conf){
        console.log(conf);
        if (conf.kafka.enabled){
            uiState.es = new EventSource("kafka/output-stream");
        }
        else {
            uiState.es = new EventSource("/mock-output-stream");
        }

        uiState.es.onmessage = function(e) {
        //console.log("got an sse");
        var sse = $.parseJSON(e.data);
        var ssetype = sse.type;
        if (conf.kafka.enabled && (typeof sse.data) === "string"){
            console.log(sse);
            console.log(sse.data);    
            sse = eval("(" + sse.data + ")");
        }
        // console.log(e);
        // console.log(sse);
        // console.log($.toJSON(sse)); 
        
        if (ssetype == "output") {

            var color = uiState.getRandomColor();

            if (sse.outputColor) {
                color = sse.outputColor;
            }

            // lookup query current state
            var qi;

            if (sse.name === undefined){
                qi = uiState.catalog.queries.keys.indexOf(sse.qname);
            }
            else {
                qi = uiState.catalog.queries.keys.indexOf(sse.name);
            }


            // console.log(qi);

            if (qi !== -1){
                var qo = uiState.catalog.queries.vals[qi];


                var iconUrl = createColoredMarker(color);
                // console.log(iconUrl);
                // create map element
                var myLatlng1 = new google.maps.LatLng(sse.point.lat,sse.point.lng);
                var marker1 = new google.maps.Marker({
                      position: myLatlng1,
                      icon: iconUrl,
                      title:sse.text
                  });
                var infowindow1 = new google.maps.InfoWindow({
                      content: "<p>"+sse.text+"</p>"
                  });
                google.maps.event.addListener(marker1,'click',function(){
                    infowindow1.open(gmap,marker1);                
                 });

                // create list element

                // hide empty message
                var outputlisting = $("#outputlisting");

                var iconElem = $("<img src='"+iconUrl+"' />");
                var iconCol = $("<span class='col-md-2'></span>").append(iconElem);
                var textCol = $("<p class='col-md-8' style='overflow-x:scroll;'>"+sse.text+"</p>");
                var tupleentry = $("<div class='row'></div>").append(iconCol).append(textCol);
                var listitem = $("<li class='list-group-item' ></li>").append(tupleentry);
                // listitem.data("query", sse.name);

                if (qo.checked === "check"){
                    marker1.setMap(gmap);                    
                }
                else{
                    listitem.toggleClass("hidden");
                }
                
                outputlisting.append( listitem );

                if (!qo.output){
                    qo.output = [];
                }
                var outputEntry = {};
                outputEntry.mapElement = marker1;
                outputEntry.listElement = listitem;

            
                qo.output.push(outputEntry);

                refreshOutputListing();

            }



        }
    }
    });
    

    

    

    var mapOptions = {
        // US 
        // 39.730255, -98.018183
        //24.507052, 45.371521

        // berlin
        // 52.520190250694526&west=13.405380249023438&south=52.51914570999911&east=13.407440185546875 
        // makkah : 22.473878, 40.121263
        center: new google.maps.LatLng(39.730255, -98.018183),
        zoom: 5
    };
    var map = new google.maps.Map(document.getElementById("map-canvas"),
            mapOptions);
    gmap = map;
    //pservice = new google.maps.places.PlacesService(gmap);
    drawingOptions.rectangleOptions = beforeRectOptions;

    drawingManager = new google.maps.drawing.DrawingManager(drawingOptions);
    drawingManager.addListener("markercomplete", function (m) {
        if ((uiState.knnList.length) == drawingContext.rectangleMaxCount) { //TODO change limit
            m.setMap(null);
        }
        else {
            var qid = uiState.knnList.length;
            var _color = uiState.getRandomColorLight();
            var _iconUrl = createColoredMarker(_color);
            uiState.knnList.push({show: true, name: "f" + qid, marker: m, color: _color, iconUrl: _iconUrl});

            var nopt = {
                      icon: _iconUrl
                  };

            //console.log(m);
            m.setOptions(nopt);

            m.addListener("position_changed", function () {
                updateKNNPred(qid, this, {});
            });
            updateKNNList();
        }
    });
    drawingManager.addListener("rectanglecomplete", function (e) {
        // console.log("a rectangle is completed");
        // console.log(e.getBounds());
        // console.log("current count = "+drawingContext.rectangles.length);
        
        if ((uiState.rangeList.length) == drawingContext.rectangleMaxCount) {
            // console.log("max count has reached which is "+drawingContext.rectangleMaxCount);
            e.setMap(null);
        }
        else {
            var qid = uiState.rangeList.length;

            var _color = uiState.getRandomColorLight();

            var rnopt = {
               clickable: false,
               draggable: false,
               editable: false,
                fillColor: _color,
                fillOpacity: 0.5,
                strokeWeight: 1
            };

            e.setOptions(rnopt);

            uiState.rangeList.push({show: true, name: "r" + qid, rect: e, color: _color});

            e.addListener("bounds_changed", function () {
                updateMBR(qid, this);
            });
            updateRangeList();
        }

    });

    drawingManager.setMap(map);


    // code mirror
    // console.log($("#textEntry").get(0));
    // console.log(document.body);
    var mime = 'text/x-mariadb';
    // get mime type
    if (window.location.href.indexOf('mime=') > -1) {
    mime = window.location.href.substr(window.location.href.indexOf('mime=') + 5);
    }
    uiState.editor = CodeMirror.fromTextArea($("#textData").get(0), {
    mode: mime,
    indentWithTabs: true,
    smartIndent: true,
    lineNumbers: true,
    matchBrackets : true,
    autofocus: true,
    extraKeys: {"Ctrl-Space": "autocomplete"},
    hintOptions: {tables: {
      users: {name: null, score: null, birthDate: null},
      countries: {name: null, population: null, size: null}
    }}});


    populateDataSources();
}

// initialize data source list
function populateDataSources(){
    

    $.getJSON( "tornado/datasources", function( data ) {
      var target = $("#regDatasources .list-group");
      target.empty();

      $.each( data, function( key, val ) {
        //console.log(key);
        //console.log(val);
        target.append( "<li id='" + key + "' class='list-group-item' >" + val.sourceName + "</li>" );
      });     
    });
}


function buildErrorMessage(e) {
    return e.line !== undefined && e.column !== undefined
      ? "Line " + e.line + ", column " + e.column + ": " + e.message
      : e.message;
}



function refreshOutputListing(){
    var outputlisting = $("#outputlisting");
    var t = outputlisting.find("li.empty:not(.hidden)");
    // console.log(t);
    if (t.length  > 0) {
        // list is empty but contains an info bullet, so make it empty
        // outputlisting.empty();  
        
        t.addClass("hidden");
        
    }
    if (outputlisting.find("li.list-group-item.hidden:not(.empty)").length == outputlisting.find("li.list-group-item:not(.empty)").length){
        // show empty 
        outputlisting.find("li.empty.hidden").removeClass("hidden");
    }
}


function createPredicateListEntry(pobj) {
    var cqobj = pobj;
    
    var colorIcon = $('<span class="glyphicon glyphicon-stop" style="float:left;margin-right:0.6em;color:'+cqobj.color+'"></span>');
    var removeIcon = $('<span class="glyphicon glyphicon-trash" style="float:left;margin-right:0.6em;color:black"></span>');
    // removeIcon.click(function(){
    //     $.ajax("tornado/queries",{
    //         method: "DELETE",
    //         data: $.toJSON(djo)
    //     }).done(function(resp){
    //         // console.log(resp);
    //         syslogAddStatus(resp);
    //         var dqi = uiState.catalog.queries.keys.indexOf(djo.name);
    //         var dqo = uiState.catalog.queries.vals[dqi];
    //         for (var i = dqo.output.length - 1; i >= 0; i--) {
    //             dqo.output[i].mapElement.setMap(null);
    //             // console.log(dqo.output[i].listElement);
    //             dqo.output[i].listElement.remove();
    //         };
    //         uiState.catalog.queries.keys.splice(dqi,1);
    //         uiState.catalog.queries.vals.splice(dqi,1);
    //         refreshRegiesteredQueryList();
    //         refreshOutputListing();
    //     });
    // });

    var pname = $('<p>'+pobj.name+'</p>');

    return $('<li class="list-group-item"></li>').append(colorIcon).append(removeIcon).append(pname);
}



function createQueryListEntry(qobj){
    var cqobj = qobj;
    if (typeof qobj === "number"){
        cqobj = uiState.catalog.queries.vals[qobj] 
    }

    // console.log(cqobj);
    var djo = {};


    if (cqobj.qname){
        djo.name = cqobj.qname;    
    }
    else if (cqobj.name) {
        djo.name = cqobj.name;
    }
    
    // add tag
    djo.tag = "-";
    var colorIcon = $('<span class="glyphicon glyphicon-stop" style="float:left;margin-right:0.6em;color:'+cqobj.outputColor+'"></span>');
    var removeIcon = $('<span class="glyphicon glyphicon-trash" style="float:left;margin-right:0.6em;color:black"></span>');
    removeIcon.click(function(){
        $.ajax("tornado/queries",{
            method: "DELETE",
            data: $.toJSON(djo)
        }).done(function(resp){
            // console.log(resp);
            syslogAddStatus(resp);
            var dqi = uiState.catalog.queries.keys.indexOf(djo.name);
            var dqo = uiState.catalog.queries.vals[dqi];
            for (var i = dqo.output.length - 1; i >= 0; i--) {
                dqo.output[i].mapElement.setMap(null);
                // console.log(dqo.output[i].listElement);
                dqo.output[i].listElement.remove();
            };
            uiState.catalog.queries.keys.splice(dqi,1);
            uiState.catalog.queries.vals.splice(dqi,1);
            refreshRegiesteredQueryList();
            refreshOutputListing();
        });
    });
    var qname = $('<p>'+djo.name+'</p>');

    var displayIcon = $('<span class="glyphicon glyphicon-'+cqobj.checked+'" style="float:left;margin-right:0.6em;color:black"></span>');
    displayIcon.click(function(){
        displayIcon.toggleClass("glyphicon-check glyphicon-unchecked"); 
        //console.log(displayIcon.is(".glyphicon-check"));
        if (displayIcon.is(".glyphicon-unchecked")){
            // update state
            cqobj.checked = "unchecked";
            // console.log("remove all output for "+djo.name+" from display");
            // var qi = uiState.catalog.queries.keys.indexOf(djo.name);  
            // var qo = uiState.catalog.queries.vals[qi];
            for (var i = cqobj.output.length - 1; i >= 0; i--) {
              cqobj.output[i].mapElement.setMap(null);
              $(cqobj.output[i].listElement).toggleClass("hidden");
            };         

        }
        else if (displayIcon.is(".glyphicon-check")){
            cqobj.checked = "check";
            for (var i = cqobj.output.length - 1; i >= 0; i--) {
              cqobj.output[i].mapElement.setMap(gmap);             
              $(cqobj.output[i].listElement).toggleClass("hidden"); 
            };              
        }        
    });
    return $('<li class="list-group-item qdata"></li>').append(colorIcon).append(removeIcon).append(displayIcon).append(qname);
}

function refreshRegiesteredQueryList(){
    $.ajax("tornado/queries").done(function(data){
            // console.log($("#regQueriesList li.empty").length);
            // console.log($("#regQueriesList li").length);
            //syslogAddStatus(data);
            // console.log(data);
            // console.log(uiState.catalog.queries.liststate);
            var qlist = $("#regQueriesList")
            var emptyState = '<li class="list-group-item empty">No registered continuous queries</li>';
            if (data.length > 0) {

                // cache data
                // new queries registered
                for (var i = data.length - 1; i >= 0; i--) {
                    var qi = uiState.catalog.queries.keys.indexOf(data[i].name);
                    if (qi === -1){
                       // new query update client state
                       qi = uiState.catalog.queries.keys.push(data[i].name) -1;    
                       var nqo = data[i];
                       nqo.checked = "check";
                       nqo.output = [];
                       console.log(nqo);
                       uiState.catalog.queries.vals.push(nqo); 
                    }                    
                };
                    //uiState.catalog.queries.liststate = data;
                    //console.log("here?");            

                    // check if qlist is currently empty (has the empty message)
                    if (qlist.find("li.empty").length > 0) qlist.empty();    

                    if (qlist.find("li").length != data.length) {
                        qlist.empty();
                        for (var i = 0; i < uiState.catalog.queries.vals.length; i++) {
                             // console.log(data[i]);
                             // console.log(data[i].outputColor);
                            qlist.append(createQueryListEntry(i));
                        };
                    }            
            }
            else{
                if (qlist.find("li.empty").length == 0) {
                    qlist.empty();    
                    qlist.append($(emptyState));
                }
                
            }

        });
}


function updateRegisteredQueries(qast, msg){
    syslogAddStatus(msg);
    // console.log(qast);
    var qlist = $("#regQueriesList");
    // check if qlist is currently empty (has the empty message)
    // if (qlist.find("li.empty").length > 0) qlist.empty();    
    // qlist.append(createQueryListEntry(qast));
    // The following will require a trip to the server to fetch the query list and update local state, can we update locally only ??
    refreshRegiesteredQueryList();            
}


// Doc load 
$(document).ready(function () {

    nodes = new vis.DataSet();
    edges = new vis.DataSet();

    // create a network
    var container = document.getElementById('visholder');
    var data = {
        nodes: nodes,
        edges: edges
    };
    var options = {
        hierarchicalLayout: true,
        shape: "database",
        smoothCurves: false,
        edges: {
            style: 'arrow'
        },
        physics:{
            "hierarchicalRepulsion": {
              "centralGravity": 0,
              "springLength": 0,
              "springConstant": 0,
              "nodeDistance": 10,
              "damping": 0
            },
            "maxVelocity": 57,
            "solver": "hierarchicalRepulsion",
            "timestep": 0.01
        }
    };
    var network = new vis.Network(container, data, options);
    
    /** Parser Code **/
    var grammartxt = $("#SyncSQLGrammar").text().trim();
    
    $.ajax("grammar.txt").done(function(data){
        //console.log(data);
        uiState.grammartxt = data;
        uiState.parser = PEG.buildParser(data, uiState);
    })

    //console.log(grammartxt);
    //var parser = PEG.buildParser(grammartxt, uiState);


    $("#buttonPanel span").click(function (event) {
        $("#textData").val($(this).attr('data'));
    });

    $('#submitSQL').click(function () {
        var ast = syncSQLparse(uiState.parser);
        var gmapView = createMBR(gmap);
        // console.log($.toJSON(gmapView));

        // add current view
        ast.currentView = gmapView;

        // add color 
        ast.outputColor = uiState.getRandomColor();
        console.log(ast.outputColor);

        //optimizeQueryPlan(ast.plan);

        // post query submitted query to server
        $.ajax( "tornado/queries", {
            method: "POST",
            data: $.toJSON(ast),
            dataType: "json"
        }).done(function(data){
            //console.log(data);
            // syslogAddStatus(data);
            updateRegisteredQueries(ast,data);

        });
    });

    $("#compileSQL").click(function () {
        var ast = syncSQLparse(uiState.parser);

        // console.log(ast);
        $("#dialog-vis").toggleClass("hidden");


    });
    /**     **/

    // Intialize google maps
//                google.maps.event.addDomListener(window, 'load', initialize);
    initialize()
    getDemoData();
    
    $('#test').click(function (event) {
        var url = '';
    });
    
    $('#addNode').click(function (event) {
        addNode();
    });

    $('#addEdge').click(function (event) {
        addEdge();
    });


    function addNode() {

        var i = Math.floor((Math.random() * 100) + 1);
        nodes.add([{id: "n" + i, label: 'Node ' + i}]);
        console.log("addning a node id=" + i);
        console.log("length = " + nodes.length);
    }

    function addEdge() {
        var ids = nodes.getIds()
        var i = Math.floor((Math.random() * ids.length));
        var j = Math.floor((Math.random() * ids.length));
        if (i == j) {
            if (j == ids.length - 1) {
                j == 0;
            }
            else {
                j = j + 1;
            }
        }

        edges.add({from: ids[i], to: ids[j]})
        console.log("adding an edge between (" + ids[i] + "," + ids[j] + ")");
    }

    function syncSQLparse(_parser) {
        try {
            
            // var sqltext = $("#textData").val().trim();
            var sqltext = uiState.editor.getValue().trim();
            var pres = _parser.parse(sqltext);
            pres.sqltext = sqltext;
            // console.log(pres);
            // prettyPrintAST('prettyPrint',pres);
            
            //console.log($.toJSON(pres));
            // var plan = pres.plan;
//                        plan = optimizeQueryPlan(plan);
            // visualizeQueryPlan(plan);
            
            return pres;
        } catch (err) {
            //$('#prettyPrint').text(jsDump.parse(err));
            var errmsg = buildErrorMessage(err);
            var msge = $("<div class='panel panel-primary'>"+errmsg+"</div>");
            $("#syslog").append(msge);
            console.log($.toJSON(err));
            return null;
        }
    }

    function getLevels(node) {

        if (node.children != undefined) {
            return getLevels(node.children[0]) + 1;
        }
        else {
            return 1;
        }
    }

    function addNodeQPlan(node, parentId, k, lv) {
        if (node != undefined) {
            var nodeId = node.type + "" + (++k);

            // check if it exists
            if (nodes.get(nodeId) != null) {
                nodeId += ""+(lv);
            }

            var nodeLabel = node.type + " : ";
            if (node.attributes != undefined) {
                nodeLabel += $.toJSON(node.attributes);
            }

            if (node.conditions != undefined) {

                // var conds = $.toJSON(node.conditions);
                var conds = node.conditions;
                //console.log(conds);
                //should only have one??
                for (var i = 0; i < node.conditions.length; i++) {
                    var cond = node.conditions[i];

                    nodeLabel += "\n";

                    if (i != 0) nodeLabel += " and ";

                    if ($.isPlainObject(cond.lhs))
                        nodeLabel += cond.lhs.sourceName + "." + cond.lhs.attributeName;
                    else nodeLabel += cond.lhs;

                    nodeLabel += "  " + cond.op; 

                    if (cond.argc == 3) {
                        nodeLabel += "(" + cond.cval + ") "; 
                    }
                    else {
                        nodeLabel += " ";
                    }

                    // nodeLabel += " , " 
                    if ($.isPlainObject(cond.rhs))
                        nodeLabel += cond.rhs.sourceName + "." + cond.rhs.attributeName;
                    else nodeLabel += "(" + cond.rhs + ") "; 
                };

                
            }

            if (node.name != undefined) {
                nodeLabel += node.name;
            }

            var n = [{id: nodeId, label: nodeLabel,shape:'text', level: (k)}];
            // console.log($.toJSON(n));
            nodes.add(n);
            // console.log("adding a node (" + nodeId + ")");
            edges.add({from: nodeId, to: parentId});
            // console.log("adding an edge between (" + nodeId + "," + parentId + ")");
            if (node.children != undefined) {
                // console.log("node children length of (" + nodeId + ")  = "+node.children.length);
                for (var i = 0; i < node.children.length; i++) {
                    addNodeQPlan(node.children[i], nodeId, k, lv);
                    // console.log("added child #"+i+"  for (" + nodeId + ")");
                }
                // console.log("finished adding all child nodes  for (" + nodeId + ")");
                
            }

        }


    }
    
    function visualizeQueryPlan(plan){
        // console.log("vizualizing : "+$.toJSON(plan));
        var i = 0;
        var rootId = plan.type + "" + (++i);
        // nodes.add([{id: rootId, label: plan.type+" : "+plan.attributes.toString()}]);
        var lv = getLevels(plan);
        // console.log("levels = " + lv);

        nodes.clear();
        edges.clear();

        // console.log("cleared nodes and edges");

        addNodeQPlan(plan, rootId, i, lv);

        // console.log("added nodes and edges");

    }

});

var markers = [];

function clearMap() {
    while (drawingContext.rectangles.length > 0) {
        drawingContext.rectangles.pop().setMap(null);
    }
    drawingOptions.rectangleOptions = beforeRectOptions;
    drawingManager.setOptions(drawingOptions);
    drawingManager.setMap(gmap);

    // remove all polylines

    while (polylines.length > 0) {
        polylines.pop().setMap(null);
    }


}

// place holder object for the request
var request = {
    mbr: null
};

// This function creates the MBR (spatial predicate) for the query
function updateMBR(qid, rect) {
    // console.log(qid);
    // console.log(uiState.rangeList[qid]);
    console.log(createMBR(rect));
    uiState.rangeList[qid].rect = rect;
}

function addRangePredEntry(contId, eid, qname) {
    $("#" + contId).append($('<li style="border:solid thin;border-color:black;"></li>').text(qname));
}

function updateRangeList() {
    $("#rangeList").empty();
    for (var i = 0; i < uiState.rangeList.length; i++) {
        var t = uiState.rangeList[i];
        //console.log(t);
        var pe = createPredicateListEntry(t);
        $("#rangeList").append(pe);
        // addRangePredEntry("rangeList", t.name, t.name);

    }
    ;
}


function updateKNNPred(qid, marker,opt) {
    // console.log(marker);
    marker.setOptions(opt);
    uiState.knnList[qid].marker = marker;
}

function addKNNPredEntry(contId, p) {
    $("#" + contId).append($('<li style="border:solid thin;border-color:' + p.color + ';"></li>').text(p.name));
}
function updateKNNList() {
    $("#knnList").empty();
    for (var i = 0; i < uiState.knnList.length; i++) {
        var p = uiState.knnList[i];
        // console.log(p);
        var pe = createPredicateListEntry(p);
        $("#knnList").append(pe);
        // addKNNPredEntry("knnList", p);
    }
    ;
}


function createLatLng(marker) {
    return {lat: marker.getPosition().lat(), lng: marker.getPosition().lng()}
}

function createMBR(rect) {
    return {
        north: rect.getBounds().getNorthEast().lat(),
        east: rect.getBounds().getNorthEast().lng(),
        south: rect.getBounds().getSouthWest().lat(),
        west: rect.getBounds().getSouthWest().lng()
    };
}

function createColoredMarker(pinColor) {
    // var pinColor = "FE7569";
    // handle pounsign
    //var pinColor = uiState.getRandomColor();

    if (pinColor[0] == '#'){
        pinColor = pinColor.slice(1).toUpperCase();
        //console.log(pinColor);
    }


    var pinImage = new google.maps.MarkerImage("http://chart.apis.google.com/chart?chst=d_map_pin_letter&chld=%E2%80%A2|" + pinColor,
            new google.maps.Size(21, 34),
            new google.maps.Point(0, 0),
            new google.maps.Point(10, 34));
    return pinImage.url;
}


function prettyPrintAST(target,ast){
    $('#'+target).text(jsDump.parse(ast));
}


function getDemoData(){
    var skydeck = $.parseJSON('{"lng":-87.6358852,"tags":[{"k":"name","v":"SkyDeck Chicago"},{"k":"operator","v":"Willis Tower"},{"k":"tourism","v":"attraction"}],"ts":"2015-03-19T19:48:41Z","nid":"2311635030","lat":41.8786383}');
    console.log(skydeck.lng);
    console.log(skydeck.lat);
    gmap.setCenter({lat: 41.8786383, lng: -87.6358852});
    gmap.setZoom(14);
}

function optimizeQueryPlan(plan){
    console.log("optimizing : "+$.toJSON(plan));
    var oper = plan;
    var parent = null;
    while (oper.type !== "stream"){
        //add back pointer to parent
//                    for (var i = 0; i < oper.children.length; i++){
//                        oper.children[i].parent = oper;
//                    }
        
//                    oper.children[0].parent = oper;
        console.log("parent added : "+$.toJSON(oper));
        
        if (oper.type === "select"){
            var leftchild = oper.children[0];
            if (leftchild.type === "stream" && leftchild.name === "places"){
                console.log("leftchild : "+$.toJSON(leftchild)+" , typeOf leftchild "+typeof(leftchild));
                //leftchild.parent = oper.parent;
                leftchild.keyword = oper.conditions[0][2];
//                            oper.parent.children[0] = leftchild; // assume left child for now
                parent.children = oper.children;
                oper = leftchild;
            }
            else{
                parent = oper;
                oper = oper.children[0];
            }
        }
        else{
            parent = oper;
            oper = oper.children[0];
        }
        
    }
    console.log("source : "+$.toJSON(oper));
    console.log("optimized : "+$.toJSON(plan));
    return plan;
}

function executeQueryOperator(oper){
    var cnum = oper.children.length;
    if (cnum === 1){
        // unary operator
        if (oper.type === "project"){
            var resset = executeOperator(oper.children[0]);
            for (i in resset) console.log(i)
        }
        
        if (oper.type === "select"){
            
        }
        
        if (oper.type === "stream"){
            if (oper.name === "places"){
                // use google API
            }
            else{
                console.log("Cannot resolve source:"+oper.name);
            }
        }
        
    }
    else if (cnum === 2){
        // binary
    }
}




