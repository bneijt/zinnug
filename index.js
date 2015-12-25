

var r = require('rethinkdb'),
    fs = require('fs'),
    _ = require('lodash'),
    async = require('async');

function initDb() {
    async.waterfall([
        function openConnection(done) {
            console.log("Connecting");
            r.connect({}, done);
        },
        function getDatabaseList(connection, done) {
            console.log("Listing databases");
            r.dbList().run(connection, function(error, result) { done(error, result, connection);});
        },
        function createDBIfNotExists(existingDatabases, connection, done) {
            console.log("Creating databases if needed");
            if(existingDatabases.indexOf("zinnug") >= 0) {
                console.log("Database exists");
                done(null, {}, connection);
            } else {
                console.log("Creating databaes");
                r.dbCreate("zinnug").run(connection, function(error, result) { return done(error, result, connection);});
            }
        },
        function createTables(databaseCreationResult, connection, done) {
            console.log("Checking tables");
            r.db("zinnug").tableList().run(connection, function(error, result) { return done(error, result, connection);});
        },
        function checkTables(existingTables, connection, done) {
            var tables = ['words', 'wordTransitions'],
                lastTable = 'wordTransitions';

            _.forEach(tables, function(tableName){
                if(existingTables.indexOf(tableName) >= 0) {
                    console.log("Table " + tableName + " already exists");
                    if(tableName == lastTable) {
                        done(null, connection);
                    }
                } else {
                    console.log("Creating table " + tableName);
                    r.db('zinnug').tableCreate(tableName).run(connection, function(err, result) {
                            if (err) {
                                throw err;
                            }
                            console.log("Created table " + tableName);
                            if(tableName == lastTable) {
                                done(null, connection);
                            }
                        });
                }
            });
        },
        function closeConnection(connection) {
            console.log("Closing connection");
            connection.close();
        }
    ]);

}

function translate(haystack, searchReplaces) {
    for(var i = 0; i < searchReplaces.length; i += 2) {
        haystack = haystack.replace(searchReplaces[i], searchReplaces[i+1]);
    }
    return haystack;
}

function loadHetBoek() {
    async.waterfall([
        function openConnection(done) {
            console.log("Connecting");
            r.connect({db:"zinnug"}, done);
        },
        function insertWords(connection, done) {
            //Split data files into words and put them in the databse
            fs.readFile('data/Het-Boek.txt', {"encoding": "utf-8"}, function(error, contents) {
                if (error) {
                    throw err
                }
                console.log("Translate")
                contents = translate(contents, [
                    /[^a-zA-Z]+/g, ' ',
                    / +/g, ' ',
                    ]);
                words = contents.split(" ");
                console.log("Loading words: " + words.length);
                wordObjects = _.map(words, function(word) { return {id: word, lastInsertSource: "Het-Boek"};});
                async.each(_.chunk(wordObjects, 500), function insertWordObjects(wordObjects, next) {
                    r.table("words").insert(wordObjects, {conflict: "replace"}).run(connection, next);
                }, function(){ console.log("done"); done(null, connection)});
            });
        },
        function closeConnection(connection) {
            console.log("Closing connection");
            connection.close();
        }
    ]);

}

function main(arguments) {
    //We have to do something to stop hitting a bug in drain()
    var command = arguments[2];
    if (command === undefined ){
        console.log("Requires a command as first argument")
    }
    switch(command) {
        case "initDb":
            initDb();
            break;
        case "loadData":
            loadHetBoek()
            break;
        default:
            console.log("Unknown command " + command);
    }

}

main(process.argv)

