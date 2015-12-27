

var r = require('rethinkdb'),
    fs = require('fs'),
    _ = require('lodash'),
    async = require('async'),
    highland = require('highland');


function readTable(tableName) {
    // Create a highland stream from a rethinkdb table cursor
    return highland(function (push, next) {
        r.connect({db : "zinnug"}, function dbOpened(err, connection){
            if(err) throw err;
            r.table(tableName).run(connection, function handleDocuments(err, cursor){
                if(err) throw err;
                cursor.each(push, function closeStreamAndDb() {
                    push(null, highland.nil);
                    connection.close();
                });
            });
        })
    });
};


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
            var tables = ['words', 'triplets', 'wordTransitions'],
                lastTable = 'wordTransitions';
            //TODO race condition here, we need to async.series the separate tables
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
            fs.readFile('data/Het-Boek.txt', {"encoding": "utf-8"}, function(error, wholeFile) {
                if (error) {
                    throw err
                }
                console.log("Translate")
                contents = translate(wholeFile, [
                    /[^a-zA-Z]+/g, ' ',
                    / +/g, ' ',
                    ]);

                //Each of the words
                words = contents.split(" ");
                //Minimum length of 1
                words = _.filter(words, function (x) { return x.length > 1; });
                words.sort();
                words = _.uniq(words, true);
                console.log("Loading words: " + words.length);
                wordObjects = _.map(words, function(word) { return {id: word, lastInsertSource: "Het-Boek"};});
                async.each(_.chunk(wordObjects, 500), function insertWordObjects(wordObjects, next) {
                    r.table("words").insert(wordObjects, {conflict: "replace"}).run(connection, next);
                }, function(){ console.log("done"); done(null, connection)});
            });
        },
        function insertTriplets(connection, done) {
            //Split data files into words and put them in the databse
            fs.readFile('data/Het-Boek.txt', {"encoding": "utf-8"}, function(error, wholeFile) {
                if (error) {
                    throw err
                }
                console.log("Loading triplets");

                //Word triplets
                words = translate(wholeFile, [
                    /[^a-zA-Z.]+/g, ' ',
                    /\./g, ' . ',
                    / +/g, ' ',
                    ]).split(" ");
                //Minimum length of 1
                words = _.filter(words, function (x) { return x.length > 1; });

                triplets = _.map(words, function crTriplets(value, index, collection){
                    var triplet = collection.slice(index, index + 3);
                    if(triplet.length == 3) {
                        return {id: triplet.join("-").toLowerCase(), fst: triplet[0], snd: triplet[1], trd: triplet[2]};
                    }
                    return null;
                });

                triplets = _.reject(triplets, _.isNull);
                console.log("Found " + triplets.length + " triplets");

                //Insert triplets
                async.each(_.chunk(triplets, 500), function insert(obj, next) {
                    r.table("triplets").insert(obj, {conflict: "replace"}).run(connection, next);
                }, function(){ console.log("done"); done(null, connection)});
            });
        },
        function closeConnection(connection) {
            console.log("Closing connection");
            connection.close();
        }
    ]);

}
function numberLetters(phoneNumber) {
    return _.map(phoneNumber, function lettersForNumber(n){
        return {
            "0": ["0"],
            "1": _.map("1abc"),
            "2": _.map("2def"),
            "3": _.map("3ghi"),
            "4": _.map("4jkl"),
            "5": _.map("5mno"),
            "6": _.map("6pqrs"),
            "7": _.map("7tuv"),
            "8": _.map("8wxyz"),
            "9": ["9"]
        }[n];
    });
}

function tripletStartsWithAnyOf(collection) {
    return function (x) {
        return _.includes(collection, x['id'][0]);
    }
}

function idStartsWithLetter(letter) {
    return function(obj) {
        return obj['id'][0] == letter;
    }
}

function orderTripletMatches(lettersForEachNumber) {
    return function (triplets) {
        console.log("Triplets: " + triplets);
        return '';
    }
}
function algo1(phoneNumber) {
    //Look up all word combinations for the numbers
    var lettersForEachNumber = numberLetters(phoneNumber);
    var letters = _.uniq(_.flatten(lettersForEachNumber));
    console.log("For " + phoneNumber);
    readTable("triplets").filter(tripletStartsWithAnyOf(letters)).collect().each(function anyMatchGiven(triplets){
        //Select any sentence
        _.each(lettersForEachNumber, function justTheTip(letters){
            var letter = letters[1];
            console.log(_.filter(triplets, idStartsWithLetter(letter))[0]['fst']);
        })
    });

}

function main(arguments) {
    //We have to do something to stop hitting a bug in drain()
    var command = arguments[2];
    if (command === undefined ){
        console.log("Requires a command as first argument")
    }
    switch(command) {
        case "init":
            //Run once after creating the database
            initDb();
            break;
        case "load":
            //Run as many times as the data changes
            loadHetBoek();
            break;
        case "run":
            var phoneNumber = "55776584"; //randomly selected phonenumber
            algo1(phoneNumber);
            break;
        default:
            console.log("Unknown command " + command);
    }

}

main(process.argv)

