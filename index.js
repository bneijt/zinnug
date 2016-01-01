

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

function loadPlainWordsFrom(filePath) {
    return function (everythingLoaded) {
        async.waterfall([
            function openConnection(done) {
                console.log("Connecting");
                r.connect({db:"zinnug"}, done);
            },
            function insertWords(connection, done) {
                //Split data files into words and put them in the databse
                fs.readFile(filePath, {"encoding": "utf-8"}, function(error, wholeFile) {
                    if (error) {
                        throw err
                    }
                    console.log("Translating " + filePath);
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
                    console.log("Read " + words.length + " words");
                    async.each(_.chunk(words, 500), function insertWordObjects(chunkOfWords, next) {
                        r.table("words").insert(_.map(chunkOfWords, function(word) { return {id: word};}), {conflict: "replace"}).run(connection, next);
                    }, function(){ console.log("done"); done(null, connection)});
                });
            },
            function closeConnection(connection, done) {
                console.log("Closing connection");
                connection.close();
                everythingLoaded(null);
            }
        ], console.log);
    }
}


function loadTripletsForm(filePath) {
    return function (everythingLoaded) {
        async.waterfall([
            function openConnection(done) {
                console.log("Connecting");
                r.connect({db:"zinnug"}, done);
            },
            function insertTriplets(connection, done) {
                //Split data files into words and put them in the databse
                fs.readFile(filePath, {"encoding": "utf-8"}, function(error, wholeFile) {
                    if (error) {
                        throw err
                    }
                    console.log("Loading triplets from " + filePath);

                    //Word triplets
                    var words = translate(wholeFile, [
                        /[^a-zA-Z.]+/g, ' ',
                        /\.+/g, ' .. ', //End of sentence marker
                        / +/g, ' ',
                        ]).split(" ");
                    //Minimum length of 1
                    words = _.filter(words, function (x) { return x.length > 1; });

                    console.log("Found " + words.length + " words in " + filePath);
                    //We are loosing triplets by chunking and then creating triplets, but we are winning memory.
                    //TODO Use stream processing here, to keep memory usage to a minium and still retain all triplets
                    //Insert triplets
                    async.each(_.chunk(words, 2000), function insert(chunkOfWords, next) {

                        var triplets = _.map(chunkOfWords, function crTriplets(value, index, collection){
                            var triplet = collection.slice(index, index + 3);
                            if(triplet.length == 3) {
                                return {
                                    id: triplet.join("-").toLowerCase(),
                                    fst: triplet[0],
                                    snd: triplet[1],
                                    trd: triplet[2]};
                            }
                            return null;
                        });

                        triplets = _.reject(triplets, _.isNull);



                        r.table("triplets").insert(triplets, {conflict: "replace"}).run(connection, next);
                    }, function(){ console.log("done"); done(null, connection)});
                });
            },
            function closeConnection(connection, done) {
                console.log("Closing connection");
                connection.close();
                everythingLoaded(null);
            }
        ], console.log);
    }
}
function loadSentencesFrom(filePath) {
    return function (everythingLoaded) {
        async.waterfall([
            function openConnection(done) {
                console.log("Connecting");
                r.connect({db:"zinnug"}, done);
            },
            function insertSentences(connection, done) {
                //Split data files into words and put them in the databse
                fs.readFile(filePath, {"encoding": "utf-8"}, function(error, wholeFile) {
                    if (error) {
                        throw err
                    }
                    console.log("Loading sentences from " + filePath);

                    //Word triplets
                    var sentences = translate(wholeFile, [
                        /[^a-zA-Z.]+/g, ' ',
                        /\.+/g, ' .. ', //End of sentence marker
                        / +/g, ' ',
                        ]).split(" .. ");
                    //Minimum length of 2 words
                    sentences = _.filter(sentences, function (x) { return x.length > 2; });

                    console.log("Found " + sentences.length + " in " + filePath);
                    //We are loosing triplets by chunking and then creating triplets, but we are winning memory.
                    //TODO Use stream processing here, to keep memory usage to a minium and still retain all triplets
                    //Insert triplets
                    async.each(_.chunk(sentences, 200), function insert(chunkOfSentences, next) {

                        var sentenceObjects = _.map(chunkOfSentences, function crSentence(value, index, collection){
                            return {"id": value};
                        });

                        r.table("sentences").insert(sentenceObjects, {conflict: "replace"}).run(connection, next);
                    }, function(){ console.log("done"); done(null, connection)});
                });
            },
            function closeConnection(connection, done) {
                console.log("Closing connection");
                connection.close();
                everythingLoaded(null);
            }
        ], console.log);
    }
}

function numberLetters(phoneNumber) {
    return _.map(phoneNumber, function lettersForNumber(n){
        return {
            "0": ["0"],
            "1": ["1"],
            "2": _.map("2abc"),
            "3": _.map("3def"),
            "4": _.map("4ghi"),
            "5": _.map("5jkl"),
            "6": _.map("6mno"),
            "7": _.map("7pqrs"),
            "8": _.map("8tuv"),
            "9": _.map("9wxyz"),
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

function scoreTriplets(lettersForEachNumber) {
    var zeroScores = new Array(lettersForEachNumber.length);
    _.fill(zeroScores, 0);
    return function (triplet) {
        triplet['score'] = zeroScores.slice();
        _.each(lettersForEachNumber, function (letters, index) {
            if(_.includes(lettersForEachNumber[index -1], triplet['fst'][0])) {
                triplet['score'][index -1] += 1;
            }
            if(_.includes(lettersForEachNumber[index   ], triplet['snd'][0])){
                triplet['score'][index] += 1;
            }
            if(_.includes(lettersForEachNumber[index +1], triplet['trd'][0])){
                triplet['score'][index +1] += 1;
            }
        });
        return triplet;
    }
}

function bestMatchPerLetterGenerator() {
    var bestMatch = undefined;

    return function bestMatchPerLetter(err, tripletWithScore, push, next) {
        if (err) {
            push(err);
            return next();
        }
        else if (tripletWithScore === highland.nil) {
            //Send best match per letter through
            push(null, bestMatch);
            push(null, highland.nil);
        }
        else {
            // console.log(tripletWithScore);
            if(bestMatch === undefined) {
                bestMatch = new Array(tripletWithScore['score'].length);
            }
            if(tripletWithScore['score'] == undefined) {
                console.log(tripletWithScore);
            }
            //Check the scores for each triplet
            _.each(bestMatch, function(currentBestMatch, index) {
                if(currentBestMatch === undefined) {
                    bestMatch[index] = tripletWithScore;
                } else if (currentBestMatch['score'][index] < tripletWithScore['score'][index]) {
                    bestMatch[index] = tripletWithScore;
                } else if (currentBestMatch['score'][index] == tripletWithScore['score'][index]) {
                    //The triplet has equal scoring at the given index, test score sum
                    if(_.sum(currentBestMatch['score']) < _.sum(tripletWithScore['score'])) {
                        bestMatch[index] = tripletWithScore;
                    }
                }
            });
            return next();
        }
    };
}
function logFstOfTripletArray(tripletArray) {
    _.each(tripletArray, function(x) {
        console.log(x['score'], x['id']);
    });
}
function algo1(phoneNumber) {
    //Look up all word combinations for the numbers
    var lettersForEachNumber = numberLetters(phoneNumber);
    var letters = _.uniq(_.flatten(lettersForEachNumber));
    console.log("For " + phoneNumber);
    readTable("triplets").
        filter(tripletStartsWithAnyOf(letters)).
        map(scoreTriplets(lettersForEachNumber)).
        consume(bestMatchPerLetterGenerator()).
        each(logFstOfTripletArray);
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
            //loadHetBoek();
            gutenbergBooks = _.map(_.filter(fs.readdirSync("data/gutenberg.org"), function(x) {return x.indexOf(".txt.utf-8") > 0; }),
                function (filename) {
                    return loadTripletsForm("data/gutenberg.org/" + filename);
                });

            async.waterfall([
                // loadPlainWordsFrom("data/Het-Boek.txt"),
                loadTripletsForm("data/Het-Boek.txt"),
                ].concat(gutenbergBooks), console.log);
            break;
        case "run":
            var number = "55776584"; //randomly selected number
            algo1(number);
            break;
        default:
            console.log("Unknown command " + command);
    }

}

main(process.argv)

