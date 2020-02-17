var zlib = require('zlib');
var redis = require('redis');
var async = require('async');
var os = require('os');
var algos = require('stratum-pool/lib/algoProperties.js');
//var store = require('store');
var logComponent = 'stats';

module.exports = function (logger, portalConfig, poolConfigs) {

    var _this = this;

    var logSystem = 'Stats';

    var redisClients = [];
    var redisStats;

    var myHistory = [];


    this.statHistory = [];
    this.statPoolHistory = [];

    this.stats = {};
    this.statsString = '';

    setupStatsRedis();
    gatherStatHistory();

    var canDoStats = true;

    Object.keys(poolConfigs).forEach(function (coin) {

        if (!canDoStats) return;

        var poolConfig = poolConfigs[coin];

        var redisConfig = poolConfig.redis;

        for (var i = 0; i < redisClients.length; i++) {
            var client = redisClients[i];
            if (client.client.port === redisConfig.port && client.client.host === redisConfig.host) {
                client.coins.push(coin);
                return;
            }
        }
        redisClients.push({
            coins: [coin],
            client: redis.createClient(redisConfig.port, redisConfig.host)
        });
    });


    function setupStatsRedis() {
        redisStats = redis.createClient(portalConfig.redis.port, portalConfig.redis.host);
        redisStats.on('error', function (err) {
            logger.error(logSystem, 'Historics', 'Redis for stats had an error ' + JSON.stringify(err));
        });
    }

    function gatherStatHistory() {

        var retentionTime = (((Date.now() / 1000) - portalConfig.website.stats.historicalRetention) | 0).toString();

        redisStats.zrangebyscore(['statHistory', retentionTime, '+inf'], function (err, replies) {
            if (err) {
                logger.error(logSystem, 'Historics', 'Error when trying to grab historical stats ' + JSON.stringify(err));
                return;
            }
            for (var i = 0; i < replies.length; i++) {
                _this.statHistory.push(JSON.parse(replies[i]));
            }
            _this.statHistory = _this.statHistory.sort(function (a, b) {
                return a.time - b.time;
            });
            _this.statHistory.forEach(function (stats) {
                addStatPoolHistory(stats);
            });
        });
    }

    function addStatPoolHistory(stats) {
        var data = {
            time: stats.time,
            pools: {}
        };
        for (var pool in stats.pools) {
            data.pools[pool] = {
                hashrate: stats.pools[pool].hashrate,
                workerCount: stats.pools[pool].workerCount,
                blocks: stats.pools[pool].blocks
            }
        }
        _this.statPoolHistory.push(data);
    }

    function getDouble(strNum) {
        var n = parseFloat(strNum);
        if (isNaN(n))
            n = 0;
        return n;
    }

    this.getGlobalStats = function (callback) {

        var statGatherTime = Date.now() / 1000 | 0;

        var allCoinStats = {};
        var z1 = 0;

        async.each(redisClients, function (client, callback) {
            var windowTime = (((Date.now() / 1000) - portalConfig.website.stats.hashrateWindow) | 0).toString();
            var redisCommands = [];


            var redisCommandTemplates = [
                ['zremrangebyscore', ':hashrate', '-inf', '(' + windowTime],
                ['zrangebyscore', ':hashrate', windowTime, '+inf'],
                ['hgetall', ':stats'],
                ['scard', ':blocksPending'],
                ['scard', ':blocksConfirmed'],
                ['scard', ':blocksKicked']
            ];

            var commandsPerCoin = redisCommandTemplates.length;

            client.coins.map(function (coin) {
                redisCommandTemplates.map(function (t) {
                    var clonedTemplates = t.slice(0);
                    clonedTemplates[1] = coin + clonedTemplates[1];
                    redisCommands.push(clonedTemplates);
                    // console.log('coin %s', coin);
                });
            });


            // R Andrews - History Page - Gather the Share percentages per Pending payment owed


            client.client.multi([
                    ['hgetall', 'biblepay:balances'],
                    ['smembers', 'biblepay:blocksPending']
                ]).exec(function (error, results) {

                    if (error) {
                        console.log('error %s', JSON.stringify(error));

                        logger.error(logSystem, logComponent, 'Could not get blocks from redis ' + JSON.stringify(error));
                    }

                    var workers1 = {};
                    var i1 = 0;
                    // Reserved for potentially exposing the amount paid:
                    /*
                    for (var w in results[0]) {
                    i1++;
                    workers1[w] = { balance: parseFloat(results[0][w]) };
                    }
                    */

                    var rounds = results[1].map(function (r) {
                        var details = r.split(':');
                        if (false)
                            console.log('details blockhash,txhash,height %s', r);

                        return {
                            blockHash: details[0],
                            txHash: details[1],
                            height: details[2],
                            serialized: r
                        };

                    });
                    //  Get Round Details

                    var shareLookups = rounds.map(function (r) {
                        return ['hgetall', 'biblepay:shares:round' + r.height]
                    });

                    client.client.multi(shareLookups).exec(function (error, allWorkerShares) {
                        if (error) {
                            console.log('error %s', error);
                            callback('Check finished - redis error with multi get rounds share');
                            return;
                        }

                        // In rounds:
                        rounds.forEach(function (round, i) {
                            var workerShares = allWorkerShares[i];

                            if (!workerShares) {
                                console.log('error 706');
                                logger.error(logSystem, logComponent, 'No worker shares for round: ' + round.height + ' blockHash: ' + round.blockHash);
                                return;
                            }
                            switch (round.category) {
                                case 'kicked':
                                case 'orphan':
                                    round.workerShares = workerShares;
                                    break;
                                default:
                                    magnitude = 1;
                                    var reward = parseInt(round.reward * magnitude);
                                    var totalShares = Object.keys(workerShares).reduce(function (p, c) {
                                        return p + parseFloat(workerShares[c])
                                    }, 0);
                                    for (var workerAddress in workerShares) {
                                        var percent = parseFloat(workerShares[workerAddress]) / totalShares;
                                        var workerRewardTotal = Math.floor(reward * percent);
                                        var worker = {};
                                        worker.reward = (worker.reward || 0) + workerRewardTotal;
                                        worker.address = workerAddress;
                                        z1++;
                                        myHistory[z1] = {
                                            address: worker.address,
                                            percent: percent,
                                            blockHash: round.blockHash,
                                            txHash: round.txHash,
                                            height: round.height
                                        };
                                        if (false)
                                            console.log('z1 %d, my worker perc %d reward  %s address %s', z1, percent, worker.reward, worker.address);

                                    }
                                    break;
                            }
                        });
                    });
                    // End of Get Round Details
                })

            // End of BiblePay History gathering for Pending Payments and share percentages

            client.client.multi(redisCommands).exec(function (err, replies) {
                if (err) {
                    logger.error(logSystem, 'Global', 'error with getting global stats ' + JSON.stringify(err));
                    callback(err);
                }
                else {
                    for (var i = 0; i < replies.length; i += commandsPerCoin) {
                        var coinName = client.coins[i / commandsPerCoin | 0];
                        var coinStats = {
                            name: coinName,
                            symbol: poolConfigs[coinName].coin.symbol.toUpperCase(),
                            algorithm: poolConfigs[coinName].coin.algorithm,
                            hashrates: replies[i + 1],
                            history: myHistory,
                            poolStats: {
                                validShares: replies[i + 2] ? (replies[i + 2].validShares || 0) : 0,
                                validabnShares: replies[i + 2] ? (replies[i + 2].validabnShares || 0) : 0,
                                validBlocks: replies[i + 2] ? (replies[i + 2].validBlocks || 0) : 0,
                                invalidShares: replies[i + 2] ? (replies[i + 2].invalidShares || 0) : 0,
                                totalPaid: replies[i + 2] ? (replies[i + 2].totalPaid || 0) : 0
                            },
                            blocks: {
                                pending: replies[i + 3],
                                confirmed: replies[i + 4],
                                orphaned: replies[i + 5]
                            }
                        };
                        allCoinStats[coinStats.name] = (coinStats);
                    }
                    callback();
                }
            });
        }, function (err) {
            if (err) {
                console.log('error getting the stats %s', err);
                logger.error(logSystem, 'Global', 'error getting all stats' + JSON.stringify(err));
                callback();
                return;
            }

            var portalStats = {
                time: statGatherTime,
                global: {
                    workers: 0,
                    hashrate: 0
                },
                algos: {},
                pools: allCoinStats
            };
            // 10-29-2019  - BiblePay

            Object.keys(allCoinStats).forEach(function (coin) {
                var coinStats = allCoinStats[coin];
                coinStats.workers = {};
                coinStats.shares = 0;

                coinStats.hashrates.forEach(function (ins) {
                    var parts = ins.split(':');
                    var workerShares = parseFloat(parts[0]);
                    var worker = parts[1];
                    //        var hashrateData = [isValidShare ? shareData.difficulty : -shareData.difficulty * penalty, shareData.worker, dateNow, shareData.xmrsuccess, shareData.xmrfail, shareData.xmrcharitysuccess, shareData.arityfail];
                    if (worker in coinStats.workers) {
                    }else{
                        coinStats.workers[worker] = {
                            shares: workerShares,
                            invalidshares: 0,
                            bbpsuccess: 0,
                            bbpfail: 0,
                            xmrsuccess: 0,
                            xmrfail: 0,
                            xmrcharitysuccess: 0,
                            xmrcharityfail: 0,
                            hashrateString: null
                        };
                    }
                    if (workerShares > 0) {
                        coinStats.shares += workerShares;
                        coinStats.workers[worker].shares += workerShares;
                    }
                    else {
                        coinStats.workers[worker].invalidshares -= workerShares; // workerShares is negative number!
                    }
                    if (parts.length > 3) {
                        coinStats.workers[worker].xmrsuccess += getDouble(parts[3]);
                        coinStats.workers[worker].xmrfail += getDouble(parts[4]);
                        coinStats.workers[worker].xmrcharitysuccess += getDouble(parts[5]);
                        coinStats.workers[worker].xmrcharityfail += getDouble(parts[6]);
                        if (false) {
                            console.log(" memorize %s xmrs %d and xmrf %d %d %d", ins, coinStats.workers[worker].xmrsuccess,
                            coinStats.workers[worker].xmrfail, coinStats.workers[worker].xmrcharitysuccess, coinStats.workers[worker].xmrcharityfail);
                        }
                    }
                });

                var shareMultiplier = Math.pow(2, 32) / algos[coinStats.algorithm].multiplier;
                coinStats.hashrate = shareMultiplier * coinStats.shares / portalConfig.website.stats.hashrateWindow;

                coinStats.workerCount = Object.keys(coinStats.workers).length;
                portalStats.global.workers += coinStats.workerCount;

                /* algorithm specific global stats */
                var algo = coinStats.algorithm;
                if (!portalStats.algos.hasOwnProperty(algo)) {
                    portalStats.algos[algo] = {
                        workers: 0,
                        hashrate: 0,
                        hashrateString: null
                    };
                }
                portalStats.algos[algo].hashrate += coinStats.hashrate;
                portalStats.algos[algo].workers += Object.keys(coinStats.workers).length;

                for (var worker in coinStats.workers) {
                    coinStats.workers[worker].hashrateString = _this.getReadableHashRateString(shareMultiplier * coinStats.workers[worker].shares / portalConfig.website.stats.hashrateWindow);
                }

                delete coinStats.hashrates;
                delete coinStats.shares;
                coinStats.hashrateString = _this.getReadableHashRateString(coinStats.hashrate);
            });

            Object.keys(portalStats.algos).forEach(function (algo) {
                var algoStats = portalStats.algos[algo];
                algoStats.hashrateString = _this.getReadableHashRateString(algoStats.hashrate);
            });

            _this.stats = portalStats;
            _this.statsString = JSON.stringify(portalStats);



            _this.statHistory.push(portalStats);
            addStatPoolHistory(portalStats);

            var retentionTime = (((Date.now() / 1000) - portalConfig.website.stats.historicalRetention) | 0);

            for (var i = 0; i < _this.statHistory.length; i++) {
                if (retentionTime < _this.statHistory[i].time) {
                    if (i > 0) {
                        _this.statHistory = _this.statHistory.slice(i);
                        _this.statPoolHistory = _this.statPoolHistory.slice(i);
                    }
                    break;
                }
            }

            redisStats.multi([
                ['zadd', 'statHistory', statGatherTime, _this.statsString],
                ['zremrangebyscore', 'statHistory', '-inf', '(' + retentionTime]
            ]).exec(function (err, replies) {
                if (err)
                    logger.error(logSystem, 'Historics', 'Error adding stats to historics ' + JSON.stringify(err));
            });
            callback();
        });

    };

    this.getReadableHashRateString = function (hashrate) {
        var i = -1;
        hashrate = hashrate;
        var byteUnits = [' H', ' KH', ' MH', ' GH', ' PH'];
        do {
            hashrate = hashrate / 1000;
            i++;
        } while (hashrate > 1000);
        return hashrate.toFixed(2) + byteUnits[i];
    };

};
