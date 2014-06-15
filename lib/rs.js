/**
 *  Copyright 2014 Paul Querna
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


var rhr = require('./redis-hr');
var async = require('async');
var data = require('./data')

function writerFunc(expireTs, hr, db, stats) {
	return function(callback) {
		async.whilst(
			function() {
				return expireTs > Date.now();
			},
			function(callback) {
				db.nextWrite(function(err, k, v) {
					if (err) {
						callback(err);
						return;
					}
					hr.set(k, v, function(err) {
						stats.writes++;
						callback(err);
					});
				});
			},
			callback);
	}
}

function readerFunc(expireTs, hr, db, stats) {
	return function(callback) {
		async.whilst(
			function() {
				return expireTs > Date.now();
			},
			function(callback) {
				db.nextRead(function(err, k) {
					if (err) {
						callback(err);
						return;
					}
					hr.get(k, function(err, reply) {
						stats.reads++;
						callback(err, reply)
					});
				});
			},
			callback);
	}
}

function StatsKeeper() {
	this.reads = 0;
	this.writes = 0;
}

StatsKeeper.prototype.print = function () {
	console.log('reads: ' + this.reads);
	console.log('writes: ' + this.writes);
};

exports.benchmark = function(options, callback) {
	var hr = new rhr.RedisHashRing(options.hosts);
	var dg =  new data.DataBench(options.size, options.valueSize);
	var stats = new StatsKeeper();

	async.series([
		function(callback) {
			hr.setup(callback);
		},
		function(callback) {
			var expireTs = Date.now() + options.time;
			var i;
			var tasks = [];

			for (i = 0; i < options.writers; i++) {
				tasks.push(writerFunc(expireTs, hr, dg, stats));
			}

			for (i = 0; i < options.readers; i++) {
				tasks.push(readerFunc(expireTs, hr, dg, stats));
			}

			console.log('starting tasks');
			async.parallelLimit(
				tasks,
				options.readers + options.writers,
				callback
			);
		}],
		function(err) {
			if (err) {
				callback(err);
				return;
			}
			stats.print()
			callback();
		});
};

function loader(hr, dg) {
	return function(callback) {
		var cont = true;
		async.whilst(
			function() { return cont; },
			function(callback) {
				dg.next(function(err, k, v) {
					if (err) {
						cont = false;
						callback();
						return;
					}
					hr.set(k, v, callback);
					return;
				});
			},
			callback);
	}
}

exports.load = function(options, callback) {
	var hr = new rhr.RedisHashRing(options.hosts);
	var dg =  new data.Datagen(options.size, options.valueSize);

	async.series([
		function(callback) {
			hr.setup(callback);
		},
		function(callback) {
			var i;
			var tasks = [];

			for (i = 0; i < options.loaders; i++) {
				tasks.push(loader(hr, dg));
			}

			async.parallelLimit(
				tasks,
				options.loaders,
				callback
			);
		}],
		callback);
};

exports.run = function(options) {
	exports[options.task](options, function(err) {
		if (err) {
			console.error(err);
			process.exit(1);
		}
		else {
			process.exit(0);
		}
	});
};
