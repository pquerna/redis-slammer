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

var HashRing = require('hashring');
var async = require('async');
var redis = require("redis");
var _ = require('underscore');

/**
 * @constructor
 */
function RedisHashRing(hosts) {
	this._hosts = hosts;
	this._clients = {};
	this._ring = new HashRing(hosts);
}

exports.RedisHashRing = RedisHashRing;

RedisHashRing.prototype._addHost = function(host, callback) {
	var blah = host.split(/:/);
	this._clients[host] = redis.createClient(blah[1], blah[0]);

	callback = _.once(callback);

	this._clients[host].on('ready', callback);
	this._clients[host].on('error', callback);
	// TOOD: handle mid-run errors?
	// this._clients[host].on('error', this._redisError);
};

RedisHashRing.prototype.setup = function(callback) {
	var rhr = this;
	async.each(this._hosts,
		function(item, callback) {
			rhr._addHost(item, callback);
		},
		callback);
};

RedisHashRing.prototype.set = function(key, value, callback) {
	var h = this._ring.get(key);
	this._clients[h].set(key, value, callback);
};

RedisHashRing.prototype.get = function(key, callback) {
	var h = this._ring.get(key);
	this._clients[h].get(key, callback);
}
