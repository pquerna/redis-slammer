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


var randomstring = require("randomstring");
var zipfan = require('zipfian');

var prefix = 'u:';

var cache = {};

function getValue(len) {
	if (cache[len] === undefined) {
		cache[len] = randomstring.generate(len)
	}
	return cache[len];
}

function Datagen(totalSize, valueSize) {
	this._targetCount = Math.floor(totalSize / valueSize);
	this._count = 0;
	this._totalSize = totalSize;
	this._valueSize = valueSize;
}

exports.Datagen = Datagen;

Datagen.prototype.next = function(callback) {
	if (this._count > this._targetCount) {
		callback(new Error('over size'));
	}
	else {
		var k = prefix + this._count;
		var v = getValue(this._valueSize);
		this._count++;
		callback(null, k, v);
	}
};

function DataBench(totalSzie, valueSize) {
	this._valueSize = valueSize;
	this._keycount = totalSzie / valueSize;
	this._zf = zipfan.getGenerator(this._keycount);
}

exports.DataBench = DataBench;

DataBench.prototype.nextRead = function(callback) {
	callback(null,
		prefix + this._zf.nextInt());
};

DataBench.prototype.nextWrite = function(callback) {
	callback(null,
		prefix + this._zf.nextInt(),
		getValue(this._valueSize));
};
