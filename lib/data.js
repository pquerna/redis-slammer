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

var uid2 = require('uid2');
var zipfan = require('zipfian');

var prefix = 'u:';

function Datagen(totalSize, valueSize) {
	this._bytesUsed = 0;
	this._count = 0;
	this._totalSize = totalSize;
	this._valueSize = valueSize;
}

exports.Datagen = Datagen;

Datagen.prototype.next = function(callback) {
	if (this._bytesUsed > this._totalSize) {
		callback(new Error('over size'));
	}
	else {
		var k = prefix + this._count;
		var v = uid2(this._valueSize);
		this._count++;
		this._bytesUsed += this._valueSize;
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
		uid2(this._valueSize));
};
