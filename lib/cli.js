#!/usr/bin/env node

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

var rs = require('./rs');
var unit = require('unitology');
var bytes = require('bytes');

var yargs = require('yargs')
	.usage('Slam a Reids.\nUsage: $0 [options] [task]')
	.example('$0 -s 200gb load', 'Load redis with 200gb of data.')
	.example('$0 -s 200gb -T 30s benchmark', 'Benchmark loaded redis for 30 seconds.')
	.demand('s')
	.alias('s', 'size')
	.describe('s', 'Size of dataset to use.')
	.alias('T', 'time')
	.describe('T', 'Time of benchmark to run.')
	.alias('H', 'host')
	.describe('H', 'Hosts to use. Specify multiple times for multiple hosts.')
	.boolean('h')
	.alias('h', 'help')
	.describe('loaderstart', 'point at which to start loader.')
	.describe('loaderend', 'point at which to start loader.');

var argv = yargs.argv;

if (argv.help) {
	yargs.showHelp();
	process.exit(0);
}

if (argv._.length != 1) {
	console.error('Missing task. Available tasks: load, benchmark\n');
	yargs.showHelp();
	process.exit(1);
}

var task = argv._[0]

if (task != 'benchmark' && task != 'load') {
	console.error('Unknown task. Available tasks: load, benchmark\n');
	yargs.showHelp();
	process.exit(1);
}

var options = {
	task: task,
	size: bytes(argv.s),
	valueSize: 1000,
	loaders: 1000,
	loaderstart: null,
	loaderend: null,
	readers: 950,
	writers: 50,
	time: 30000,
	hosts: [],
};

if (argv.H) {
	options.hosts = [].concat(argv.H);
}

if (options.hosts.length == 0) {
	options.hosts.push('127.0.0.1:6379');
}

if (argv.T) {
	options.time = unit.time(argv.T).to('ms');
}

if (argv.loaderstart) {
	options.loaderstart = argv.loaderstart;
}

if (argv.loaderend) {
	options.loaderend = argv.loaderend;
}

rs.run(options);
