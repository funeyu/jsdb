/*
**	return 1 when key1 > key2; return 0 key1 = key2; otherwise false
*/
const fs = require('fs');
const path = require('path');

const MIN_KEY = '-1';
const compare= function(key1, key2) {
	if(typeof key1 !== 'string' || typeof key2 !== 'string') {
		console.log(key1, key2);
		throw new Error('can not compare key not string!')
	}
	if(key1 === MIN_KEY && key1 !== MIN_KEY) {
		return -1;
	}
	if(key1 === MIN_KEY && key2 === MIN_KEY) {
		return 0;
	}
	if(key1 !== MIN_KEY && key2 === MIN_KEY) {
		return 1;
	}
	let cur = 0;
	for(;;) {
		if(key1[cur] && !key2[cur]) {
			return 1;
		}
		if(!key1[cur] && key2[cur]) {
			return -1
		}
		if(key1[cur] > key2[cur]) {
			return 1;
		}
		if(key2[cur] > key1[cur]) {
			return -1;
		}
		if(cur === key2.length && cur == key1.length) {
			return 0
		}
		cur ++;
	}
}

exports.compare = compare

const ByteSize = function(str) {
    str = str.toString();
    let len = str.length;

    for (let i = str.length; i--; ) {
        const code = str.charCodeAt(i);
        if (0xdc00 <= code && code <= 0xdfff) {
            i--;
        }

        if (0x7f < code && code <= 0x7ff) {
            len++;
        } else if (0x7ff < code && code <= 0xffff) {
            len += 2;
        }
    }

    return len;
}

exports.ByteSize = ByteSize


// generate auto incresed id and the length of id : 6
let count = 0; // less than 256 * 256
let id = 0;
const IdGen = function() {
    let timeId = + new Date();
    timeId = ~~(timeId / 10000);
    if(timeId > id) {
        count = 0;
        id = timeId
    } else {
        count ++
    }

    return {
    	timeId: timeId,
		count: count,
	};
}

exports.IdGen = IdGen;

// return 1 when id1 > id2; 0 when id1 === id2; otherwise -1
const IdCompare = function(id1Info, id2Info) {
    if(id1Info.timeId > id2Info.timeId) {
        return 1
    } else if(id1Info.timeId === id2Info.timeId) {
        if(id1Info.count > id2Info.count) {
            return 1;
        } else if(id1Info.count === id2Info.count) {
            return 0;
        } else if(id1Info.count < id2Info.count) {
            return -1;
        }
    } else {
        return -1;
    }
}
exports.IdCompare = IdCompare;

const hash = function(key) {
    let hash = 5381,
        i=key.length;
    while(i){
        hash = (hash * 33) ^ key.charCodeAt(--i);
    }

    return hash >>>0;
}
exports.hash = hash;

// 这里先简单的以JS的parse和stringify函数
const jsonStringify = function(jsonData) {
    let string = JSON.stringify(jsonData);

    return string;
}
exports.jsonStringify = jsonStringify;

const jsonParse = function(jsonString) {
    let json = JSON.parse(jsonString);
    return json;
}
exports.jsonParse = jsonParse;

const CreateFileIfNotExist = function(filePath) {
    let pathSep = filePath.split(path.sep);
    console.log('pathSep', pathSep);
    let _path = './';
    for(let i = 0; i < pathSep.length; i ++) {
        if(pathSep[i]) {
            _path += (path.sep + pathSep[i]);
            if(!fs.existsSync(_path)) {
                fs.mkdirSync(_path);
            }
        }
    }
}
exports.CreateFileIfNotExist = CreateFileIfNotExist;
