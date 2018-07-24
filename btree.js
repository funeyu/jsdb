const {DataPage, IdPage} = require('./page.js');

/*
**	return 1 when key1 > key2; return 0 key1 = key2; otherwise false
*/
const compare= function(key1, key2) {
	if(typeof key1 !== 'string' || typeof key2 !== 'string') {
		throw new Error('can not compare key not string!')
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

// generate auto incresed id and the length of id : 6
let count = 0; // less than 256 * 256
let id = 0;
const IdGen = function() {
	let timeId = + new Date();
	if(timeId > id) {
		count = 0;
	} else {
		count ++
	}
	id = timeId;
	return {
		timeId: timeId,
		count: count
	}
}

// return 1 when id1 > id2; 0 when id1 === id2; otherwise -1
const IdCompare= function(id1, id2) {
	if(id1.timeId > id2.timeId) {
		return 1
	} else if(id1.timeId === id2.timeId) {
		if(id1.count > id2.count) {
			return 1;
		} else if(id1.count === id2.count) {
			return 0;
		} else if(id1.count < id2.count) {
			return -1;
		}
	} else {
		return -1;
	}
}

console.log(compare('key21', 'key2'));

let currentPageNo = DataPage.getPageSize();

const insertData = function(data) {
	let id = IdGen();

	DataPage.load(currentPageNo, function(page) {
		let result = page.insertCell(id, data);
		if(!result) {
			page.flush();
			IdPage.insertCell(id, currentPageNo);
		}
	})
}


const reblance = function() {

}
