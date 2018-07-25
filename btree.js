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

let currentDataPageNo = DataPage.getPageSize();
let currentIdPageNo = IdPage.getPageSize();

const insertrecursively(idPage, id, pagePageNo) {
	if(!idPage.insertCell(id, pagePageNo)) {
		idPage.flush();
		currentIdPageNo ++;

		// not set parentPage
		let newIdPage = new IdPage(null, currentIdPageNo);
		newIdPage.insertCell(id, pagePageNo);

		if(idPage.)
	}
}

const insertData = function(data) {
	let id = IdGen();

	DataPage.load(currentDataPageNo, function(page) {
		let result = page.insertCell(id, data);
		if(!result) {
			page.flush();
			// currentDataPageNo = DataPage.getPageSize();
			currentDataPageNo ++;

			let dataPageNo = currentDataPageNo + 1
			let dataPage = new DataPage(dataPageNo);
			// to do: if the data large than DataPage size
			dataPage.insertCell(id, data);

			IdPage.load(currentIdPageNo, function(idPage) {
				let insertResult;
				for(;;) {
					insertResult = idPage.insertCell(id, dataPageNo);
					if(insertResult) {
						break;
					} else {
						let newIdPage = idPg
						idPage = idPage.getPageParent();
					}

				}
			})
		}
	})
}


const reblance = function() {

}
