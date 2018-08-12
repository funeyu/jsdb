const {DataPage, IdPage} = require('./page.js');
const {compare} = require('./utils.js');

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
let currentLeafIdNo, rootIdPageNo;
IdPage.getLeafNo().then(leafNo=> {
	currentLeafIdNo = leafNo;
})

IdPage.getRootPage().then(rootNo=> {
	rootIdPageNo = rootNo;
})

// pageData: {id: xxx, pageNo: xxxx}
const insertrecursively(idPage, pageData) {
	let {id, pageNo} = pageData

	let result = idPage.insertCell(id, pageNo);
	if(result === 'FULL') {
		if(idPage.isRoot()) {
			let newIdPage = new IdPage(null, ++currentIdPageNo);
			newIdPage.setRoot(true);
			newIdPage.insertCell(id, pageNo);

			IdPage.setRootPage(newIdPage.pageNo);
		}

		IdPage.load(idPage.getPageParent(), function(parentPage) {
			let newIdPage = new IdPage(null, ++currentIdPageNo);
			if(parentPage.isRoot()) {
				// mark this idPage as root page
				newIdPage.setRoot(true);
				newIdPage.insertCell(id, pageNo);
				IdPage.setRootPage(newIdPage.pageNo);
			} else {
				insertrecursively(parentPage, {
					id: pageData.id, 
					pageNo: parentPage.pageNo
				})
			}
		});
	}
	else if(result === 'LINK') {
		let newIdPage = new IdPage(idPage.getPageParent(), ++currentIdPageNo);
		newIdPage.insertCell(pageData.id, pageData.pageNo);
		
		newIdPage.setPre(idPage);
		idPage.setNext(newIdPage);
	}
}

const insertData = function(data) {
	let id = IdGen();

	DataPage.load(currentDataPageNo, function(page) {
		let result = page.insertCell(id, data);
		if(result === 'FULL') {
			page.flush();
			let pageNo = currentDataPageNo;
			currentDataPageNo ++;

			let dataPage = new DataPage(currentDataPageNo);
			// to do: if the data large than DataPage size
			dataPage.insertCell(id, data);

			IdPage.load(currentLeafIdNo, function(idLeafPage) {
				insertrecursively(idLeafPage, {id, pageNo});
			})
		}
	})
}

const getDataById = function(id, cb, idPageNo) {
	idPageNo = idPageNo || rootIdPageNo;
	IdPage.load(idPageNo, function(idPage) {
		let childPageNo = idPage.getChildPageNo(id);
		if(idPage.isLeaf()) {
			DataPage.load(childPageNo, function(dataPage) {
				cb(null, dataPage);
			});
		} else {
			getDataById(id, cb, childPageNo);
		}
	});
}


const reblance = function() {

}
