const {DataPage, IdPage, IndexPage, 
	PAGE_TYPE_ID, 
	PAGE_TYPE_INDEX, 
	PAGE_TYPE_ROOT,
	PAGE_TYPE_INTERNAL,
	PAGE_TYPE_LEAF
} = require('./page.js');
const {compare} = require('./utils.js');
const MIN_KEY = '-1'

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
const insertrecursively = function(idPage, pageData) {
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

const rootPage = new IndexPage(null, 0, 
	PAGE_TYPE_INDEX |PAGE_TYPE_ROOT | PAGE_TYPE_LEAF);
let IndexPageNo = 0;

const insertKey = function(key, id, childPageNo) {
	let deepestPage = walkDeepest(key);
	let hasRoom = deepestPage.hasRoomFor(key);
	console.log('hasRoom', hasRoom, 'no', deepestPage.getPageNo())
	if(hasRoom) {
		deepestPage.insertCell(key, id, childPageNo);
		return ;
	}
	reblance(deepestPage, {key, id, childPageNo});
}

const deleteKey = function(key) {

}

const walkDeepest = async(key)=> {
	let startPage = rootPage;
	while(! startPage.getType() & PAGE_TYPE_LEAF){
		startPage = await startPage.getChildPage(key);
	}
	return startPage;
}

const reblance = function(startPage, indexInfo) {
	if(startPage.hasRoomFor(indexInfo.key)) {
		startPage.insertCell(
			indexInfo.key,
			indexInfo.id,
			indexInfo.childPageNo
		);
	} else {
		let splices = startPage.half(indexInfo);
		let middleCellInfo = splices.shift();
		let splitPage = new IndexPage(rootPage, ++IndexPageNo, 
			(PAGE_TYPE_INDEX | PAGE_TYPE_INTERNAL));
		middleCellInfo.childPageNo = splitPage.getPageNo();

		if(startPage.isRoot()) {
			let rootPageNo = ++IndexPageNo;
			let rootNewPage = new IndexPage(null, rootPageNo, 
				(PAGE_TYPE_INDEX | PAGE_TYPE_ROOT));

			splices.forEach(s=> {
				splitPage.insertCell(s.key, s.id, s.childPageNo);
			});

			rootNewPage.insertCell(MIN_KEY, null, startPage.getPageNo());
			console.log('middleCellInfo')
			rootNewPage.insertCell(
				middleCellInfo.key,
				middleCellInfo.id,
				middleCellInfo.childPageNo
			);
		} else {
			let parentPage = startPage.getPageParent();
			reblance(parentPage, middleCellInfo);
		}
	}
}

const keys = [1, 3, 4,  8, 33, 234, 256, 432]
const diveIntoLeaf = function(key) {
	let page = rootPage;
	let min = 0, max = keys.length;

	while(max > min) {
		if(keys[min] >= key) {
			return min;
		}
		if(keys[max] <= key) {
			return max;
		}
		if(keys[min] < key && keys[min + 1] > key) {
			return min;
		}
		let middle = (min + max) >>1;
		if(keys[middle] > key) {
			max = middle;
		}
		if(keys[middle] === key) {
			return middle;
		}
		if(keys[middle] < key) {
			min = middle;
		}
	}
}
console.log("diveIntoLeaf", diveIntoLeaf(80))

var keyss = ['java', 'nodejs', 'eclipse', 'webstorm', 'c', 'go', 'window', 'linux', 'mac', 'blockchain']
for(var i = 1; i < 10; i ++) {
	insertKey(keyss[i], i, i*10);
}
console.log(rootPage)

console.log('window:', rootPage.findId('eclipse'))

