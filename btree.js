const {DataPage, IdPage, IndexPage, 
	PAGE_TYPE_ID, 
	PAGE_TYPE_INDEX, 
	PAGE_TYPE_ROOT,
	PAGE_TYPE_INTERNAL,
	PAGE_TYPE_LEAF
} = require('./page.js');
const {compare, IdGen, IdCompare} = require('./utils.js');

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

let rootPage = new IndexPage(null, 0,
	PAGE_TYPE_INDEX |PAGE_TYPE_ROOT | PAGE_TYPE_LEAF);
let IndexPageNo = 0;

const insertKey = async(key, id, childPageNo)=> {
	let deepestPage = await walkDeepest(key);

	let hasRoom = deepestPage.hasRoomFor(key);
	console.log('hasRoom', hasRoom, 'no', deepestPage.getPageNo())
	if(hasRoom) {
		deepestPage.insertCell(key, id, 0);
		return ;
	}
	rebalance(deepestPage, {key, id, childPageNo: 0});


}

const deleteKey = function(key) {

}

const walkDeepest = async(key)=> {
	let startPage = rootPage;
	while(!(startPage.getType() & PAGE_TYPE_LEAF)){
		startPage = await startPage.getChildPage(key);
	}
	return startPage;
}

const rebalance = function(startPage, indexInfo) {
	if(startPage.hasRoomFor(indexInfo.key)) {
		startPage.insertCell(
			indexInfo.key,
			indexInfo.id,
			indexInfo.childPageNo
		);
	} else {
		let splices = startPage.half(indexInfo);
		let middleCellInfo = splices.shift();
		let pageType = PAGE_TYPE_INDEX
		if(startPage.isLeaf()) {
			pageType |= PAGE_TYPE_LEAF;
		} else {
			pageType |= PAGE_TYPE_INTERNAL;
		}

		let splitPage = new IndexPage(startPage.getParentPageNo(), ++IndexPageNo, pageType);
		middleCellInfo.childPageNo = splitPage.getPageNo();

		if(startPage.isRoot()) {
			startPage.setType(pageType);

			let rootPageNo = ++IndexPageNo;
			let rootNewPage = new IndexPage(null, rootPageNo, 
				(PAGE_TYPE_INDEX | PAGE_TYPE_ROOT));

			splices.forEach(s=> {
				splitPage.insertCell(s.key, s.id, s.childPageNo);
			});

			rootNewPage.insertCell(MIN_KEY, null, startPage.getPageNo());
			rootNewPage.insertCell(
				middleCellInfo.key,
				middleCellInfo.id,
				middleCellInfo.childPageNo
			);
			rootPage = rootNewPage
		} else {
			let parentPage = startPage.getPageParent();
			rebalance(parentPage, middleCellInfo);
		}
	}
}

const insert = function(data, ... key) {

}

var keyss = ['java', 'nodejs', 'eclipse', 'webstorm', 'c', 'go', 'window', 'linux', 'mac', 'blockchain']
var keys = [];
for(var i = 0; i < 100; i ++) {
	keyss.forEach(k=> keys.push(k + i));
}
var test = async()=> {
    for(var i = 0; i < 56; i ++) {
    	if(i>=55) {
    		await insertKey(keys[i], i, i*10 + 1);
		} else {
            await insertKey(keys[i], i, i*10 + 1);
		}
    }
}
test().then(()=> {
	rootPage.findId('eclipse3').then((data)=> {
		console.log('findId', data);
	})
})


class IdBtree {
	constructor(rootPageNo, workingPageNo) {
        this.rootPageNo = rootPageNo;
        this.workingPageNo = workingPageNo;
        return Promise.all([
        	IdPage.Load(rootPageNo),
	        IdPage.Load(workingPageNo)
        ]).then((pages)=> {
        	this.rootPage = pages[0];
        	this.workingPage = pages[1];
        	return this;
        })
    }

	async __diveIntoLeaf(idInfo) {
		let startPage = this.rootPage;
		while(!startPage.isLeaf()) {
			let childPageNo = startPage.getChildPageNo(idInfo);
			if(childPageNo) {
				startPage = await IdPage.Load(childPageNo);
			} else {
				return ;
			}
		}

		return startPage;
	}

	insertId(idInfo, childPageNo) {

	}
}

