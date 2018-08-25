const {DataPage, IdPage, IndexPage, 
	PAGE_TYPE_ID, 
	PAGE_TYPE_INDEX, 
	PAGE_TYPE_ROOT,
	PAGE_TYPE_INTERNAL,
	PAGE_TYPE_LEAF,
    PAGE_SIZE,
	MIN_KEY
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

// keyvar keyss = ['java', 'nodejs', 'eclipse', 'webstorm', 'c', 'go', 'window', 'linux', 'mac', 'blockchain']
// var keys = [];
// for(var i = 0; i < 100; i ++) {
// 	keyss.forEach(k=> keys.push(k + i));
// }
// var test = async()=> {
//     for(var i = 0; i < 56; i ++) {
//     	if(i>=55) {
//     		await insertKey(keys[i], i, i*10 + 1);
// 		} else {
//             await insertKey(keys[i], i, i*10 + 1);
// 		}
//     }
// }
// test().then(()=> {
// 	rootPage.findId('eclipse3').then((data)=> {
// 		console.log('findId', data);
// 	})
// })

const ID_BTREE_META_BYTES = 8;
const ID_BTREE_META_HEADER = 8 + 4 + 1;
const PAGE_NO_BYTES = 4;
/*
* Btree tree的元信息， 占据索引文件的第一个page, 用户索引的key大小不能大于32b
* 其结构为：
*  ---------------------------------------------------------------------------
*             idBtreeMeta | maxPageNo| btreeSize |
*         [ offset1, offset2, ] ...... [ meta1, meta2, ]
*  ---------------------------------------------------------------------------
*  idBreeMeta的数据结构：
*   rootPageNo(4b)+workingPageNo(4b)
*  maxPageNo:
*   标识索引文件最大的页码,新建btree Page的时候都会自增1;
*  btreeSize：
*   如果btree有用户的索引,比如有5个定义的索引, 则btreeSize=2^3
*  Meta 和 IdBtreeMeta都有以上的数据.Meta还有size,nextMetaOffset和keyRaw：
*   size(1b)+rootPageNo(4b)+workingPageNo(4b)+nextMetaOffset(2b)+keyRaw(nb)
*  作为指向下一个冲突的key；meta从buffer的底部开始往上添加
*  [offset...] 和 [meta...]两个形成 BtreeKey的字典
* */
class BtreeMeta {
	// 直接传递一块page大小buffer，包含所有的BtreeMeta
	constructor(pageBuffer) {
		this.data = pageBuffer;

		let idBtreeMetaRootPageNo = pageBuffer.readInt32LE(0);
		let idBtreeMetaWorkingPageNo = pageBuffer.readInt32LE(4);
		this.idBtreeMeta = {
			rootPageNo: idBtreeMetaRootPageNo,
			workingPageNo: idBtreeMetaWorkingPageNo,
		};
		this.maxPageNo = pageBuffer.readInt32LE(ID_BTREE_META_BYTES);
		this.btreeSize = pageBuffer.readInt8(ID_BTREE_META_BYTES
				+ PAGE_NO_BYTES);
	}

	idWorkingOnPageNo() {
		return this.idBtreeMeta.workingPageNo;
	}

	idRootPageNo() {
		return this.idBtreeMeta.rootPageNo;
	}

	isEmpty() {
		return this.btreeSize < 1;
	}

	setBtreeSize(size) {
		this.btreeSize = size;
		this.data.writeInt8(size, ID_BTREE_META_BYTES + PAGE_NO_BYTES);
		return this;
	}

	setMaxPageNo(pageNo) {
		this.maxPageNo = pageNo;
		this.data.writeInt32LE(pageNo, ID_BTREE_META_BYTES);
		return this;
	}

	increaseMaxPageNo() {
		this.maxPageNo++;
		return this.setMaxPageNo(this.maxPageNo);
	}

	setIdBtreeMeta(idBtreeMeta) {
		this.idBtreeMeta = idBtreeMeta;
		this.data.writeInt32LE(idBtreeMeta.rootPageNo, 0);
		this.data.writeInt32LE(idBtreeMeta.workingPageNo, 4);
		return this;
	}

	setIdBtreeWorkingPage(pageNo) {
		this.idBtreeMeta.workingPageNo = pageNo;
		this.data.writeInt32LE(pageNo, 4);
	}
	getMaxPageNo() {
		return this.maxPageNo;
	}
}
class IdBtree {
	constructor(btreeMeta) {
		this.btreeMeta = btreeMeta;
		// IdBtree为空，整个表为空
		if(this.btreeMeta.isEmpty()) {
			let idPage = new IdPage(PAGE_TYPE_ID|PAGE_TYPE_ROOT|PAGE_TYPE_LEAF,
					-1, 1);
			btreeMeta.setMaxPageNo(1)
					 .setIdBtreeMeta({
						 rootPageNo: 1,
						 workingPageNo: 1
					 });
			this.workingPage = this.rootPage = idPage;
			return Promise.resolve(this);
		} else {
			let rootPageNo = btreeMeta.idRootPageNo();
			let workingPageNo = btreeMeta.idWorkingOnPageNo();
			return  Promise.all([
				IdPage.Load(rootPageNo),
				IdPage.Load(workingPageNo)
			]).then((pages)=> {
				this.rootPage = pages[0];
				this.workingPage = pages[1];
				return this;
			})
		}
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

	async insertRecursily(page, insertInfo) {
		let {id, childPageNo} = insertInfo;
		let insertResult = page.insertCell(id, childPageNo);
		if(insertResult) {
			return page.getPageNo();
		} else {
			let maxPageNo = this.btreeMeta.getMaxPageNo();
			let pageType = PAGE_TYPE_ID;
			if(page.isLeaf()) {
				pageType = pageType | PAGE_TYPE_LEAF;
			} else {
				pageType = pageType | PAGE_TYPE_INTERNAL
			}
			if(page.isRoot()) {
				maxPageNo ++;
				let idRootPage = new IdPage(PAGE_TYPE_ID|PAGE_TYPE_ROOT, -1,
						maxPageNo);
				this.rootPage = idRootPage;
				let minIdInfo = page.getMinIdInfo();
				idRootPage.insertCell(minIdInfo.id, page.getPageNo());
				maxPageNo++;
				let nextPage = new IdPage(pageType, idRootPage.getPageNo(),
						maxPageNo);
                idRootPage.insertCell(id, maxPageNo);
				page.setNextPage(maxPageNo, true);
                nextPage.insertCell(id, childPageNo);
				nextPage.setPrePage(page.getPageNo(), true);
				// 如果是叶节点,则需要记录workingPage
				if(page.isLeaf()) {
					// maxPageNo为nextPage的页码
					this.btreeMeta.setIdBtreeWorkingPage(maxPageNo);
					this.workingPage = nextPage;
				}
				this.btreeMeta.setMaxPageNo(maxPageNo);

				return nextPage.getPageNo();
			} else {
				maxPageNo ++;
				let parentPage = await IdPage.Load(page.getPageNo());
				let pageNo = await this.insertRecursily(parentPage, {
					id: id,
					childPageNo: maxPageNo
				});
				let nextPage = new IdPage(pageType, pageNo, maxPageNo);
                if(page.isLeaf()) {
					this.btreeMeta.setIdBtreeWorkingPage(maxPageNo);
					this.workingPage = nextPage;
                }
                page.setNextPage(maxPageNo);
                nextPage.setPrePage(page.getPageNo());
				nextPage.insertCell(id, childPageNo);
				return nextPage.getPageNo();
			}
		}
	}

	async insertId(idInfo, dataPageNo) {
		return this.insertRecursily(this.workingPage, {
			id: idInfo,
			childPageNo: dataPageNo
		})
	}

	// 查找DataPage的pageNo
	async findPageNo(idInfo) {
		let leafPage = await this.__diveIntoLeaf(idInfo);
		if(!leafPage) {
			return ;
		}
		let childPageNo = leafPage.getChildPageNo(idInfo);
		return childPageNo;
	}
}

console.log('// test\n' +
    '//====================================================================');
let btreeMeta = new BtreeMeta(Buffer.alloc(PAGE_SIZE));
let idBtree = new IdBtree(btreeMeta);
let id;
let test = async (btree)=> {
	for(var i = 0; i < 120; i ++) {
        id = IdGen();
        if(i === 101) {
        	await btree.insertId(id, 1);
        } else {
        	await btree.insertId(id, 1);
        }
	}
}
idBtree.then(btree=> {
	test(btree).then(()=> {
        btree.findPageNo(id).then(data=> {
            console.log('data', data);
        })
	});
});
