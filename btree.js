const {DataPage, IdPage, IndexPage, 
	PAGE_TYPE_ID, 
	PAGE_TYPE_INDEX, 
	PAGE_TYPE_ROOT,
	PAGE_TYPE_INTERNAL,
	PAGE_TYPE_LEAF,
    PAGE_SIZE,
	MIN_KEY
} = require('./page.js');
const {compare, IdGen, IdCompare, hash, ByteSize} = require('./utils.js');

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
const ID_BTREE_META_HEADER = 8 + 4 + 1 + 1 + 2 + 1;
const PAGE_NO_BYTES = 4;
/*
* Btree tree的元信息， 占据索引文件的第一个page, 用户索引的key大小不能大于32b
* 其结构为：
*  ---------------------------------------------------------------------------
*             idBtreeMeta | maxPageNo| btreeSize | slotSize | offset
*         [ offset1, offset2, ] ...... [ meta1, meta2, ]
*  ---------------------------------------------------------------------------
*  idBreeMeta(8b)的数据结构：
*    rootPageNo(4b)+workingPageNo(4b)
*  maxPageNo(4b):
*    标识索引文件最大的页码,新建btree Page的时候都会自增1;
*  slotSize(1b)：
*    如果btree有用户的索引,比如有btreeSize = 5, 则slotSize(1b)=2^3;
*  offset(2b):
*    标识meta数组的写的偏移,meta从底部开始写,当btreeSize=0时,offset=1024;
*  Meta 和 IdBtreeMeta都有以上的数据.Meta还有size,nextMetaOffset和keyRaw：
*    rootPageNo(4b)+nextMetaOffset(1b)+size(1b)+keyRaw(nb)
*  nextMetaOffset作为指向下一个冲突的key；meta从buffer的底部开始往上添加
*  [offset...] 和 [meta...]两个形成 BtreeKey的字典;
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
		let slotSize = pageBuffer.readInt8(ID_BTREE_META_HEADER -3);
		if(!slotSize) {
			// 如果slotSize为空, 则置初始值 2^3
			this.slotSize = 8;
			this.data.writeInt8(this.slotSize, ID_BTREE_META_HEADER -3);
		}
		this.offset = this.data.readInt16LE(ID_BTREE_META_HEADER - 2);
		if(this.offset === 0) {
			// 初始状态 offset为最大值
			this.offset = PAGE_SIZE;
			this.data.writeInt16LE(PAGE_SIZE, ID_BTREE_META_HEADER -2);
		}
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

	//======================================================================
	// 以下实现一个简易的基于哈希索引的存储, 用来存储用户定义的索引信息
	// 根据key查找用户设置的btree的rootPageNo, 是hash表查询方式
	getIndexRootPageNo(key) {
		let offset = this.__slotOffset(key);
		if(!offset) {
			return ;
		}
		let metaInfo = this.getIndexMetaInfo(offset);
		if(compare(key, metaInfo.key) === 0) {
			return metaInfo.rootPageNo;
		}
		while(metaInfo.nextOffset) {
			metaInfo = this.getIndexMetaInfo(metaInfo.nextOffset);
			if(compare(key, metaInfo.key) === 0) {
				return metaInfo.rootPageNo;
			}
		}
	}


	getIndexMetaInfo(offset) {
		let dataCopy = Buffer.from(this.data);
		let rootPageNo = dataCopy.readInt32LE(offset - 4);
		let nextOffset = dataCopy.readInt16LE(offset - 6);
		let size = dataCopy.readInt8(offset - 7);
		let keyBuffer = dataCopy.slice(offset - 7 - size, offset - 7);
		return {
			rootPageNo,
			nextOffset,
			size,
			key: keyBuffer.toString()
		}
	}

	__scale() {
		this.slotSize <<= 1;
		let beginOffset = PAGE_SIZE;
		// 先粗暴的将所有的key先收集起来, 再重新添加
		let keys = [];
		while(beginOffset >= this.offset) {
			let rootPageNo = this.data.readInt32LE(beginOffset - 4);
			let keySize = this.data.readInt8(beginOffset - 7);
			let key = this.data.slice(beginOffset - 7 - keySize,
					beginOffset - 7).toString();
			keys.push({
				key,
				rootPageNo
			})
			beginOffset -= (7 + keySize);
		}

		this.btreeSize = 0;
		this.offset = PAGE_SIZE;
		keys.forEach(k=> {
			this.addIndexRootPage(k.key, k.rootPageNo);
		})
	}

	__rewriteNextOffset(offset, newNextOffset) {
		this.data.writeInt16LE(newNextOffset, offset - PAGE_NO_BYTES - 2);
	}

	__free() {
		return this.offset - this.slotSize * 2 - ID_BTREE_META_HEADER;
	}

	// 根据key获取offset槽的value, 返回的offset 大于0,则代表该槽已经被占,否则为空
	__slotOffset(key) {
		let keyCode = hash(key);
		let slotIndex = keyCode & (this.slotSize - 1);
		let offset = this.data.readInt16LE(ID_BTREE_META_HEADER
				+ slotIndex * 2);
		return offset;
	}

	__writeOffsetInSlot(index, offset) {
		this.data.writeInt16LE(offset, ID_BTREE_META_HEADER + index * 2);
	}

	// 在冲突链中查找最后一个节点, 返回该metaInfo
	__findLastMetaInChain(startOffset) {
		let findedOffset = startOffset;
		let metaInfo = this.getIndexMetaInfo(startOffset);
		while(metaInfo.nextOffset) {
			findedOffset = metaInfo.nextOffset;
			metaInfo = this.getIndexMetaInfo(metaInfo.nextOffset);
		}
		return {offset: findedOffset, ...metaInfo}
	}

	writeOneIndexMeta(key, rootPageNo, nextOffset) {
		let keySize = ByteSize(key);
		// key的长度 + rootPage 的长度 + nextOffset 长度 + size
		let oneIndexMetaSize = keySize + PAGE_NO_BYTES + 2 + 1;
		let start = this.offset;
		this.data.writeInt32LE(rootPageNo, start - 4);
		this.data.writeInt16LE(nextOffset, start - 6);
		this.data.writeInt8(keySize, start - 7);
		this.data.write(key, start - oneIndexMetaSize);
		this.btreeSize ++;
		this.data.writeInt8(this.btreeSize,
				ID_BTREE_META_BYTES + PAGE_NO_BYTES);
		this.offset -= (7 + keySize)
	}

	addIndexRootPage(key, rootPageNo) {
		let keyBytes = ByteSize(key);
		// rootPageNo + nextOffset + size + keyRaw
		let needRoom =  4 + 1 + 1 + keyBytes;
		if(this.__free() > needRoom) {
           if(this.btreeSize >= this.slotSize) {
           	   // 先扩容
                this.__scale();
               let keyCode = hash(key);
               let index = keyCode & (this.slotSize - 1);
                this.__writeOffsetInSlot(index, this.offset);
				this.writeOneIndexMeta(key, rootPageNo, 0);
            } else {
           	    let keyCode = hash(key);
           	    let index = keyCode & (this.slotSize - 1);
                let offset = this.__slotOffset(key);
				if(offset) { // 产生冲突
					let metaInfo = this.__findLastMetaInChain(offset);
                    this.__rewriteNextOffset(metaInfo.offset, this.offset);
                    this.writeOneIndexMeta(key, rootPageNo, 0);
				} else {
                    this.__writeOffsetInSlot(index, this.offset);
					this.writeOneIndexMeta(key, rootPageNo, 0);

				}
            }
		} else {
			throw new Error('cannot support too many indexes!');
		}
	}
	//=======================================================================
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
				let parentPage = await IdPage.Load(page.getPageParent());
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
let idArray = [];
let test = async (btree)=> {
	for(var i = 0; i < 101; i ++) {
        id = IdGen();
        idArray.push(id);
        if(i === 100) {
        	await btree.insertId(id, i);
        } else {
        	await btree.insertId(id, i);
        }
	}
	console.log('finished')
}
idBtree.then(btree=> {
	// test(btree).then(()=> {
	// 	console.log('19', idArray[1])
     //    btree.findPageNo(idArray[47]).then(data=> {
     //        console.log('data', data);
     //    })
	// });

	btree.btreeMeta.addIndexRootPage('java', 12345);
	btree.btreeMeta.addIndexRootPage('javanodejs',234);
	btree.btreeMeta.addIndexRootPage('odejs', 78);
	btree.btreeMeta.addIndexRootPage('hello', 89);
	btree.btreeMeta.addIndexRootPage('javahello', 7);
	btree.btreeMeta.addIndexRootPage('jeeee', 78);
	btree.btreeMeta.addIndexRootPage('jjajf;a', 7897);
	btree.btreeMeta.addIndexRootPage('ja;ajff',453);
	btree.btreeMeta.addIndexRootPage('nodjes', 88773);
	btree.btreeMeta.addIndexRootPage(';asfj;af', 988);
	btree.btreeMeta.addIndexRootPage('quiet', 7897);
	console.log('java', btree.btreeMeta.getIndexRootPageNo('nodjes'))
});
