const fs = require('fs');
const path = require('path');

const {DataPage, IdPage, IndexPage,
	PAGE_TYPE_ID, 
	PAGE_TYPE_INDEX, 
	PAGE_TYPE_ROOT,
	PAGE_TYPE_INTERNAL,
	PAGE_TYPE_LEAF,
    PAGE_SIZE,
	INDEXPATH
} = require('./page.js');
const {MIN_KEY, MIN_ID} = require('./constants');
const {compare, IdGen, IdCompare, hash, ByteSize} = require('./utils.js');


const ID_BTREE_META_BYTES = 8;
const ID_BTREE_META_HEADER = 8 + 4 + 1 + 1 + 2;
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
*  btreeSize(1b):
*  	存储btree索引树的size
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
		this.slotSize = pageBuffer.readInt8(ID_BTREE_META_HEADER -3);
		if(!this.slotSize) {
			// 如果slotSize为空, 则置初始值 2^3
			this.slotSize = 8;
		}
        this.data.writeInt8(this.slotSize, ID_BTREE_META_HEADER -3);
		this.offset = this.data.readInt16LE(ID_BTREE_META_HEADER - 2);
		if(this.offset === 0) {
			// 初始状态 offset为最大值
			this.offset = PAGE_SIZE;
			this.data.writeInt16LE(PAGE_SIZE, ID_BTREE_META_HEADER -2);
		}
	}

	static LoadFromDisk(directory) {
		// 读取首页
		let page0Buffer = Buffer.alloc(PAGE_SIZE);
		let filePath = path.join(directory, INDEXPATH);
        return new Promise((resolve, reject)=> {
            fs.open(filePath, 'r', (err, file)=> {
                if(err) {
                    return reject(err);
                }
                fs.read(file, page0Buffer, 0, PAGE_SIZE, 0,
                    (err, data)=> {
                        if(err) {
                            return reject(err);
                        }
                        resolve(new BtreeMeta(page0Buffer));
                    });
            })
        });
	}

	// 返回所有的key
	allKeys() {
        let beginOffset = PAGE_SIZE;
        // 先粗暴的将所有的key先收集起来, 再重新添加
        let keys = [];
        while(beginOffset > this.offset) {
            let rootPageNo = this.data.readInt32LE(beginOffset - 4);
            let keySize = this.data.readInt8(beginOffset - 7);
            let key = this.data.slice(beginOffset - 7 - keySize,
                beginOffset - 7).toString();
            keys.push({
                key,
                rootPageNo
            });
            beginOffset -= (7 + keySize);
        }

        return keys;
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
		this.setMaxPageNo(this.maxPageNo);
		return this.maxPageNo;
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

	setRootPageNo(pageNo) {
		this.idBtreeMeta.rootPageNo = pageNo;
		this.data.writeInt32LE(pageNo, 0);
	}
	getMaxPageNo() {
		return this.maxPageNo;
	}

	//======================================================================
	// 以下实现一个简易的基于哈希索引的存储, 用来存储用户定义的索引信息
	// 根据key查找用户设置的btree的rootPageNo, 是hash表查询方式
	getIndexRootPageNo(key) {
		let metaInfo = this.__getMetaInfoByKey(key);
		if(metaInfo) {
			return metaInfo.rootPageNo;
		}
		return;
	}

	__getMetaInfoByKey(key) {
        let offset = this.__slotOffset(key);
        if(!offset) {
            return ;
        }
        let metaInfo = this.getIndexMetaInfo(offset);
        if(compare(key, metaInfo.key) === 0) {
            return {
	            ... metaInfo,
	            'offset': offset
            };
        }
        while(metaInfo.nextOffset) {
            metaInfo = this.getIndexMetaInfo(metaInfo.nextOffset);
            if(compare(key, metaInfo.key) === 0) {
                return {
	                ... metaInfo,
	                'offset': metaInfo.nextOffset
                };
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
		let keys = this.allKeys();

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

	__writeOneIndexMeta(key, rootPageNo, nextOffset) {
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
		this.offset -= (7 + keySize);
		this.data.writeInt16LE(this.offset, ID_BTREE_META_HEADER - 2);
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
				this.__writeOneIndexMeta(key, rootPageNo, 0);
            } else {
           	    let keyCode = hash(key);
           	    let index = keyCode & (this.slotSize - 1);
                let offset = this.__slotOffset(key);
				if(offset) { // 产生冲突
					let metaInfo = this.__findLastMetaInChain(offset);
                    this.__rewriteNextOffset(metaInfo.offset, this.offset);
                    this.__writeOneIndexMeta(key, rootPageNo, 0);
				} else {
                    this.__writeOffsetInSlot(index, this.offset);
					this.__writeOneIndexMeta(key, rootPageNo, 0);

				}
            }
		} else {
			throw new Error('cannot support too many indexes!');
		}
	}

	__changeRootPageNo(offset, rootPageNo) {
		this.data.writeInt32LE(rootPageNo, offset - 4);
	}

	// 更新btree索引的rootPageNo, btree每当rootPage裂变的时候, 都得更新;
	updateIndexRootPage(rootPageNo, key) {
		let metaInfo = this.__getMetaInfoByKey(key);
		if(!metaInfo) {
			throw new Error(`BtreeMeta cannot update key(${key}) not exists!`);
		}
		this.__changeRootPageNo(metaInfo.offset, rootPageNo);
	}
	//=======================================================================
}
exports.BtreeMeta = BtreeMeta;

class IdBtree {
	constructor(btreeMeta) {
		this.btreeMeta = btreeMeta;
		// IdBtree为空，整个表为空
		if(!this.btreeMeta || this.btreeMeta.isEmpty()) {
            if(!this.btreeMeta) {
                let pageBuffer = Buffer.alloc(PAGE_SIZE);
                this.btreeMeta = new BtreeMeta(pageBuffer);
            }
            let idPage = new IdPage(PAGE_TYPE_ID|PAGE_TYPE_ROOT|PAGE_TYPE_LEAF,
                -1, 1);
            idPage.setPageNo(1);
            this.btreeMeta.setMaxPageNo(1)
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

    static LoadFromScratch() {
		let buffer = Buffer.alloc(PAGE_SIZE);
		let btreeMeta = new BtreeMeta(buffer);

		return new IdBtree(btreeMeta);
	}

	getBtreeMeta () {
		return this.btreeMeta;
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
                // 记录idBtree的rootPage
                this.btreeMeta.setRootPageNo(maxPageNo);

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
                this.btreeMeta.setMaxPageNo(maxPageNo);
                page.setNextPage(maxPageNo, true);
                nextPage.setPrePage(page.getPageNo(), true);
				nextPage.insertCell(id, childPageNo);
				return nextPage.getPageNo();
			}
		}
	}

	async insertId(idInfo, dataPageNo) {
		if(typeof dataPageNo !== 'number') {
			throw new Error('the argument dataPageNo in idBtree.insertId' +
				'must be number!');
		}
		await this.insertRecursily(this.workingPage, {
			id: idInfo,
			childPageNo: dataPageNo
		});

		return
	}

	// 根据id查找DataPage的pageNo
	async findPageNo(idInfo) {
		let leafPage = await this.__diveIntoLeaf(idInfo);
		if(!leafPage) {
			return ;
		}
		let childPageNo = leafPage.getChildPageNo(idInfo);
		return childPageNo;
	}
}

exports.IdBtree = IdBtree;

class IndexBtree {
    constructor(btreeMeta, key, fromDisk) {
        this.btreeMeta = btreeMeta;
        this.key = key;
        if(fromDisk) {
        	let rootPageNo = btreeMeta.getIndexRootPageNo(key);
        	this.rootPageNo = rootPageNo;
        	return;
        }
        this.rootPageNo = btreeMeta.increaseMaxPageNo();
        let indexPage = new IndexPage(
            PAGE_TYPE_INDEX|PAGE_TYPE_ROOT|PAGE_TYPE_LEAF,
	        -1, this.rootPageNo);
        btreeMeta.addIndexRootPage(key, this.rootPageNo);
        this.rootPage = indexPage;
    }

    async loadRootPage() {
    	let rootPageNo = this.rootPageNo;
    	let page = await IndexPage.LoadPage(rootPageNo);
    	this.rootPage = page;
    	return this;
    }

    getRootPage() {
        return this.rootPage;
    }

    updateRootPageNo(rootPageNo) {
        this.rootPageNo = rootPageNo;
        this.btreeMeta.updateIndexRootPage(rootPageNo);
    }

    async insertKey(key, id) {
        let deepestPage = await this.walkDeepest(key);

        let hasRoom = deepestPage.hasRoomFor(key);
        console.log('hasRoom', hasRoom, 'no', key,
	        deepestPage.getPageNo());
        if(hasRoom) {
            deepestPage.insertCell(key, id, 0);
            return ;
        }
        await this.rebalance(deepestPage, {key, id, childPageNo: 0});
    }

    async findId(key) {
    	let startPage = this.rootPage;
        let cellInfo = startPage.__findNearestCellInfo(key);
        if(cellInfo && compare(cellInfo.key, key) === 0) {
            return cellInfo.id
        }
        console.log('cellInfo+++++++++++++++++++++++++++++++', cellInfo);
        while(cellInfo.childPageNo > 0) {
            let childPage = await IndexPage.LoadPage(cellInfo.childPageNo);
            console.log('childPage', childPage);
            cellInfo = childPage.__findNearestCellInfo(key);
            console.log('cellINfo', cellInfo);
            if(cellInfo && compare(cellInfo.key, key) === 0) {
                return cellInfo.id
            }
        }
    }

    async walkDeepest(key) {
        let startPage = this.rootPage;
        while(!(startPage.getType() & PAGE_TYPE_LEAF)){
            startPage = await startPage.getChildPage(key);
        }
        return startPage;
    }

    async rebalance(startPage, indexInfo) {
    	if(startPage.getPageNo() === 0) {
    		console.log('dddd')
	    }
        if(startPage.hasRoomFor(indexInfo.key)) {
            startPage.insertCell(
                indexInfo.key,
                indexInfo.id,
                indexInfo.childPageNo
            );
            return startPage.getPageNo();
        } else {
            let splices = startPage.half(indexInfo);
            let middleCellInfo = splices.shift();
            if(!middleCellInfo) {
            	console.log('ddd')
            }
            let pageType = PAGE_TYPE_INDEX;
            if(startPage.isLeaf()) {
                pageType |= PAGE_TYPE_LEAF;
            } else {
                pageType |= PAGE_TYPE_INTERNAL;
            }

            if(startPage.isRoot()) {
				let splitPageNo = this.btreeMeta.increaseMaxPageNo();
				let rootPageNo = this.btreeMeta.increaseMaxPageNo();
                startPage.setType(pageType);
                startPage.setParentPage(rootPageNo);
				let splitPage = new IndexPage(pageType, rootPageNo,
						splitPageNo);
                let rootNewPage = new IndexPage(
                	(PAGE_TYPE_INDEX | PAGE_TYPE_ROOT), null, rootPageNo);

                splices.forEach(s=> {
                    splitPage.insertCell(s.key, s.id, s.childPageNo);
                });

                rootNewPage.insertCell(MIN_KEY, MIN_ID, startPage.getPageNo());
                middleCellInfo.childPageNo = splitPageNo;
                rootNewPage.insertCell(
                    middleCellInfo.key,
                    middleCellInfo.id,
                    middleCellInfo.childPageNo
                );
                this.rootPage = rootNewPage;
                this.btreeMeta.updateIndexRootPage(rootNewPage.getPageNo(),
	                    this.key);
                if(IdCompare(indexInfo.key, middleCellInfo.key) >= 0) {
                	return splitPageNo;
                }
                return startPage.getPageNo();
            } else {
                let parentPage = await startPage.getPageParent();
                let splitPageNo = this.btreeMeta.increaseMaxPageNo();
                middleCellInfo.childPageNo = splitPageNo;
                let splitParentNo = await this.rebalance(parentPage, middleCellInfo);
                let splitPage = new IndexPage(pageType, splitParentNo,
	                    splitPageNo);
                splices.forEach(s=> {
                	splitPage.insertCell(s.key, s.id, s.childPageNo);
                });
                if(IdCompare(indexInfo.key, middleCellInfo.key) >= 0) {
                	return splitPageNo;
                }
                return startPage.getPageNo();
            }
        }
    }
}

exports.IndexBtree = IndexBtree;

