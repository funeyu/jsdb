const fs = require('fs');
const assert = require('assert');
const path = require('path');
const util = require('util');

const Lru = require('lru-cache');
const {compare, ByteSize, IdGen, IdCompare,
	CreateFileIfNotExist} = require('./utils.js');

const PAGE_SIZE = 1024;
exports.PAGE_SIZE = PAGE_SIZE;
const RECORD_ID_BYTES_SIZE = 6;
const FILEPATH = 'js.db';
const INDEXPATH = 'js.index';
exports.FILEPATH = FILEPATH;
exports.INDEXPATH = INDEXPATH;
const PAGE_TYPE_ID = 1;
const PAGE_TYPE_LEAF = 1 << 1;
const PAGE_TYPE_INTERNAL = 1 << 2;
const PAGE_TYPE_ROOT = 1 << 3;
const PAGE_TYPE_INDEX = 1 << 4;
const PAGE_TYPE_REUSE = 1 << 5;
const PAGE_TYPE_SIZE = 1;     // the byteSize of the type

const cache = Lru(128 * 1024);
const DATACACHE = Lru(64 * 64 * 32);
const ID_CELL_BYTES_SIZE = 8;
const DATA_PAGE_HEADER_BYTES_SIZE = 8;
const DATA_CELL_MAX_SIZE = 256;
const DATA_CELL_SIZE_BYTES = 1;
const OFFSET_BYTES_SIZE = 2;

class DataPage {
	/*
	* B-tree:
	*  the data format is :
	*  -------------------------------------------------------------
	*   pageNo | size | offset | [idCell1...] | [dataCell1 ...]
	*  -------------------------------------------------------------
	*  idCell format is:
	*	idBuffer + dataOffset
	*
	*  dataCell format is :
	*	cellByteSize + rawData
	*
	*  the size of the field above:
	*  pageNo: 4b
	*  size:   2b
	*  offset: 2b
	*  idBuffer:     6b
	*  dataOffset: 	 2b
	*  cellByteSize: 1b
	*  rawData: nb (n <= 256)
	* */
	constructor(pageNo) {
		this.pageNo = pageNo;      	// start from zero
		this.data = Buffer.alloc(PAGE_SIZE);
		this.data.writeInt32LE(pageNo, 0);
		this.size = 0;				// the size of data
		this.offset = PAGE_SIZE;  	// write data from bottom up

		DATACACHE.set(pageNo, this);
	}
	
	freeData() {
		return (this.offset - DATA_PAGE_HEADER_BYTES_SIZE
			-this.size * ID_CELL_BYTES_SIZE);
	}

	getPageNo() {
		return this.pageNo;
	}

	insertCell(id, cellData) {
		let dataSize = ByteSize(cellData) + DATA_CELL_SIZE_BYTES;
		let needByte = dataSize + ID_CELL_BYTES_SIZE;
		if((dataSize - DATA_CELL_SIZE_BYTES) >= DATA_CELL_MAX_SIZE) {
			throw new Error('cannot insert data size large than 256 b!')
		}
		let freeDataSize = this.freeData();
		if(this.freeData() > needByte) {
			// write the cellData's size
			this.data.writeInt16LE(dataSize - DATA_CELL_SIZE_BYTES,
				this.offset - dataSize);
			// write the cellData
			this.data.write(cellData, this.offset - dataSize +
				DATA_CELL_SIZE_BYTES);
			this.offset = this.offset - dataSize;

			this.data.writeInt32LE(id.timeId,
				this.size * ID_CELL_BYTES_SIZE + DATA_PAGE_HEADER_BYTES_SIZE);
			this.data.writeInt16LE(id.count,
				this.size * ID_CELL_BYTES_SIZE +
				DATA_PAGE_HEADER_BYTES_SIZE + 4);
			this.data.writeInt16LE(this.offset,
				this.size * ID_CELL_BYTES_SIZE +
				DATA_PAGE_HEADER_BYTES_SIZE + 6);
			this.size ++;
			// write size
			this.data.writeInt16LE(this.size, PAGENO_BYTES);
			// write offset
			this.data.writeInt16LE(this.offset,
					PAGENO_BYTES + OFFSET_BYTES_SIZE);
			return true;
		} else {		// has no room for cellData
			return false;
		}
	}

	__formId(index) {
		let start = DATA_PAGE_HEADER_BYTES_SIZE + index * ID_CELL_BYTES_SIZE;
		let timeId = this.data.readInt32LE(start);
		let count = this.data.readInt16LE(start + 4);
		let offset = this.data.readInt16LE(start + 4 + 2);
		return {timeId, count, offset}
	}

	__getDataByOffset(offset) {
		let dataSize = this.data.readInt8(offset);
		let copyData = Buffer.from(this.data);
		let buffer = copyData.slice(offset + 1, offset + 1 + dataSize);
		return buffer.toString();
	}

	getCellData(idInfo) {
        let size = this.data.readInt16LE(PAGENO_BYTES);

        let maxIdInfo = this.__formId(size - 1);
        let minIdInfo = this.__formId(0);

        if(IdCompare(idInfo, maxIdInfo) > 0
	            || IdCompare(idInfo, minIdInfo) < 0) {
            console.log('no data matched id:', idInfo);
        } else if(IdCompare(idInfo, maxIdInfo) === 0) {
        	return this.__getDataByOffset(maxIdInfo['offset']);
        } else if(IdCompare(idInfo, minIdInfo) === 0) {
        	return this.__getDataByOffset(minIdInfo['offset']);
        }
        else {
            let max= size - 1, min = 0;
            while(max > min) {
            	let minIdInfo = this.__formId(min);
            	let maxIdInfo = this.__formId(max);
            	if(IdCompare(idInfo, minIdInfo) === 0) {
            		return this.__getDataByOffset(minIdInfo['offset']);
				}
				if(IdCompare(idInfo, maxIdInfo) === 0) {
            		return this.__getDataByOffset(maxIdInfo['offset']);
				}
				if((max - min) === 1) {
            		return;
				}
            	let middle = (max + min) >> 1;
            	let midIdInfo = this.__formId(middle);
            	if(IdCompare(idInfo, midIdInfo) === 0) {
            		return this.__getDataByOffset(midIdInfo['offset']);
				} else if(IdCompare(idInfo, midIdInfo) > 0) {
            		min = middle
				} else {
					max = middle
                }
			}
        }
	}

	flush(directory) {
        let filePath = path.join(directory, FILEPATH);
		return new Promise((resolve, reject)=> {
            fs.open(filePath, 'a', (err, file)=> {
                if(err) {
                	return reject(err)
				}
                fs.write(file, this.data, 0, PAGE_SIZE,
	                    this.pageNo * PAGE_SIZE, (err, data)=> {
                			if(err) {
                				return reject(err);
							}
							resolve();
					});
            });
		})
	}

	setPageNo(pageNo) {
		this.pageNo = pageNo;
		return this;
	}

	setSize(size) {
		this.size = size;
		return this;
	}

	setOffset(offset) {
		this.offset = offset;
		return this;
	}

	// header组成：pageNo(4b) + size(2b) + offset(2b)
	__initPage() {
		let pageNo = this.data.readInt32LE(0);
		let size = this.data.readInt16LE(4);
		let offset = this.data.readInt16LE(6);

		this.setPageNo(pageNo)
			.setSize(size)
			.setOffset(offset);
	}

	static async Load(directory, pageNo) {
		let filePath = path.join(directory, FILEPATH);
		if(!fs.existsSync(filePath)) {
			throw new Error('DataPage.load(), direcotry:%s not exits',
					filePath);
		}
		let page = DATACACHE.get(pageNo);
		if(page) {
			return page;
		}

		page = new DataPage(pageNo);
		let loadFromDisk = new Promise((resolve, reject)=> {
			fs.open(filePath, 'r', (err, file)=> {
				if(err) {
					return reject(err);
				}
				fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
					(err, data)=> {
						if(err) {
							return reject(err);
						}
						page.__initPage();
						DATACACHE.set(pageNo, page);
						resolve(page);
					});
			})
		});

		let filledPage = await loadFromDisk;
		return filledPage;
	}

	static InitFile(directory) {
		let filePath = path.join(directory, FILEPATH);
		CreateFileIfNotExist(filePath);
	}

	static MaxPageNo(directory) {
		let filePath = path.join(directory, FILEPATH);
		if(fs.existsSync(filePath)) {
            const stat = fs.statSync(filePath);
            return stat.size / PAGE_SIZE;
        }
        return 0;
	}

	static CachePage() {
		return DATACACHE;
	}
}

class KeyPage {

}
const ONE_ID_CELL_BYTES = 10;
const PAGENO_BYTES = 4;
const SIZENO_BYTES_IN_CELL = 2;
const ID_BYTES = 6;
const ID_HEADER_PAGE_BYTES = 1 + PAGENO_BYTES * 4 + SIZENO_BYTES_IN_CELL;
class IdPage {
	/*
		the format of this kind page is :
		--------------------------------------------------------------------
		| type |  pageParent  |  size  |  pageNo | prePageNo, nextPageNo |
		[cell1, cell2, cell3, cell4 ...]
		--------------------------------------------------------------------
		every cell is : childPageNo + id

		the size of the filed above is:
		type：       1b
		pageParent:  4b
		size      :  2b
		pageNo    :  4b
		prePageNo:   4b
		nextPageNo:  4b
		childPageNo: 4b
		id:          6b
	*/
	constructor(type, pageParent, pageNo) {
        this.data = Buffer.alloc(PAGE_SIZE);
		this.type = type;
		if(typeof pageParent === 'number') {
			this.pageParent = pageParent;
            this.data.writeInt32LE(pageParent, PAGE_TYPE_SIZE);
		}

		if(typeof pageNo === 'number') {
			this.pageNo = pageNo;
			this.data.writeInt32LE(pageNo, PAGE_TYPE_SIZE + PAGENO_BYTES
				+ SIZENO_BYTES_IN_CELL);
			cache.set(pageNo, this);
		}

		type && this.data.writeInt8(type, 0);
		this.size = 0;
		this.data.writeInt16LE(this.size,  PAGE_TYPE_SIZE + PAGENO_BYTES);
	}

	static InitFile(directory) {
		let filePath = path.join(directory, INDEXPATH);
		CreateFileIfNotExist(filePath);
	}

    // needStore 标识是否要回写this.data
	setType(type, needStore) {
		this.type = type;
		needStore && this.data.writeInt8(type, 0);
		return this;
	}

	setPageParent(pageNo, needStore) {
		this.pageParent = pageNo;
		let start = PAGE_TYPE_SIZE;
		needStore && this.data.writeInt32LE(pageNo, start);
		return this;
	}

	setSize(size, needStore) {
		this.size = size;
		let start = PAGE_TYPE_SIZE + PAGENO_BYTES;
		needStore && this.data.writeInt16LE(size, start);
		return this;
	}

    setPageNo(pageNo, needStore) {
        this.pageNo = pageNo;
        let start = PAGE_TYPE_SIZE + PAGENO_BYTES + SIZENO_BYTES_IN_CELL;
        needStore && this.data.writeInt32LE(pageNo, start);
        return this;
    }

	setPrePage(prePageNo, needStore) {
		this.prePageNo = prePageNo;
		let start = PAGE_TYPE_SIZE + PAGENO_BYTES * 2 + SIZENO_BYTES_IN_CELL;
		needStore && this.data.writeInt32LE(prePageNo, start);
		return this;
	}

	setNextPage(nextPageNo, needStore) {
		this.nextPageNo = nextPageNo;
		let start = PAGE_TYPE_SIZE + PAGENO_BYTES * 3 + SIZENO_BYTES_IN_CELL;
		needStore && this.data.writeInt32LE(nextPageNo, start);
		return this;
	}

    getPageNo() {
        return this.pageNo;
    }

    getPageParent() {
        return this.pageParent;
    }
    getSize() {
        return this.size;
    }
    isLeaf() {
        return this.type & PAGE_TYPE_LEAF;
    }

    isRoot() {
        return this.type & PAGE_TYPE_ROOT;
    }

	__getCellInfoByIndex(index) {
        let cellByteSize = this.size * ONE_ID_CELL_BYTES;
        // sizeOf(type) +
        // sizeOf(paegParent, pageNo, prePage, nextPage) + sizeOf(size)
        let cellByteBegin = PAGE_TYPE_SIZE +
            PAGENO_BYTES * 4 + SIZENO_BYTES_IN_CELL;

        let timeId = this.data.readInt32LE(cellByteBegin +
	            index * ONE_ID_CELL_BYTES);
        let count = this.data.readInt16LE(cellByteBegin +
	            index * ONE_ID_CELL_BYTES + 4);
		let childPageNo = this.data.readInt32LE(cellByteBegin +
				index * ONE_ID_CELL_BYTES + 4 + 2);

		return {
			id: {timeId, count},
			childPageNo
		}
	}

	getChildPageNo(id) {

		let minIndex = 0, maxIndex = this.size - 1;
		let minCellInfo = this.__getCellInfoByIndex(minIndex),
			maxCellInfo = this.__getCellInfoByIndex(maxIndex);

		if(IdCompare(minCellInfo.id, id) > 0) {
			return ;
		}
		// rootPage只有一个cell, 满足该rootPage's id小于id,直接返回
		if(this.size === 1) {
			return minCellInfo.childPageNo;
		}
		if(this.isLeaf() && IdCompare(maxCellInfo.id, id) < 0) {
			return ;
		}

		while(maxIndex > minIndex) {
            minCellInfo = this.__getCellInfoByIndex(minIndex);
            maxCellInfo = this.__getCellInfoByIndex(maxIndex);
			if(IdCompare(minCellInfo.id, id) === 0) {
				return minCellInfo.childPageNo;
			}
			if(IdCompare(maxCellInfo.id, id) <= 0) {
				return maxCellInfo.childPageNo;
			}
			if((maxIndex - minIndex) === 1) {
				if(IdCompare(minCellInfo.id, id) < 0
					&& IdCompare(maxCellInfo.id, id) > 0 ) {
					return minCellInfo.childPageNo
				}
				return ;
			}
			let middle = (minIndex + maxIndex) >> 1;
			let middleCellInfo = this.__getCellInfoByIndex(middle);
			if(this.isLeaf()) { // 精确查找 dataPageNo
                if(IdCompare(middleCellInfo.id, id) === 0) {
                    return middleCellInfo.childPageNo;
                } else if(IdCompare(middleCellInfo.id, id) > 0) {
                    maxIndex = middle;
                } else if(IdCompare(middleCellInfo.id, id) < 0) {
                    minIndex = middle;
                }
			} else { //查找含有此id的ChildPageNo
				let middleNextCellInfo = this.__getCellInfoByIndex(middle + 1);
				if(IdCompare(middleCellInfo.id, id) <= 0
					&& IdCompare(middleNextCellInfo.id, id) > 0) {
					return middleCellInfo.childPageNo;
				}
				if(IdCompare(middleCellInfo.id, id) > 0) {
					maxIndex = middle;
				} else {
					minIndex = middle;
				}
			}

		}
	}

	// get the size of free room
	freeData() {
		return (PAGE_SIZE - ID_HEADER_PAGE_BYTES
				- this.size * ONE_ID_CELL_BYTES);
	}

	static getRootPage() {
		let pageOne = Buffer.alloc(PAGE_SIZE);
		return new Promise((resolve, reject)=> {
			fs.open(INDEXPATH, 'r', (err, file)=> {
				if(err) {
					return reject(err)
				}
				fs.read(file, pageOne, 0, PAGE_SIZE, 0, (err, data)=> {
				let rootPageNo = pageOne.readInt32LE();
				if(err) {
					return reject(err);
				}
				resolve(rootPageNo)
			})
			})
		})
	}

	static setRootPage(rootPageNo) {
		let pageData = Buffer.alloc(PAGE_SIZE);
		pageData.writeInt32LE(rootPageNo, 0);

		fs.open(INDEXPATH, 'w', (err, file)=> {
			fs.writeSync(file, pageData, 0, PAGE_SIZE, 0);
		})
	}

	increaseSize() {
		this.size ++;
		this.setSize(this.size, true);
	}

	insertCell(id, childPageNo) {

		if (this.freeData() >= ONE_ID_CELL_BYTES) {
			let start = this.size * ONE_ID_CELL_BYTES + ID_HEADER_PAGE_BYTES;
			this.data.writeInt32LE(id.timeId, start);
			this.data.writeInt16LE(id.count, start + 4);
			this.data.writeInt32LE(childPageNo, start + 4 + 2);
			this.increaseSize();
			return true;
		} else {
			return false;
		}

	}

	getMinIdInfo() {
		return this.__getCellInfoByIndex(0);
	}

	getMaxIdInfo() {
		return this.__getCellInfoByIndex(this.size - 1);
	}
	// 是否是悬页,即不是跟节点,但却没有parentPage,就为悬页
	isPendingPage() {
		return this.pageParent < 0;
	}

	__initPage() {
		let dataBuffer = this.data;
		let type = dataBuffer.readInt8(0);
		let pageParent = dataBuffer.readInt32LE(PAGE_TYPE_SIZE);
		let size = dataBuffer.readInt16LE(PAGE_TYPE_SIZE + PAGENO_BYTES);
		let pageNo = dataBuffer.readInt32LE(PAGE_TYPE_SIZE + PAGENO_BYTES
				+ SIZENO_BYTES_IN_CELL);
		let prePageNo = dataBuffer.readInt32LE(PAGE_TYPE_SIZE
				+ PAGENO_BYTES * 2 + SIZENO_BYTES_IN_CELL);
		let nextPageNo = dataBuffer.readInt32LE(PAGE_TYPE_SIZE
				+ PAGENO_BYTES * 3 + SIZENO_BYTES_IN_CELL);

		this.setType(type)
			.setPageParent(pageParent)
			.setSize(size)
			.setPageNo(pageNo)
			.setPrePage(prePageNo)
			.setNextPage(nextPageNo);

	}
	static Load(pageNo) {
		let filePath = path.join('js', INDEXPATH);
		return new Promise((resolve, reject)=> {
            let cachedPage = cache.get(pageNo);
            if(cachedPage) {
            	return resolve(cachedPage)
            }
            let page = new IdPage();
            fs.open(filePath, 'r', (err, file)=> {
                fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
                    (err, data)=> {
                		cache.set(pageNo, page);
	                    page.__initPage();
                        resolve(page);
                    })
            })
		})
	}

	static getPage(pageNo, cb) {
		let page = cache.get(pageNo);
		if(page) {
			cb(page);
		} else {
			IdPage.load(pageNo, cb);
		}
	}

	static getPageSize() {
		const stat = fs.statSync(INDEXPATH);
		return stat.size / PAGE_SIZE
	}

	static getLeafNo() {
		let pageOne = Buffer.alloc(PAGE_SIZE);
		return new Promise((resolve, reject)=> {
			fs.open(INDEXPATH, 'r', (err, file)=> {
				if(err) {
					return reject(err)
				}
				fs.read(file, pageOne, 0, PAGE_SIZE, 0, (err, data)=> {
				let leafNo = pageOne.readInt32LE(4);
				if(err) {
					return reject(err);
				}
				resolve(leafNo)
			})
			})
		})                  
	}

	flush(directory) {
        let filePath = path.join(directory, INDEXPATH);
        return new Promise((resolve, reject)=> {
            fs.open(filePath, 'a', (err, file)=> {
                if(err) {
                    return reject(err);
                }

                fs.writeSync(file, this.data, 0, PAGE_SIZE,
                    this.pageNo * PAGE_SIZE );
                resolve(null);
            })
        });
	}
};

const INDEXPAGE_HEADER_SIZE = 1 + 4 + 2 + 4 + 2 + 4 + 4;
const CELLDATA_BYTE_SIZE = 2;
class IndexPage {
	/*
		the format of this kind page is :
-------------------------------------------------------------------------
		| type |  pageParent  |  size  |  pageNo  | offset | prePageNo | nextPageNo
				[offset1, offset2, offset3 ......]
				......
				[cell1, cell2, cell3......]
-------------------------------------------------------------------------
		the size of the filed above is:
		Header:
			type: 		 1b  // this page type
			pageParent:  4b  // this page's parent page number
			size:        2b  // the size of cell
			pageNo:      4b  // this page number
			offset:      2b  // where this page write from
			prePageNo:   4b  // the pre page of this page (same level)
			nextPageNo:  4b  // the next page of this page (same level)

		offsets:
			offset:      2b  // pointer of the cellData

		cellData:
			dataSize     2b  // the total size of the cell
			key:         nb  // the key content
			id:          6b  // the key pair <key, id>
			childPageNo: 4b  // cell pointers for its child page
	*/
	constructor(type, pageParent, pageNo) {
		this.data = Buffer.alloc(PAGE_SIZE);
		if(!pageParent && !pageNo && !type) {
		    return;
        }

		this.type = type;
		this.data.writeInt8(type, 0);

		if(typeof pageParent === 'number') {
			this.pageParent = pageParent;
			this.data.writeInt32LE(pageParent, 1);
		}
		if(typeof pageNo === 'number') {
			this.pageNo = pageNo;
			this.data.writeInt32LE(pageNo, 7);
			cache.set(pageNo, this);
		}

		this.offset = PAGE_SIZE;
		this.data.writeInt16LE(this.offset, 1 + 4 + 2 + 4);
		this.size = 0;
		this.data.writeInt16LE(this.size, 5);  
	}

	oneCellBytes(key) {
	    return ByteSize(key) + PAGENO_BYTES + CELLDATA_BYTE_SIZE + ID_BYTES;
    }

	freeData() {
		return this.offset - INDEXPAGE_HEADER_SIZE - this.size * 2;
	}

	getType() {
		return this.type;
	}

	setType(type) {
	    this.type = type;
	    this.data.writeInt8(type, 0);
	    return this;
    }

    setPrePageNo(prePageNo) {
		this.prePageNo = prePageNo;
		this.data.writeInt32LE(prePageNo, INDEXPAGE_HEADER_SIZE-8);
		return this;
	}

	setNextPageNo(nextPageNo) {
		this.nextPageNo = nextPageNo;
		this.data.writeInt32LE(nextPageNo, INDEXPAGE_HEADER_SIZE-4);
		return this;
	}

    getPrePageNo() {
		return this.data.readInt32LE(INDEXPAGE_HEADER_SIZE-8);
    }

	getNextPageNo() {
		return this.data.readInt32LE(INDEXPAGE_HEADER_SIZE-4);
	}

	getNextPage() {
		let nextPageNo = this.getNextPageNo();
		return IndexPage.LoadPage(nextPageNo);
	}

	getPrePage() {
		let prePageNo = this.getPrePageNo();
		return IndexPage.LoadPage(prePageNo);
	}

	hasRoomForBytes(bytesNum) {
		let free = this.freeData();
		return free >= bytesNum;
	}

	// true: this page has room for key, false: not 
	hasRoomFor(key) {
		let oneCellBytes = this.oneCellBytes(key) + 2;
		// 需要的空间为一个cell的空间和一个offset指向；
		return this.hasRoomForBytes(oneCellBytes);
	}	

	setParentPage(pageNo) {
        this.pageParent = pageNo;
        this.data.writeInt32LE(pageNo, 1);
        return this;
	}

	getParentPageNo() {
		return this.pageParent
	}

	setPageNo(pageNo) {
	    this.pageNo = pageNo;
	    this.data.writeInt32LE(pageNo, 1 + 4 + 2);
	    return this;
    }

    setSize(size) {
	    this.size = size;
	    // the header is like: type(1b) + pageParent(4b) + size(2b) ...
		this.data.writeInt16LE(size, 1 + 4);
		return this;
    }

    getSize() {
	    return this.size;
    }

	getPageNo() {
		return this.pageNo;
	}

	setOffset(offset) {
	    this.offset = offset;
	    this.data.writeInt16LE(offset, 1 + 4 + 2 + 4);
	    return this;
    }

    getOffset() {
	    return this.offset;
    }

	isRoot() {
		return this.type & PAGE_TYPE_ROOT;
	}

	isLeaf(){
		return this.type & PAGE_TYPE_LEAF;
	}

    /**
	 * reuse page的数据结构：
	 * 	 +-------------------------------+
		 |type(1b) |next(4b)|  ...       |
		 +-------------------------------+
     * @param nextReuseNo
     */
    transformToReuse(nextReuseNo) {
        this.setType(PAGE_TYPE_REUSE, true);
        this.nextReuseNo = nextReuseNo;
		this.data.writeInt32LE(nextReuseNo, 1);
    }

    static LoadAsReuse(pageNo) {
    	return IndexPage.__loadRawPage(pageNo, (dataBuffer)=> {
    		let reuseIndexPage = new IndexPage().setType(PAGE_TYPE_REUSE);
    		reuseIndexPage.nextReuseNo = dataBuffer.readInt32LE(1);
    		cache.set(pageNo, reuseIndexPage);
    		return reuseIndexPage;
		});
	}
	// 只在type为PAGE_TYPE_REUSE时候才调用
	reUseNext() {
    	let nextReuseNo = this.data.readInt32LE(1);
    	return nextReuseNo;
	}

    /**
	 * 每个cell的构成如：
					 +-------------------+------> offset
					 |    dataSize(2b)   |------> dataSize = size(key) + size(id) + size(childPageNo)
					 +-------------------+
					 |                   |
					 |      key(nb)      |
					 |                   |
					 +-------------------+
					 |      id(6b)       |
					 +-------------------+
					 |   childPageNo(4b) |
					 +-------------------+
     * @param offset
     * @param index
     * @returns {{key: String, id: {timeId: null, count: null}, childPageNo: Number}}
     */
	getCellByOffset(offset, index) {
	    console.log('offset', offset, index);
	    if(offset === 0) {
	    	console.log(index);
	    }
		let dataSize = this.data.readInt16LE(offset);
		let data = this.data.slice(
			offset + CELLDATA_BYTE_SIZE, 
			dataSize + offset + CELLDATA_BYTE_SIZE);
		let keySize = dataSize - ID_BYTES - PAGENO_BYTES;
		let keyBuffer = data.slice(0, keySize);
		let key = keyBuffer.toString();

		let idBuffer = data.slice(keySize, ID_BYTES + keySize);
		let id = {timeId: null, count: null};
		id.timeId = idBuffer.readInt32LE(0);
		id.count = idBuffer.readInt16LE(4);

		let childPageNoBuffer = data.slice(
			keySize + ID_BYTES, PAGENO_BYTES + ID_BYTES + keySize);
		let childPageNo = childPageNoBuffer.readInt32LE(0);

		return {key, id, childPageNo}
	}

	resortOffsetArray(offset, position) {
		assert(position >= 0 && this.size >= position);
		
		let dataCopy = Buffer.from(this.data);
		// this.data.slice share the same buffer with this.data
		// let halfBuffer = this.data.slice(
        //      INDEXPAGE_HEADER_SIZE + position * 2,
		// 		INDEXPAGE_HEADER_SIZE + this.size * 2);
		let halfBuffer = dataCopy.slice(INDEXPAGE_HEADER_SIZE + position * 2,
				INDEXPAGE_HEADER_SIZE + this.size * 2);
		this.data.writeInt16LE(offset, INDEXPAGE_HEADER_SIZE + position * 2);
		halfBuffer.copy(
				this.data, 
				INDEXPAGE_HEADER_SIZE + position * 2 + 2, 
				0, 
				(this.size - position) * 2
			);
		this.size ++;
		this.setSize(this.size);
	}

	// position starts from 0
	__getOffsetByIndex(position) {
		return this.data.readInt16LE(INDEXPAGE_HEADER_SIZE + position * 2);
	}

	getCellInfoByIndex(index) {
		let offset = this.__getOffsetByIndex(index);
		return this.getCellByOffset(offset, index);
	}

	__rewritePage(cells) {
		this.setSize(0);
		this.setOffset(PAGE_SIZE);

		for(let cell of cells) {
			let {key, id, childPageNo} = cell;
			this.insertCell(key, id, childPageNo);
		}
	}

    /**
	 * 判断没有key的情况下，该page是否是小于或者大于一半；
	 * 由于key不是定长，而且page由一整page分裂成两半，
	 * 所以这里判断是否大于一半，也是根据其和其相邻page能否被合并成一个page判断；
	 * 如果能被合并成一个page页面，则返回true，否则返回false;
	 *
	 * 这里做相应的简化：
	 * a: 最左的page和右侧的邻page匹配判断
	 * b: 其他的page和左侧的邻page匹配判断
     * @param key
     */
	async isLessHalfWithoutKey(key) {
		let cellSize = this.oneCellBytes(key);
		let adjacentPage,
			adjacentPageNo,
			adjacentCellsBytes, needBytes;
		if(this.isLeftMost()) {
			adjacentPageNo = this.getNextPageNo();
		} else {
            adjacentPageNo = this.getPrePageNo();
		}
        adjacentPage = await IndexPage.LoadPage(adjacentPageNo);
        adjacentCellsBytes = adjacentPage.cellsBytes();
        // todo: adjacentCellsBytes小于cellSize
		needBytes = adjacentCellsBytes - cellSize;

        return this.hasRoomForBytes(needBytes);
	}

    deleteCellByIndex(index) {
		let filtered = [];
		for(let i = 0; i < this.size; i ++) {
			if(i !== index) {
				filtered.push(this.getCellInfoByIndex(i));
			}
		}
		this.__rewritePage(filtered);
    }

    updateCellInfo(cellInfo, index) {
		let filtered = [];
		for(let i = 0; i < this.size; i ++) {
			if(i===index) {
				filtered.push(cellInfo);
			} else {
				filtered.push(this.getCellInfoByIndex(i));
			}
		}

		this.__rewritePage(filtered);
    }

    // 返回所有的cells信息
    allCells() {
		let all = [];
		for(let i = 0; i < this.size; i ++) {
			all.push(this.getCellInfoByIndex(i));
		}
		return all;
    }

    batchInsertCells(cells) {
		for(let cell of cells) {
			let {key, id, childPageNo} = cell;
			this.insertCell(key, id, childPageNo);
		}
	}

    // 返回该page的offset区域和celldata区域总bytes
    cellsBytes() {
        return this.size * OFFSET_BYTES_SIZE + (PAGE_SIZE - this.offset);
    }

    // 判断该page是否为最左边
    isLeftMost() {
		return !this.getPrePageNo();
	}

	// 判断该page是否为最右边
	isRightMost() {
		return !this.getNextPageNo();
	}

    // 找到最左的和key一样大小的cellInfo
	__theLeftMostEqual(start, key) {
		let cellInfo = this.getCellInfoByIndex(start);
		while(compare(key, cellInfo['key']) === 0) {
			if(start === 0) {
				return {... cellInfo, cellIndex: 0};
			}
			start --;
			cellInfo = this.getCellInfoByIndex(start);
		}

		return {... this.getCellInfoByIndex(start + 1), cellIndex: start + 1};
	}

    /**
     * 获取左侧最接近的cell；
     * 如 cells如下： (key1 < key2 < key3 < ...)
     * ---------------------------------------------------------------------
     * [key1, id1], [key1, id2], [key2, id3], [key2, id4], [key3, id4]......
     * ---------------------------------------------------------------------
     * 如果__findNearestCellInfo(key1)则返回的为id1;
     *    __findNearestCellInfo(key2) 返回的为id3;
     * @param key
     * @returns {*}
     * @private
     */
    findNearestCellInfo(key) {
		if(this.size === 0) {
			return;
		}
		if(this.size === 1) {
			let onlyCellInfo = this.getCellInfoByIndex(0);
			if(compare(key, onlyCellInfo.key) === 0) {
				return onlyCellInfo;
			}
		}

		let minIndex = 0, maxIndex = this.size - 1;
		while(minIndex < maxIndex) {
			let minCellInfo = this.getCellInfoByIndex(minIndex);
			let maxCellInfo = this.getCellInfoByIndex(maxIndex);

			if(minIndex + 1 === maxIndex
				&& compare(key, minCellInfo.key) > 0
				&& compare(key, maxCellInfo.key) < 0
			) {
					return {... minCellInfo, cellIndex: minIndex}
			}

			let middle = (minIndex + maxIndex) >> 1;
			let middleCellInfo = this.getCellInfoByIndex(middle);
			if(compare(key, maxCellInfo.key) >= 0) {
				// return {... maxCellInfo, cellIndex: maxIndex};
				if(compare(key, maxCellInfo.key) > 0) {
					return {... maxCellInfo, cellIndex: maxIndex};
				}
				return this.__theLeftMostEqual(maxIndex, key);
			}
			if(compare(key, minCellInfo.key) <= 0) {
				return {... minCellInfo, cellIndex: 0}
			}
			if(compare(middleCellInfo.key, key) > 0) {
				maxIndex = middle;
			} else if(compare(middleCellInfo.key, key) === 0) {
				return this.__theLeftMostEqual(middle, key);
			} else {
				let middleNext = middle + 1;
				let middleNextCellInfo = this.getCellInfoByIndex(middleNext);
				if(compare(middleNextCellInfo.key, key) > 0) {
					return { ... middleCellInfo, cellIndex: middle};
				} else {
					minIndex = middle;
				}
			}
		}
	}

	collectAllEqualIds(key) {
    	let result = [];
		let leftMost = this.findNearestCellInfo(key);
		if(leftMost) {
            let startCellInfo = leftMost;
            let startIndex = leftMost.cellIndex;
            do {
                result.push(startCellInfo);
                if (startIndex === this.size - 1) {
                    return result;
                }
                startIndex ++;
                startCellInfo = {
	                ... this.getCellInfoByIndex(startIndex),
	                cellIndex: startIndex
                }
            } while (compare(key, startCellInfo.key) === 0);
        }

		return result;
	}

	isLastCellInfo(cellIndex) {
        return this.size === cellIndex;
	}

	static __loadRawPage(pageNo, fill) {
    	let cachedPage = cache.get(pageNo);
    	if(cachedPage) {
    		return Promise.resolve(cachedPage);
		}

        return new Promise((resolve, reject)=> {
            fs.open('js/js.index', 'r', (err, file)=> {
                if(err) {
                    return reject(err);
                }
				let dataBuffer = Buffer.alloc(PAGE_SIZE);
                fs.read(file, dataBuffer, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
                    (err, data)=> {
                        if(err) {
                            return reject(err);
                        }
                        resolve(fill(dataBuffer));
                    })
            })
        });
	}

	static LoadPage(pageNoN) {
		return this.__loadRawPage(pageNoN, (dataBuffer)=> {
			let page = new IndexPage();
            let type = dataBuffer.readInt8(0);
            page.setType(type);
            let pageParent = dataBuffer.readInt32LE(1);
            page.setParentPage(pageParent);
            let size = dataBuffer.readInt16LE(1+4);
            page.setSize(size);
            let pageNo = dataBuffer.readInt32LE(1+4+2);
            page.setPageNo(pageNo);
            let offset = dataBuffer.readInt16LE(1+4+2+4);
            page.setOffset(offset);
            let nextPageNo = dataBuffer.readInt32LE(INDEXPAGE_HEADER_SIZE - 8);
            page.setNextPageNo(nextPageNo);
            let prePageNo = dataBuffer.readInt32LE(INDEXPAGE_HEADER_SIZE - 4);
            page.setPrePageNo(prePageNo);
            cache.set(pageNo, page);
            return page;
		});
	}

    getPageParent() {
        let parentNo = this.getParentPageNo();
        return IndexPage.LoadPage(parentNo);
    }

	insertCell(key, id, childPageNo) {
		let keyByteSize = ByteSize(key);
		let totalByteSize =
            CELLDATA_BYTE_SIZE + keyByteSize + ID_BYTES + PAGENO_BYTES;
		this.offset -= totalByteSize;
		// 先更新this.offset
		this.setOffset(this.offset);

		if(this.size > 20) {
			console.log(this);
		}
		this.data.writeInt16LE(totalByteSize - CELLDATA_BYTE_SIZE,
				this.offset);
		this.data.write(key, this.offset + CELLDATA_BYTE_SIZE);
		// 依次写入id信息 timeId: 4b, count: 2b
		this.data.writeInt32LE(id.timeId,
            this.offset + CELLDATA_BYTE_SIZE + keyByteSize);
		this.data.writeInt16LE(id.count,
			this.offset + CELLDATA_BYTE_SIZE + keyByteSize + 4);
		this.data.writeInt32LE(childPageNo,
            this.offset + CELLDATA_BYTE_SIZE + keyByteSize + ID_BYTES);

		if(this.size > 0) {
			if(this.size === 1) {
				let onlyKey = this.getCellInfoByIndex(0)['key'];
				if(compare(key, onlyKey) > 0) {
					this.resortOffsetArray(this.offset, 1);
				} else {
					this.resortOffsetArray(this.offset, 0);
				}
				return;
			}

			// let nearestCellInfo = this.__findNearestCellInfo(key);
			// console.log('nearestCellInfo', nearestCellInfo)
			// this.resortOffsetArray(this.offset, nearestCellInfo.cellIndex);
		
			let minIndex = 0, maxIndex = (this.size - 1);
			while(maxIndex > minIndex) {
				let minKey = this.getCellInfoByIndex(minIndex)['key'];
				let maxKey = this.getCellInfoByIndex(maxIndex)['key'];
				if(compare(minKey, key) > 0) { // key is smaller than minKey
					this.resortOffsetArray(this.offset, minIndex);
					return ;
				} else if(compare(key, maxKey) >= 0) {
					this.resortOffsetArray(this.offset, maxIndex + 1);
					return ;
				} else {
					let middleIndex = (minIndex + maxIndex) >>1;
					let middleKey = this.getCellInfoByIndex(middleIndex).key;
                    let nextKey = this.getCellInfoByIndex(middleIndex + 1).key;
                    // find the correct position
					if(compare(nextKey, key) > 0
                        && compare(middleKey, key) <= 0) {
						this.resortOffsetArray(this.offset, middleIndex + 1);
						return ;
					}

					if(compare(middleKey, key) > 0) {
						maxIndex = middleIndex;
					} else {
						minIndex = middleIndex;
					}

				}
			}
			
		} else {
			this.size ++;
			this.setSize(this.size);
			this.data.writeInt16LE(this.offset, INDEXPAGE_HEADER_SIZE);
		}
	}

    flush(directory) {
        let filePath = path.join(directory, INDEXPATH);
	    return new Promise((resolve, reject)=> {
	        fs.open(filePath, 'a', (err, file)=> {
	            if(err) {
	                return reject(err);
                }

                fs.write(file, this.data, 0, PAGE_SIZE,
                    this.pageNo * PAGE_SIZE, (err, data)=> {
                		if(err) {
                			return reject(err);
						}
						resolve();
					});
                resolve(null);
            })
        })
    }

    static FlushPageToDisk(directory, pageBuffer, pageNo) {
		let filePath = path.join(directory, INDEXPATH);
		return new Promise((resolve, reject)=> {
			fs.open(filePath, 'a', (err, file)=> {
				if(err) {
					return reject(err);
				}
				fs.write(file, pageBuffer, 0, PAGE_SIZE, pageNo * PAGE_SIZE, (err, data)=> {
					if(err) {
						return reject(err);
					}
                    resolve(null);
				});
			})
		})
    }

    getNearestChildPage(key) {
		if(this.type & PAGE_TYPE_LEAF) {
			return Promise.resolve(this);
		}
		let cellInfo = this.findNearestCellInfo(key);
		if(!cellInfo) {
			console.log('cellinfo',key);
		}

		return IndexPage.LoadPage(cellInfo.childPageNo);
	}

	findCorrectCellInfo(key, id) {
    	let startIndex = this.findNearestCellInfo(key);
    	let startCellInfo ;
    	let startPage = this;

    	// todo 跨page的查找
		do {
			startCellInfo = this.getCellInfoByIndex(startIndex);
			if(compare(startCellInfo.key, key) === 0
				&& IdCompare(startCellInfo.id, id) === 0) {
				return Object.assign(startCellInfo, {cellIndex: startIndex});
			}
			startIndex++;
		} while(startIndex < startPage.getSize());
	}

	// split into half by a indexPair
	half(insertCellInfo) {
		let {key, id, childPageNo} = insertCellInfo;
		let tempArray = [];
		for(var i = 0; i < this.size; i ++) {
			let cellInfo = this.getCellInfoByIndex(i);
			if(i === 0) {
				if(compare(cellInfo.key, key) >= 0) {
					tempArray.push(insertCellInfo);
				}
			}
            tempArray.push(cellInfo);
			if(i < this.size -1) {
				let nextInfo = this.getCellInfoByIndex(i + 1);
                if(compare(cellInfo.key, key) < 0
	                && compare(nextInfo.key, key) >= 0) {
                    tempArray.push(insertCellInfo);
                }
			}
			if(i === this.size - 1) {
				if(compare(cellInfo.key, key) <= 0) {
					tempArray.push(insertCellInfo);
				}
			}
		}

		let halfSize = (this.size + 1) >> 1;
		let splitInfo = [];
		for(var i = halfSize; i <= this.size; i ++) {
			splitInfo.push(tempArray[i]);
		}

		// rewrite the cells from begining
		this.offset = PAGE_SIZE;
		this.size = 0;
		for(var i = 0; i < halfSize; i ++) {
			let cellInfo = tempArray[i];
			this.insertCell(cellInfo['key'], cellInfo['id'],
                cellInfo['childPageNo']);
		}

		return splitInfo;
	}

	static CachePage() {
		return cache;
	}
}

exports.DataPage = DataPage;
exports.IdPage = IdPage;
exports.IndexPage = IndexPage;
exports.PAGE_TYPE_ID = PAGE_TYPE_ID;
exports.PAGE_TYPE_INDEX = PAGE_TYPE_INDEX;
exports.PAGE_TYPE_ROOT = PAGE_TYPE_ROOT;
exports.PAGE_TYPE_INTERNAL = PAGE_TYPE_INTERNAL;
exports.PAGE_TYPE_LEAF = PAGE_TYPE_LEAF;
