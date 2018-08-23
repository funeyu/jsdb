const fs = require('fs');
const assert = require('assert');

const Lru = require('lru-cache');
const {compare, ByteSize, IdGen, IdCompare} = require('./utils.js')

const PAGE_SIZE = 1024;
const RECORD_ID_BYTES_SIZE = 6;
const FILEPATH = 'js.db';
const INDEXPATH = 'js.index';
const PAGE_TYPE_ID = 1;
const PAGE_TYPE_LEAF = 1 << 1;
const PAGE_TYPE_INTERNAL = 1 << 2;
const PAGE_TYPE_ROOT = 1 << 3;
const PAGE_TYPE_INDEX = 1 << 4;
const PAGE_TYPE_SIZE = 1;     // the byteSize of the type
const MIN_KEY = '-1';

const cache = Lru(64 * 1024);
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
		this.size = 0;				// the size of data
		this.offset = PAGE_SIZE;  	// write data from bottom up

		cache.set(pageNo + '_Data', this);
	}
	
	freeData() {
		return (this.offset - DATA_PAGE_HEADER_BYTES_SIZE
			-this.size * ID_CELL_BYTES_SIZE);
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
			this.data.writeInt16LE(this.offset, PAGENO_BYTES + OFFSET_BYTES_SIZE);
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

	getCell(idInfo) {
        let size = this.data.readInt16LE(PAGENO_BYTES);

        let maxIdInfo = this.__formId(this.size - 1);
        let minIdInfo = this.__formId(0);

        if(IdCompare(idInfo, maxIdInfo) > 0 || IdCompare(idInfo, minIdInfo) < 0) {
            console.log('no data matched id:', id);
        } else {
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
            		return this.__getDataByOffset(middleInfo['offset']);
				} else if(IdCompare(idInfo, midIdInfo) > 0) {
            		min = middle
				} else {
					max = middle
                }
			}
        }
	}

	flush() {
		return new Promise((resolve, reject)=> {
            fs.open('js.db', 'w', (err, file)=> {
                if(err) {
                	return reject(err)
				}
                fs.writeSync(file, this.data, 0, PAGE_SIZE, this.pageNo * PAGE_SIZE);
                resolve()
            });
		})
	}

	static load(pageNo, cb) {
		let page = cache.get(pageNo + '_Data');
		if(page) {
			return cb(null, page);
		}

		page = new DataPage(pageNo);
		fs.open('js.db', 'r', (err, file)=> {
			fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
                (err, data)=> {
				cache.set(pageNo + '_Data', page);
				
				cb(err, page);
				console.log('data size', page.data.readInt32LE());
			});
		})
	}

	static getPageSize() {
		const stat = fs.statSync(FILEPATH);
		return stat.size / PAGE_SIZE;
	}
}

const ONE_CELL_BYTES = 10;
const PAGENO_BYTES = 4;
const SIZENO_BYTES_IN_CELL = 2;
const ID_BYTES = 4;
const IDPAGE_HEADER_BYTES =
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
		prePageNo:     4b
		nextPageNo:    4b
		childPageNo: 4b
		id:          6b
	*/
	constructor(type, pageParent, pageNo) {
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
		this.data = Buffer.alloc(PAGE_SIZE);
		this.data.writeInt8(type, 0);
		this.offset = PAGE_SIZE;
		this.size = 0;
		this.data.writeInt16LE(this.size,  PAGE_TYPE_SIZE + PAGENO_BYTES);
	}

	setPrePage(prePageNo) {
		this.prePageNo = prePageNo;
		let start = PAEG_TYPE_SIZE + PAGENO_BYTES * 2 + SIZENO_BYTES_IN_CELL;
		this.data.writeInt32LE(prePageNo, start);
	}

	setNextPage(nextPageNo) {
		this.nextPageNo = nextPageNo;
		let start = PAGE_TYPE_SIZE + PAGENO_BYTES * 3 + SIZENO_BYTES_IN_CELL;
		this.data.writeInt32LE(nextPageNo, start);
	}

	__getCellInfoByIndex(cellsBuffer, index) {
		let childPageNo = cellsBuffer.readInt32LE(index * ONE_CELL_BYTES);
		let timeId = cellsBuffer.readInt32LE(index * ONE_CELL_BYTES + 4);
		let count = cellsBuffer.readInt16LE(index * ONE_CELL_BYTES + 8);

		return {
			id: {timeId, count},
			childPageNo
		}
	}

	getChildPageNo(id) {
		let cellByteSize = this.size * ONE_CELL_BYTES;
		// sizeOf(size) +
		// sizeOf(paegParent, pageNo, prePage, nextPage) + sizeOf(size)
		let cellByteBegin = PAGE_TYPE_SIZE +
				PAGENO_BYTES * 4 + SIZENO_BYTES_IN_CELL;
		let copyData = Buffer.from(this.data);
		let cellsBuffer = copyData.slice(cellByteBegin,
                cellByteBegin + cellByteSize);

		let minIndex = 0, maxIndex = this.size - 1;
		let minCellInfo = this.__getCellInfoByIndex(cellsBuffer, minIndex),
			maxCellInfo = this.__getCellInfoByIndex(cellsBuffer, maxIndex);

		if(IdCompare(minCellInfo.id, id) > 0
			|| IdCompare(maxCellInfo.id, id) < 0) {
			return
		}

		while(maxIndex > minIndex) {
			let middle = (minIndex + maxIndex) >> 1;
			let middleCellInfo = this.__getCellInfoByIndex(cellsBuffer, middle);
			if(IdCompare(middleCellInfo.id, id) === 0) {
				return middleCellInfo.childPageNo;
			} else if(IdCompare(middleCellInfo.id, id) > 0) {
				maxIndex = middle;
			} else if(IdCompare(middleCellInfo.id, id) < 0) {
				minIndex = middle;
			}
		}
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
		return this.type | PAGE_TYPE_LEAF;
	}

	isRoot() {
		return this.type | PAGE_TYPE_ROOT;
	}

	setPageNo(pageNo) {
		this.pageNo = pageNo;
		return this;
	}
	setPageParent(pageParent) {
		this.pageParent = pageParent;
		return this;
	}
	setSize(size) {
		this.size = size;
		return this;
	}
	setRoot(isRoot) {
		this.isRoot = isRoot;
		return this;
	}

	// get the size of free room
	freeData() {
		// this.offset - sizeof(pageParent) - sizeof(size) - sizeof(pageNo)
		return this.offset - 4 - 2 - 4;
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

	insertCell(id, childPageNo) {

		if (this.freeData() >= 8) {
			this.data.writeInt32LE(id, this.offset - 8);
			this.data.writeInt32LE(childPageNo, this.offset - 4);
			this.offset = this.offset - 8;

			return true;
		} else {
			return false;
		}

	}

	static load(pageNo, cb) {
		let page = new IdPage();
		fs.open(INDEXPATH, 'r', (err, file)=> {
			fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
                (err, data)=> {
				let pageParent = page.data.readInt32LE(0);
				let size = page.data.readInt16LE(4);
				let pageNo = page.data.readInt32LE(6);
				page.setPageParent(pageParent)
					.setSize(size)
					.setPageNo(pageNo);

				cb(err, page);
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

	flush() {
		fs.open(INDEXPATH, 'w', (err, file)=> {
			fs.writeSync(file, this.data, 0, PAGE_SIZE, this.pageNo * PAGE_SIZE);
		})
	}
}

const INDEXPAGE_HEADER_SIZE = 1 + 4 + 2 + 4 + 2;
const CELLDATA_BYTE_SIZE = 2;
class IndexPage {
	/*
		the format of this kind page is :
-------------------------------------------------------------------------
| type |  pageParent  |  size  |  pageNo  | offset | [cell1, cell2......]
-------------------------------------------------------------------------
		every cell is : cellSize + childPageNo + id + rawKey

		the size of the filed above is:
		type: 		 1b  // this page type
		pageParent:  4b  // this page's parent page number
		size:        2b  // the size of cell
		pageNo:      4b  // this page number
		offset:      2b  // where this page write from 

		cellOffset:  2b  // one cell bytes offset
		childPageNo: 4b  // cell pointers for its child page
		id:          4b  // the key pair <key, id>
		rawKey: 	 nb  // the raw data of key
	*/
	constructor(pageParent, pageNo, type) {
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

	static LoadKeyDictionary() {

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
    }

	// true: this page has room for key, false: not 
	hasRoomFor(key) {
		let free = this.freeData(), oneCellBytes = this.oneCellBytes(key);

		return free >= oneCellBytes
	}	

	setParentPage(pageNo) {
        this.pageParent = pageNo;
	}

	getParentPageNo() {
		return this.pageParent
	}

	setPageNo(pageNo) {
	    this.pageNo = pageNo;
    }

    setSize(size) {
	    this.size = size;
    }

    getSize() {
	    return this.size;
    }

	getPageNo() {
		return this.pageNo;
	}

	setOffset(offset) {
	    this.offset = offset;
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

	getCellByOffset(offset, index) {
	    console.log('offset', offset, index);
		let dataSize = this.data.readInt16LE(offset);
		let data = this.data.slice(
			offset + CELLDATA_BYTE_SIZE, 
			dataSize + offset + CELLDATA_BYTE_SIZE);
		let keySize = dataSize - ID_BYTES - PAGENO_BYTES;
		let keyBuffer = data.slice(0, keySize);
		let key = keyBuffer.toString();
		let idBuffer = data.slice(keySize, ID_BYTES + keySize);
		let id = idBuffer.readInt32LE(0);
		let childPageNoBuffer = data.slice(
			keySize + ID_BYTES, PAGENO_BYTES + ID_BYTES + keySize);
		let childPageNo = childPageNoBuffer.readInt16LE(0);

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
	}

	// position starts from 0
	__getOffsetByIndex(position) {
		return this.data.readInt16LE(INDEXPAGE_HEADER_SIZE + position * 2);
	}

	getCellInfoByIndex(index) {
		let offset = this.__getOffsetByIndex(index);
		return this.getCellByOffset(offset, index);
	}

	__findNearestCellInfo(key) {
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

			let middle = (minIndex + maxIndex) >> 1;
			let middleCellInfo = this.getCellInfoByIndex(middle);
			if(compare(key, maxCellInfo['key']) >= 0) {
				return {... maxCellInfo, cellIndex: maxIndex}
			}
			if(compare(key, minCellInfo['key']) <= 0) {
				return {... minCellInfo, cellIndex: 0}
			}
			if(compare(middleCellInfo.key, key) > 0) {
				maxIndex = middle;
			} else if(compare(middleCellInfo.key, key) === 0) {
				return { ... middleCellInfo, cellIndex: middle};
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

	static LoadPage(pageNo) {
		let cachedPage = cache.get(pageNo);
		if(cachedPage) {
			return Promise.resolve(cachedPage);
		}
        let page = new IndexPage();
        return new Promise((resolve, reject)=> {
            fs.open(INDEXPATH, 'r', (err, file)=> {
                if(err) {
                    return reject(err);
                }

                fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
                    (err, data)=> {
                        if(err) {
                            return reject(err);
                        }
                        let type = page.data.readInt8(0);
                        page.setType(type);
                        let pageParent = page.data.readInt32LE(1);
                        page.setPageNo(pageParent);
                        let size = page.data.readInt16LE(1+4);
                        page.setSize(size);
                        let pageNo = page.data.readInt32LE(1+4+2);
                        page.setPageNo(pageNo);
                        let offset = page.data.readInt16LE(1+4+2+2);
                        page.setOffset(offset);

                        resolve(page);
                    })
            })
        })

	} 

	async findId(key) {
		let cellInfo = this.__findNearestCellInfo(key)
        if(cellInfo && compare(cellInfo.key, key) === 0) {
            return cellInfo.id
        }
		console.log('cellInfo+++++++++++++++++++++++++++++++', cellInfo);
		while(cellInfo.childPageNo >= 0) {
			let childPage = await IndexPage.LoadPage(cellInfo.childPageNo)
			cellInfo = childPage.__findNearestCellInfo(key);
            if(cellInfo && compare(cellInfo.key, key) === 0) {
                return cellInfo.id
            }
		}
	}

	insertCell(key, id, childPageNo) {
		let keyByteSize = ByteSize(key);
		let totalByteSize =
            CELLDATA_BYTE_SIZE + keyByteSize + ID_BYTES + PAGENO_BYTES;
		this.offset -= totalByteSize;
		this.data.writeInt16LE(totalByteSize - CELLDATA_BYTE_SIZE, this.offset);
		this.data.write(key, this.offset + CELLDATA_BYTE_SIZE);
		this.data.writeInt32LE(id,
            this.offset + CELLDATA_BYTE_SIZE + keyByteSize);
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
					let middleKey = this.getCellInfoByIndex(middleIndex)['key'];
                    let nextKey = this.getCellInfoByIndex(middleIndex + 1)['key'];
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
			this.data.writeInt16LE(this.offset, INDEXPAGE_HEADER_SIZE);
		}
	}

    flushToDisk() {
	    return new Promise((resolve, reject)=> {
	        fs.open(INDEXPATH, 'w', (err, file)=> {
	            if(err) {
	                return reject(err);
                }

                fs.writeSync(file, this.data, 0, PAGE_SIZE,
                    this.pageNo * PAGE_SIZE )
                resolve(null);
            })
        })
    }

	getChildPage(key) {
		if(this.type & PAGE_TYPE_LEAF) {
			return Promise.resolve(this);
		}
		let cellInfo = this.__findNearestCellInfo(key);	
		let cachedPage = cache.get(cellInfo.childPageNo);
		if(cachedPage) {
			return Promise.resolve(cachedPage);
		} else {
			return IndexPage.LoadPage(cellInfo.childPageNo);
		}
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
                if(compare(cellInfo.key, key) < 0 && compare(nextInfo.key, key) >= 0) {
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
}

let page = new DataPage(1);

let id1 = IdGen();
let id2 = IdGen();
let id3 = IdGen();

page.insertCell(id1, 'stringDataPage1');
page.insertCell(id2, 'nodejsDataPage1');
page.insertCell(id3, 'javaDataPage1123');
console.log(id1)
console.log(id2)
console.log(id3)
page.flush().then(data=> {
    DataPage.load(1, (err, dataPage)=> {
    	console.log('errror', err)
	    console.log('id3', id3)
        let cellInfo = dataPage.getCell(id3);
    	console.log('cellInfo', cellInfo)
    });
})


// for(var i = 0; i < 10000; i ++) {
// 	console.log(IdGen())
// }

exports.DataPage = DataPage;
exports.IdPage = IdPage;
exports.IndexPage = IndexPage;
exports.PAGE_TYPE_ID = PAGE_TYPE_ID;
exports.PAGE_TYPE_INDEX = PAGE_TYPE_INDEX;
exports.PAGE_TYPE_ROOT = PAGE_TYPE_ROOT;
exports.PAGE_TYPE_INTERNAL = PAGE_TYPE_INTERNAL;
exports.PAGE_TYPE_LEAF = PAGE_TYPE_LEAF;

const st = function() {
	let st = fs.statSync(INDEXPATH);
	console.log('st.size:', st.size)
}
st();