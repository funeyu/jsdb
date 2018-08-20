const fs = require('fs');
const assert = require('assert');

const Lru = require('lru-cache');
const {compare} = require('./utils.js')

const PAGE_SIZE = 1024;
const FILEPATH = 'js.db';
const INDEXPATH = 'js.index';
const PAGE_TYPE_ID = 1;
const PAGE_TYPE_LEAF = 1 << 1;
const PAGE_TYPE_INTERNAL = 1 << 2;
const PAGE_TYPE_ROOT = 1 << 3;
const PAGE_TYPE_INDEX = 1 << 4;
const MIN_KEY = '-1';


const ByteSize = function(str) {
	str = str.toString();
	let len = str.length;

	for (let i = str.length; i--; ) {
    	const code = str.charCodeAt(i);
    	if (0xdc00 <= code && code <= 0xdfff) {
      	i--;
    }

    if (0x7f < code && code <= 0x7ff) {
      len++;
    } else if (0x7ff < code && code <= 0xffff) {
      len += 2;
    }
  }

  return len;
}

const cache = Lru(64 * 1024);

class DataPage {
	constructor(pageNo) {
		this.pageNo = pageNo;      	// start from zero
		this.data = Buffer.alloc(PAGE_SIZE);
		this.size = 0;				// the size of data
		this.offset = PAGE_SIZE;  	// write data from bottom up

		cache.set(pageNo + '_Data', this);
	}
	
	freeData() {
		return this.offset - 4 - this.size*8
	}

	insertCell(id, cellData) {
		let byteSize = ByteSize(cellData)
		if((this.freeData() - byteSize - 8) > 0) {
			this.data.write(cellData, this.offset - byteSize);
			this.offset = this.offset - byteSize

			this.data.writeInt32LE(id, this.size*8 + 4);
			this.data.writeInt32LE(this.offset, this.size*8 + 4 +4);
			this.size ++;
			this.data.writeInt32LE(this.size, 0);

			return true;
		} else {		// has no room for cellData
			return false;
		}
	}

	getCell(id) {
		fs.open('js.db', 'r', (err, file)=> {
			fs.read(file, this.data, 0, PAGE_SIZE, this.pageNo * PAGE_SIZE,
                (err, data)=> {
				let size = this.data.readInt32LE();

				let maxId = this.data.readInt32LE(size*8 - 4);
				let minId = this.data.readInt32LE(4);
				if(id > maxId || id < minId) {
					console.log('no data matched id:', id);
				} else {
					let max=size, min = 1;
					let middle = Math.ceil((max + min) / 2);
					let midId = this.data.readInt32LE(middle*8 -4);
					while(midId !== id) {
						console.log('midId', midId)
						if((max - min) === 1) {
							midId = null;
							break;
						}
						if(midId > id) {
							max = middle
						} else {
							min = middle
						}
						middle = Math.ceil((max + min) / 2);
						midId = this.data.readInt32LE(middle*8 -4);
					}

					if(midId) {
						let offset = this.data.readInt32LE(middle*8);
						console.log('offset', offset)

						let result = Buffer.alloc(13);
						this.data.copy(result, 0, offset, offset + 13)
						console.log('result', result.toString())
					} else {
						console.log('no matched result')
					}
				}
			})
		})
	}

	flush() {
		fs.open('js.db', 'w', (err, file)=> {
			console.log(err)
			fs.writeSync(file, this.data, 0, PAGE_SIZE, this.pageNo * PAGE_SIZE);
		});
	}

	static load(pageNo, cb) {
		let page = cache.get(pageNo + '_Data');
		if(page) {
			cb(page);
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

const ONE_CELL_BYTES = 8;
const PAGEPARENT_BYTES_IN_CELL = 4;
const PAGENO_BYTES = 4;
const SIZENO_BYTES_IN_CELL = 2;
const ID_BYTES = 4;
class IdPage {
	/*
		the format of this kind page is :
		---------------------------------------------------------------
		|  pageParent  |  size  |  pageNo | [cell1, cell2, cell3 ......]
		---------------------------------------------------------------
		every cell is : childPageNo + id

		the size of the filed above is:
		pageParent:  4b
		size      :  2b
		pageNo    :  4b
		childPageNo: 4b
		id:          4b
	*/
	constructor(pageParent, pageNo) {
		if(typeof pageParent === 'number') {
			this.pageParent = pageParent;
		}

		if(typeof pageNo === 'number') {
			this.pageNo = pageNo;
			cache.set(pageNo, this);
		}

		this.data = Buffer.alloc(PAGE_SIZE);
		this.data.writeInt32LE(pageParent, 0);
		this.offset = PAGE_SIZE;
		this.size = 0;
	}

	getChildPageNo(id) {
		let cellByteSize = this.size * ONE_CELL_BYTES;
		// sizeOf(paegParent) + sizeOf(size) + sizeOf(pageNo)
		let cellByteBegin =
            (PAGEPARENT_BYTES_IN_CELL + SIZENO_BYTES_IN_CELL + PAGENO_BYTES);
		let cellByteBuffer = this.data.slice(cellByteBegin,
                cellByteBegin + cellByteSize);

		let minIndex = 0, maxIndex = this.size;
		let minId = cellByteBuffer.readInt32LE(0),
			maxId = cellByteBuffer.readInt32LE(
			    cellByteBegin + cellByteSize - ID_BYTES);

		if(minId > id || maxId < id) {
			return
		}

		while((maxIndex - minIndex) > 1) {
			let middle = (minIndex + maxIndex) >> 1;
			let middleId = cellByteBuffer.readInt32LE(
                cellByteBegin + middle * ONE_CELL_BYTES - ID_BYTES);
			if(middleId === id) {
				return cellByteBuffer.readInt32LE(
				    cellByteBegin + (middle-1) * ONE_CELL_BYTES);
			} else if(middleId > id) {
				maxIndex = middle;
			} else if(middleId < id) {
				minIndex = middle;
			}
		}

		return cellByteBuffer.readInt32LE(
		    cellByteBegin + (maxIndex-1) * ONE_CELL_BYTES);
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
		return this.type === 'LEAF';
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
		let free = this.freeData(), keySize = ByteSize(key);

		return free >= keySize
	}	

	getParentPage(pageNo) {
        this.pageParent = pageNo;
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

	getCellByOffset(offset) {
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
		return this.getCellByOffset(offset);
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
			// assert(compare(minCellInfo.key, key) <= 0);
			// assert(compare(maxCellInfo.key, key) >= 0);

			let middle = (minIndex + maxIndex) >> 1;
			let middleCellInfo = this.getCellInfoByIndex(middle);
			if(compare(middleCellInfo.key, key) > 0) {
				maxIndex = middle;
				continue;
			} else if(compare(middleCellInfo.key, key) === 0) {
				return { ... middleCellInfo, cellIndex: middle};
			} else {
				let middleNext = middle + 1;
				let middleNextCellInfo = this.getCellInfoByIndex(middleNext);
				if(compare(middleNextCellInfo.key, key) > 0) {
					return { ... middleNextCellInfo, cellIndex: middle};
				} else if(compare(middleNextCellInfo.key, key) === 0) {
					return {... middleNextCellInfo, cellIndex: middleNext};
				} else {
					minIndex = middle;
				}
			}
		}
	}

	static LoadPage(pageNo) {
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

	findId(key) {
		let cellInfo = this.__findNearestCellInfo(key);
		console.log("cellInfo", cellInfo)
		if(cellInfo && compare(cellInfo.key, key) === 0) {
			return cellInfo.id;
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
			return new Promise().resolve(this);
		}
		
		let cellInfo = this.__findNearestCellInfo(key);	
		let cachedPage = cache.get(cellInfo.childPageNo);
		if(cachedPage) {
			return new Promise().resolve(cachedPage);
		} else {
			return IndexPage.LoadPage(cellInfo.childPageNo);
		}
	}

	// split into half by a indexPair
	half(insertCellInfo) {
		let {key, id, childPageNo} = insertCellInfo;
		let tempArray = [];
		let totalSize = this.size + 1;
		for(var i = 0; i < totalSize; i ++) {
			let offset = this.__getOffsetByIndex(i);
			let cellInfo = this.getCellByOffset(offset);
			if(compare(cellInfo.key, key) < 0) {
				tempArray.push(cellInfo);
				if(i === this.size) {
					tempArray.push(insertCellInfo);
				}
			} else {
				tempArray.push(insertCellInfo);
				tempArray.push(cellInfo);
			}
		}

		let halfSize = totalSize >> 1;
		let splitInfo = [];
		for(var i = halfSize; i < totalSize; i ++) {
			splitInfo.push(tempArray[i]);
		}

		// rewrite the cells from begining
		this.offset = PAGE_SIZE;
		for(var i = 0; i < halfSize; i ++) {
			let cellInfo = tempArray[i];
			this.insertCell(cellInfo['key'], cellInfo['id'],
                cellInfo['childPageNo']);
		}

		return splitInfo;
	}
}

// let page = new DataPage(1);
// page.insertCell(1, 'stringDataPage1');
// page.insertCell(2, 'nodejsDataPage1');
// page.insertCell(3, 'javaDataPage1');
// page.flush()
// let page1 = DataPage.load(1, (err, page)=> {
// 	page.getCell(3)
// })

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
exports.PAEG_TYPE_LEAF = PAGE_TYPE_LEAF;

const st = function() {
	let st = fs.statSync(INDEXPATH);
	console.log('st.size:', st.size)
}
st();

cache.set('jva', {java: 'nodejs'});
console.log(cache.get('jva'))
