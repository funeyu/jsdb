const fs = require('fs');

const Lru = require('lru-cache');
const {compare} = require('./utils.js')

const PAGE_SIZE = 1024;
const FILEPATH = 'js.db';
const INDEXPATH = 'js.index';
const PAGE_TYPE_ID = 1;
const PAGE_TYPE_INDEX = 2;
const PAGE_TYPE_INDEX_LEAF = 4;
const PAGE_TYPE_INDEX_ROOT = 8;
const PAGE_TYPE_INDEX_ROOT_LEAF = 12;

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

const cache = Lru(64);

class DataPage {
	constructor(pageNo) {
		this.pageNo = pageNo;      	// start from zero
		this.data = Buffer.alloc(PAGE_SIZE);
		this.size = 0;				// the size of data
		this.offset = PAGE_SIZE;  	// write data from bottom up

		cache.put(pageNo + '_Data', this);
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
			fs.read(file, this.data, 0, PAGE_SIZE, this.pageNo * PAGE_SIZE, (err, data)=> {
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
			fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE, (err, data)=> {
				cache.put(pageNo + '_Data', page);
				
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
		-----------------------------------------------------------------------
		|  pageParent  |  size  |  pageNo | [cell1, cell2, cell3, cell4 ......]
		-----------------------------------------------------------------------
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
		let cellByteBegin = (PAGEPARENT_BYTES_IN_CELL + SIZENO_BYTES_IN_CELL + PAGENO_BYTES);
		let cellByteBuffer = Buffer.from(this.data, cellByteBegin, cellByteBegin + cellByteBuffer);

		let minIndex = 0, maxIndex = this.size;
		let minId = cellByteBuffer.readInt32LE(),
			maxId = cellByteBuffer.readInt32LE(cellByteBegin + cellByteSize - ID_BYTES);

		if(minId > id || maxId < id) {
			return
		}

		while((maxIndex - minIndex) > 1) {
			let middle = (minIndex + maxIndex) >> 1;
			let middleId = cellByteBuffer.readInt32LE(cellByteBegin + middle * ONE_CELL_BYTES - ID_BYTES);
			if(middleId === id) {
				return cellByteBuffer.readInt32LE(cellByteBegin + (middle-1) * ONE_CELL_BYTES);
			} else if(middleId > id) {
				maxIndex = middle;
			} else if(middleId < id) {
				minIndex = middle;
			}
		}

		return cellByteBuffer.readInt32LE(cellByteBegin + (maxIndex-1) * ONE_CELL_BYTES);
	}

	getPageNo() {
		return this.pageNo;
	}

	getPageParent() {
		return this.paegParent;
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
		pageData.writeInt32LE(rootPageNo);

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
			fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE, (err, data)=> {
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
class IndexPage {
	/*
		the format of this kind page is :
		--------------------------------------------------------------------------------------
		| type |  pageParent  |  size  |  pageNo  | offset | [cell1, cell2, cell3, cell4 ......]
		--------------------------------------------------------------------------------------
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
		this.type = type;
		this.data.writeInt8(type);

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

	getCellByOffset(offset) {
		let dataSize = this.data.readInt16LE(offset);
		let data = Buffer.from(this.data, offset, dataSize);
		let keySize = dataSize - ID_BYTES - PAGENO_BYTES;
		let key = Buffer.from(data, 0, keySize).toString();
		let id = Buffer.from(data, keySize, ID_BYTES).toString();
		let childPageNo = Buffer.from(data, keySize + ID_BYTES, PAGENO_BYTES).toString();

		return {key, id, childPageNo}
	}

	getChildPageNo(key) {
		if(this.type === PAGE_TYPE_INDEX_LEAF) {
			reuturn this
		}

	}

	getType() {
		return this.type;
	}

	// true: this page has room for key, false: not 
	hasRoomFor(key) {
		let free = this.freeData, keySize = ByteSize(key);

		return free >= keySize
	}	

	resortOffsetArray(offset, position) {
		assert(position >= 0 && this.size >= position);
		let halfBuffer = Buffer.from(this.data, 
			INDEXPAGE_HEADER_SIZE + position * 2,
			(this.size - position) * 2);

		this.data.writeInt16LE(offset, INDEXPAGE_HEADER_SIZE + position * 2);
		halfBuffer.copy(this.data, position*2 + 2, 0, (this.size - position) * 2);
	}

	// position starts from 0
	__getOffsetByIndex(position) {
		return this.data.readInt16LE(INDEXPAGE_HEADER_SIZE + position * 2);
	}

	insertCell(key, id, childPageNo) {
		let keyByteSize = ByteSize(key);
		this.offset -= (keyByteSize + ID_BYTES + PAGENO_BYTES);
		this.data.write(key, this.offset);
		this.data.writeInt32LE(id, this.offset + keyByteSize);
		this.data.writeInt32LE(childPageNo, this.offset + keyByteSize + ID_BYTES);

		if(this.size > 0) {
			let minIndex = 0, maxIndex = (this.size - 1);

			while(maxIndex > minIndex) {
				let minOffset = this.__getOffsetByIndex(minIndex),
			 	maxOffset = this.__getOffsetByIndex(maxIndex);

				let minKey = this.getCellByOffset(minOffset)['key'];
				let maxKey = this.getCellByOffset(maxOffset)['key'];
				if(compare(minKey, key) > 0) { // key is smaller than minKey
					this.resortOffsetArray(offset, minIndex);
					return ;
				} else if(compare(key, maxKey)) {
					this.resortOffsetArray(offset, maxIndex);
					return ;
				} else {
					let nextOffset = this.__getOffsetByIndex(minIndex + 1);
					let nextKey = this.getCellByOffset(nextOffset)['key'];
					let middleIndex = this.size >>1;
					let middleOffset = this.__getOffsetByIndex(middleIndex);
					let middleKey = this.getCellByOffset(middleOffset)['key'];

					if(compare(nextKey, key) >= 0) { // find the correct position
						this.resortOffsetArray(offset, minIndex);
						return ;
					}

					if(compare(middleKey, key) > 0) {
						maxIndex = middleIndex;
					} else {
						minIndex = middleIndex;
					}

				}
			}
			
		}
	}

	half() {
		let halfSize = this.size >> 1;
		let splitInfo = [];
		for(var i = halfSize; i < this.size; i ++) {
			let offset = this.__getOffsetByIndex(i);
			let cellInfo = this.getCellByOffset(offset);
			splitInfo.append(cellInfo);
		}

		// rewrite the cells from begining
		this.offset = PAGE_SIZE;
		for(var i = 0; i < halfSize; i ++) {
			let offset = this.__getOffsetByIndex(i);
			let cellInfo = this.getCellByOffset(offset);
			this.insertCell(cellInfo['key'], cellInfo['id'], cellInfo['childPageNo'])
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
exports.PAGE_TYPE_INDEX_ROOT_LEAF = PAGE_TYPE_INDEX_ROOT_LEAF;
exports.PAGE_TYPE_INDEX_ROOT = PAGE_TYPE_INDEX_ROOT;
exports.PAGE_TYPE_INDEX_LEAF = PAGE_TYPE_INDEX_LEAF;

const st = function() {
	let st = fs.statSync(INDEXPATH);
	console.log('st.size:', st.size)
}
st();
