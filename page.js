const fs = require('fs');

const Lru = require('lru-cache');

const PAGE_SIZE = 1024;
const FILEPATH = 'js.db';
const INDEXPATH = 'js.index';

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
		const stat = fs.statcSync(FILEPATH);
		return stat.size / PAGE_SIZE;
	}
}

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
		let cellByteSize = this.size * 8;
		// sizeOf(paegParent) + sizeOf(size) + sizeOf(pageNo)
		let cellByteBegin = (4 + 2 + 4);
		let cellByteBuffer = Buffer.from(this.data, cellByteBegin, cellByteBegin + cellByteBuffer);

		let minIndex = 0, maxIndex = this.size;
		let minId = cellByteBuffer.readInt32LE(),
			maxId = cellByteBuffer.readInt32LE(cellByteBegin + cellByteSize - 4);

		if(minId > id || maxId < id) {
			return
		}

		while((maxIndex - minIndex) > 1) {
			let middle = Math.floor((minIndex + maxIndex) / 2);
			let middleId = cellByteBuffer.readInt32LE(cellByteBegin + middle * 8 - 4);
			if(middleId === id) {
				return cellByteBuffer.readInt32LE(cellByteBegin + middle * 8 - 8);
			} else if(middleId > id) {
				maxIndex = middle;
			} else if(middleId < id) {
				minIndex = middle;
			}
		}

		return cellByteBuffer.readInt32LE(cellByteBegin + maxIndex * 8 - 8);
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
				let rootPageNo = data.readInt32LE();
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
				let leafNo = data.readInt32LE(4);
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

const st = function() {
	let st = fs.statSync(INDEXPATH);
	console.log('st.size:', st.size)
}
st();
