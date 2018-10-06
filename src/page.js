const fs = require('fs');
const assert = require('assert');
const path = require('path');
const Lru = require('lru-cache');
const {
  compare, ByteSize, IdCompare, CreateFileIfNotExist,
} = require('./utils.js');

const PAGE_SIZE = 1024;
exports.PAGE_SIZE = PAGE_SIZE;
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
const PAGE_TYPE_SIZE = 1; // the byteSize of the type
const PAGENO_BYTES = 4;

const cache = Lru(128 * 1024);
const DATACACHE = Lru(64 * 64 * 32);
const ID_CELL_BYTES_SIZE = 8;
const DATA_PAGE_HEADER_BYTES_SIZE = 8;
const DATA_CELL_MAX_SIZE = 256;
const DATA_CELL_SIZE_BYTES = 1;
const OFFSET_BYTES_SIZE = 2;

class DataPage {
  /**
   * B-tree:
   *  the data format is :
   *  -------------------------------------------------------------
   *  pageNo | size | offset | [idCell1...] | [dataCell1 ...]
   *  -------------------------------------------------------------
   *  idCell format is:
   *  idBuffer + dataOffset
   *
   * dataCell format is :
   * cellByteSize + rawData
   *
   * the size of the field above:
   * pageNo: 4b
   * size:   2b
   *  offset: 2b
   *  idBuffer:     6b
   *  dataOffset:   2b
   *  cellByteSize: 1b
   *  rawData: nb (n <= 256)
   *  */
  constructor(pageNo) {
    this.pageNo = pageNo; // start from zero
    this.data = Buffer.alloc(PAGE_SIZE);
    this.data.writeInt32LE(pageNo, 0);
    this.size = 0; // the size of data
    this.offset = PAGE_SIZE; // write data from bottom up

    DATACACHE.set(pageNo, this);
  }

  freeData() {
    return (this.offset - DATA_PAGE_HEADER_BYTES_SIZE - this.size * ID_CELL_BYTES_SIZE);
  }

  getPageNo() {
    return this.pageNo;
  }

  insertCell(id, cellData) {
    const dataSize = ByteSize(cellData) + DATA_CELL_SIZE_BYTES;
    const needByte = dataSize + ID_CELL_BYTES_SIZE;
    if ((dataSize - DATA_CELL_SIZE_BYTES) >= DATA_CELL_MAX_SIZE) {
      throw new Error('cannot insert data size large than 256 b!');
    }
    if (this.freeData() > needByte) {
      // write the cellData's size
      this.data.writeUInt16LE(dataSize - DATA_CELL_SIZE_BYTES,
        this.offset - dataSize);
      // write the cellData
      this.data.write(cellData, this.offset - dataSize + DATA_CELL_SIZE_BYTES);
      this.offset = this.offset - dataSize;

      this.data.writeUInt32LE(id.timeId,
        this.size * ID_CELL_BYTES_SIZE + DATA_PAGE_HEADER_BYTES_SIZE);
      this.data.writeInt16LE(id.count,
        this.size * ID_CELL_BYTES_SIZE + DATA_PAGE_HEADER_BYTES_SIZE + 4);
      this.data.writeUInt16LE(this.offset,
        this.size * ID_CELL_BYTES_SIZE + DATA_PAGE_HEADER_BYTES_SIZE + 6);
      this.size += 1;
      // write size
      this.data.writeUInt16LE(this.size, PAGENO_BYTES);
      // write offset
      this.data.writeUInt16LE(this.offset,
        PAGENO_BYTES + OFFSET_BYTES_SIZE);
      return true;
    }
    // has no room for cellData
    return false;
  }

  __formId(index) {
    const start = DATA_PAGE_HEADER_BYTES_SIZE + index * ID_CELL_BYTES_SIZE;
    const timeId = this.data.readUInt32LE(start);
    const count = this.data.readUInt16LE(start + 4);
    const offset = this.data.readUInt16LE(start + 4 + 2);
    return { timeId, count, offset };
  }

  __getDataByOffset(offset) {
    const dataSize = this.data.readUInt8(offset);
    const copyData = Buffer.from(this.data);
    const buffer = copyData.slice(offset + 1, offset + 1 + dataSize);
    return buffer.toString();
  }

  getCellData(idInfo) {
    const size = this.data.readUInt16LE(PAGENO_BYTES);

    let maxIdInfo = this.__formId(size - 1);
    let minIdInfo = this.__formId(0);

    if (IdCompare(idInfo, maxIdInfo) > 0 || IdCompare(idInfo, minIdInfo) < 0) {
      console.log('no data matched id:', idInfo);
    } else if (IdCompare(idInfo, maxIdInfo) === 0) {
      return this.__getDataByOffset(maxIdInfo.offset);
    } else if (IdCompare(idInfo, minIdInfo) === 0) {
      return this.__getDataByOffset(minIdInfo.offset);
    } else {
      let max = size - 1; let
        min = 0;
      while (max > min) {
        minIdInfo = this.__formId(min);
        maxIdInfo = this.__formId(max);
        if (IdCompare(idInfo, minIdInfo) === 0) {
          return this.__getDataByOffset(minIdInfo.offset);
        }
        if (IdCompare(idInfo, maxIdInfo) === 0) {
          return this.__getDataByOffset(maxIdInfo.offset);
        }
        if ((max - min) === 1) {
          return null;
        }
        const middle = (max + min) >> 1;
        const midIdInfo = this.__formId(middle);
        if (IdCompare(idInfo, midIdInfo) === 0) {
          return this.__getDataByOffset(midIdInfo.offset);
        } if (IdCompare(idInfo, midIdInfo) > 0) {
          min = middle;
        } else {
          max = middle;
        }
      }
    }
    return null;
  }

  flush(directory) {
    const filePath = path.join(directory, FILEPATH);
    return new Promise((resolve, reject) => {
      fs.open(filePath, 'a', (err, file) => {
        if (err) {
          reject(err);
        } else {
          fs.write(file, this.data, 0, PAGE_SIZE,
            this.pageNo * PAGE_SIZE, (error) => {
              if (error) {
                reject(error);
              } else {
                resolve();
              }
            });
        }
      });
    });
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
    const pageNo = this.data.readUInt32LE(0);
    const size = this.data.readUInt16LE(4);
    const offset = this.data.readUInt16LE(6);

    this.setPageNo(pageNo)
      .setSize(size)
      .setOffset(offset);
  }

  static async Load(directory, pageNo) {
    const filePath = path.join(directory, FILEPATH);
    if (!fs.existsSync(filePath)) {
      throw new Error('DataPage.load(), direcotry:%s not exits',
        filePath);
    }
    let page = DATACACHE.get(pageNo);
    if (page) {
      return page;
    }

    page = new DataPage(pageNo);
    const loadFromDisk = new Promise((resolve, reject) => {
      fs.open(filePath, 'r', (err, file) => {
        if (err) {
          reject(err);
        } else {
          fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
            (error) => {
              if (error) {
                reject(err);
              } else {
                page.__initPage();
                DATACACHE.set(pageNo, page);
                resolve(page);
              }
            });
        }
      });
    });

    const filledPage = await loadFromDisk;
    return filledPage;
  }

  static InitFile(directory) {
    const filePath = path.join(directory, FILEPATH);
    CreateFileIfNotExist(filePath);
  }

  static MaxPageNo(directory) {
    const filePath = path.join(directory, FILEPATH);
    if (fs.existsSync(filePath)) {
      const stat = fs.statSync(filePath);
      return stat.size / PAGE_SIZE;
    }
    return 0;
  }

  static CachePage() {
    return DATACACHE;
  }
}

const ONE_ID_CELL_BYTES = 10;
const SIZENO_BYTES_IN_CELL = 2;
const ID_BYTES = 6;
const ID_HEADER_PAGE_BYTES = 1 + PAGENO_BYTES * 4 + SIZENO_BYTES_IN_CELL;
class IdPage {
  /**
   * the format of this kind page is :
   * --------------------------------------------------------------------
   * | type |  pageParent  |  size  |  pageNo | prePageNo, nextPageNo |
   * [cell1, cell2, cell3, cell4 ...]
   * --------------------------------------------------------------------
   * every cell is : childPageNo + id
   * the size of the filed above is:
   * type：       1b
   * pageParent:  4b
   * size      :  2b
   * pageNo    :  4b
   * prePageNo:   4b
   * nextPageNo:  4b
   * childPageNo: 4b
   * id:          6b
   * */
  constructor(type, pageParent, pageNo) {
    this.data = Buffer.alloc(PAGE_SIZE);
    this.type = type;
    if (typeof pageParent === 'number') {
      this.pageParent = pageParent;
      this.data.writeUInt32LE(pageParent, PAGE_TYPE_SIZE);
    }

    if (typeof pageNo === 'number') {
      this.pageNo = pageNo;
      this.data.writeUInt32LE(pageNo, PAGE_TYPE_SIZE + PAGENO_BYTES + SIZENO_BYTES_IN_CELL);
      cache.set(pageNo, this);
    }

    if (type) {
      this.data.writeUInt8(type, 0);
    }
    this.size = 0;
    this.data.writeUInt16LE(this.size, PAGE_TYPE_SIZE + PAGENO_BYTES);
  }

  static InitFile(directory) {
    const filePath = path.join(directory, INDEXPATH);
    CreateFileIfNotExist(filePath);
  }

  // needStore 标识是否要回写this.data
  setType(type, needStore) {
    this.type = type;
    if (needStore) {
      this.data.writeUInt8(type, 0);
    }
    return this;
  }

  setPageParent(pageNo, needStore) {
    this.pageParent = pageNo;
    const start = PAGE_TYPE_SIZE;
    if (needStore) {
      this.data.writeUInt32LE(pageNo, start);
    }
    return this;
  }

  setSize(size, needStore) {
    this.size = size;
    const start = PAGE_TYPE_SIZE + PAGENO_BYTES;
    if (needStore) {
      this.data.writeUInt16LE(size, start);
    }
    return this;
  }

  setPageNo(pageNo, needStore) {
    this.pageNo = pageNo;
    const start = PAGE_TYPE_SIZE + PAGENO_BYTES + SIZENO_BYTES_IN_CELL;
    if (needStore) {
      this.data.writeUInt32LE(pageNo, start);
    }
    return this;
  }

  setPrePage(prePageNo, needStore) {
    this.prePageNo = prePageNo;
    const start = PAGE_TYPE_SIZE + PAGENO_BYTES * 2 + SIZENO_BYTES_IN_CELL;
    if (needStore) {
      this.data.writeUInt32LE(prePageNo, start);
    }
    return this;
  }

  setNextPage(nextPageNo, needStore) {
    this.nextPageNo = nextPageNo;
    const start = PAGE_TYPE_SIZE + PAGENO_BYTES * 3 + SIZENO_BYTES_IN_CELL;
    if (needStore) {
      this.data.writeUInt32LE(nextPageNo, start);
    }
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
    // sizeOf(type) +
    // sizeOf(paegParent, pageNo, prePage, nextPage) + sizeOf(size)
    const cellByteBegin = PAGE_TYPE_SIZE
            + PAGENO_BYTES * 4 + SIZENO_BYTES_IN_CELL;

    const timeId = this.data.readUInt32LE(cellByteBegin + index * ONE_ID_CELL_BYTES);
    const count = this.data.readUInt16LE(cellByteBegin + index * ONE_ID_CELL_BYTES + 4);
    const childPageNo = this.data.readUInt32LE(cellByteBegin + index * ONE_ID_CELL_BYTES + 4 + 2);

    return {
      id: { timeId, count },
      childPageNo,
    };
  }

  getChildPageNo(id) {
    let minIndex = 0; let
      maxIndex = this.size - 1;
    let minCellInfo = this.__getCellInfoByIndex(minIndex);


    let maxCellInfo = this.__getCellInfoByIndex(maxIndex);

    if (IdCompare(minCellInfo.id, id) > 0) {
      return null;
    }
    // rootPage只有一个cell, 满足该rootPage's id小于id,直接返回
    if (this.size === 1) {
      return minCellInfo.childPageNo;
    }
    if (this.isLeaf() && IdCompare(maxCellInfo.id, id) < 0) {
      return null;
    }

    while (maxIndex > minIndex) {
      minCellInfo = this.__getCellInfoByIndex(minIndex);
      maxCellInfo = this.__getCellInfoByIndex(maxIndex);
      if (IdCompare(minCellInfo.id, id) === 0) {
        return minCellInfo.childPageNo;
      }
      if (IdCompare(maxCellInfo.id, id) <= 0) {
        return maxCellInfo.childPageNo;
      }
      if ((maxIndex - minIndex) === 1) {
        if (IdCompare(minCellInfo.id, id) < 0
          && IdCompare(maxCellInfo.id, id) > 0) {
          return minCellInfo.childPageNo;
        }
        return null;
      }
      const middle = (minIndex + maxIndex) >> 1;
      const middleCellInfo = this.__getCellInfoByIndex(middle);
      if (this.isLeaf()) { // 精确查找 dataPageNo
        if (IdCompare(middleCellInfo.id, id) === 0) {
          return middleCellInfo.childPageNo;
        } if (IdCompare(middleCellInfo.id, id) > 0) {
          maxIndex = middle;
        } else if (IdCompare(middleCellInfo.id, id) < 0) {
          minIndex = middle;
        }
      } else { // 查找含有此id的ChildPageNo
        const middleNextCellInfo = this.__getCellInfoByIndex(middle + 1);
        if (IdCompare(middleCellInfo.id, id) <= 0
          && IdCompare(middleNextCellInfo.id, id) > 0) {
          return middleCellInfo.childPageNo;
        }
        if (IdCompare(middleCellInfo.id, id) > 0) {
          maxIndex = middle;
        } else {
          minIndex = middle;
        }
      }
    }
    return null;
  }

  // get the size of free room
  freeData() {
    return (PAGE_SIZE - ID_HEADER_PAGE_BYTES - this.size * ONE_ID_CELL_BYTES);
  }

  static getRootPage() {
    const pageOne = Buffer.alloc(PAGE_SIZE);
    return new Promise((resolve, reject) => {
      fs.open(INDEXPATH, 'r', (err, file) => {
        if (err) {
          reject(err);
        } else {
          fs.read(file, pageOne, 0, PAGE_SIZE, 0, (error) => {
            const rootPageNo = pageOne.readUInt32LE();
            if (error) {
              reject(error);
            } else {
              resolve(rootPageNo);
            }
          });
        }
      });
    });
  }

  static setRootPage(rootPageNo) {
    const pageData = Buffer.alloc(PAGE_SIZE);
    pageData.writeUInt32LE(rootPageNo, 0);

    fs.open(INDEXPATH, 'w', (err, file) => {
      fs.writeSync(file, pageData, 0, PAGE_SIZE, 0);
    });
  }

  increaseSize() {
    this.size += 1;
    this.setSize(this.size, true);
  }

  insertCell(id, childPageNo) {
    if (this.freeData() >= ONE_ID_CELL_BYTES) {
      const start = this.size * ONE_ID_CELL_BYTES + ID_HEADER_PAGE_BYTES;
      this.data.writeUInt32LE(id.timeId, start);
      this.data.writeUInt16LE(id.count, start + 4);
      this.data.writeUInt32LE(childPageNo, start + 4 + 2);
      this.increaseSize();
      return true;
    }
    return false;
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
    const dataBuffer = this.data;
    const type = dataBuffer.readUInt8(0);
    const pageParent = dataBuffer.readUInt32LE(PAGE_TYPE_SIZE);
    const size = dataBuffer.readUInt16LE(PAGE_TYPE_SIZE + PAGENO_BYTES);
    const pageNo = dataBuffer.readUInt32LE(PAGE_TYPE_SIZE + PAGENO_BYTES
      + SIZENO_BYTES_IN_CELL);
    const prePageNo = dataBuffer.readUInt32LE(PAGE_TYPE_SIZE
      + PAGENO_BYTES * 2 + SIZENO_BYTES_IN_CELL);
    const nextPageNo = dataBuffer.readUInt32LE(PAGE_TYPE_SIZE
      + PAGENO_BYTES * 3 + SIZENO_BYTES_IN_CELL);

    this.setType(type)
      .setPageParent(pageParent)
      .setSize(size)
      .setPageNo(pageNo)
      .setPrePage(prePageNo)
      .setNextPage(nextPageNo);
  }

  static Load(pageNo) {
    const filePath = path.join('js', INDEXPATH);
    return new Promise((resolve, reject) => {
      const cachedPage = cache.get(pageNo);
      if (cachedPage) {
        resolve(cachedPage);
      } else {
        const page = new IdPage();
        fs.open(filePath, 'r', (err, file) => {
          fs.read(file, page.data, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
            (error) => {
              if (error) {
                reject(error);
              } else {
                cache.set(pageNo, page);
                page.__initPage();
                resolve(page);
              }
            });
        });
      }
    });
  }

  static getPage(pageNo, cb) {
    const page = cache.get(pageNo);
    if (page) {
      cb(page);
    } else {
      IdPage.load(pageNo, cb);
    }
  }

  static getPageSize() {
    const stat = fs.statSync(INDEXPATH);
    return stat.size / PAGE_SIZE;
  }

  static getLeafNo() {
    const pageOne = Buffer.alloc(PAGE_SIZE);
    return new Promise((resolve, reject) => {
      fs.open(INDEXPATH, 'r', (err, file) => {
        if (err) {
          reject(err);
        } else {
          fs.read(file, pageOne, 0, PAGE_SIZE, 0, (error) => {
            const leafNo = pageOne.readUInt32LE(4);
            if (error) {
              reject(err);
            } else {
              resolve(leafNo);
            }
          });
        }
      });
    });
  }

  flush(directory) {
    const filePath = path.join(directory, INDEXPATH);
    return new Promise((resolve, reject) => {
      fs.open(filePath, 'a', (err, file) => {
        if (err) {
          reject(err);
        } else {
          fs.writeSync(file, this.data, 0, PAGE_SIZE,
            this.pageNo * PAGE_SIZE);
          resolve(null);
        }
      });
    });
  }
}

const INDEXPAGE_HEADER_SIZE = 1 + 4 + 2 + 4 + 2 + 4 + 4;
const CELLDATA_BYTE_SIZE = 2;
class IndexPage {
  /**
   * the format of this kind page is :
-------------------------------------------------------------------------
   | type |  pageParent  |  size  |  pageNo  | offset | prePageNo | nextPageNo
   [offset1, offset2, offset3 ......]
         ......
   [cell1, cell2, cell3......]
-------------------------------------------------------------------------
   * the size of the filed above is:
   * Header:
      type:        1b  // this page type
      pageParent:  4b  // this page's parent page number
      size:        2b  // the size of cell
      pageNo:      4b  // this page number
      offset:      2b  // where this page write from
      prePageNo:   4b  // the pre page of this page (same level)
      nextPageNo:  4b  // the next page of this page (same level)
   * offsets:
      offset:      2b  // pointer of the cellData

   * cellData:
      dataSize     2b  // the total size of the cell
      key:         nb  // the key content
      id:          6b  // the key pair <key, id>
      childPageNo: 4b  // cell pointers for its child page
   */
  constructor(type, pageParent, pageNo, bufferData) {
    this.data = bufferData || Buffer.alloc(PAGE_SIZE);
    if (!pageParent && !pageNo && !type) {
      return;
    }

    this.type = type;
    this.data.writeUInt8(type, 0);

    if (typeof pageParent === 'number') {
      this.pageParent = pageParent;
      this.data.writeUInt32LE(pageParent, 1);
    }
    if (typeof pageNo === 'number') {
      this.pageNo = pageNo;
      this.data.writeUInt32LE(pageNo, 7);
      cache.set(pageNo, this);
    }

    this.offset = PAGE_SIZE;
    this.data.writeUInt16LE(this.offset, 1 + 4 + 2 + 4);
    this.size = 0;
    this.data.writeUInt16LE(this.size, 5);
  }

  static oneCellBytes(key) {
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
    this.data.writeUInt8(type, 0);
    return this;
  }

  setPrePageNo(prePageNo) {
    this.prePageNo = prePageNo;
    this.data.writeUInt32LE(prePageNo, INDEXPAGE_HEADER_SIZE - 8);
    return this;
  }

  setNextPageNo(nextPageNo) {
    this.nextPageNo = nextPageNo;
    this.data.writeUInt32LE(nextPageNo, INDEXPAGE_HEADER_SIZE - 4);
    return this;
  }

  getPrePageNo() {
    return this.data.readUInt32LE(INDEXPAGE_HEADER_SIZE - 8);
  }

  getNextPageNo() {
    return this.data.readUInt32LE(INDEXPAGE_HEADER_SIZE - 4);
  }

  getNextPage() {
    const nextPageNo = this.getNextPageNo();
    return IndexPage.LoadPage(nextPageNo);
  }

  getPrePage() {
    const prePageNo = this.getPrePageNo();
    return IndexPage.LoadPage(prePageNo);
  }

  hasRoomForBytes(bytesNum) {
    const free = this.freeData();
    return free >= bytesNum;
  }

  // true: this page has room for key, false: not
  hasRoomFor(key) {
    const oneCellBytes = IndexPage.oneCellBytes(key) + 2;
    // 需要的空间为一个cell的空间和一个offset指向；
    return this.hasRoomForBytes(oneCellBytes);
  }

  setParentPage(pageNo) {
    this.pageParent = pageNo;
    this.data.writeUInt32LE(pageNo, 1);
    return this;
  }

  getParentPageNo() {
    return this.pageParent;
  }

  setPageNo(pageNo) {
    this.pageNo = pageNo;
    this.data.writeUInt32LE(pageNo, 1 + 4 + 2);
    return this;
  }

  setSize(size) {
    this.size = size;
    // the header is like: type(1b) + pageParent(4b) + size(2b) ...
    this.data.writeUInt16LE(size, 1 + 4);
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
    this.data.writeUInt16LE(offset, 1 + 4 + 2 + 4);
    return this;
  }

  getOffset() {
    return this.offset;
  }

  isRoot() {
    return this.type & PAGE_TYPE_ROOT;
  }

  isLeaf() {
    return this.type & PAGE_TYPE_LEAF;
  }

  /**
   * reuse page的数据结构：
   * +-------------------------------+
   * |type(1b) |next(4b)|  ...       |
   * +-------------------------------+
   *  @param nextReuseNo
   */
  transformToReuse(nextReuseNo) {
    this.setType(PAGE_TYPE_REUSE, true);
    this.nextReuseNo = nextReuseNo;
    this.data.writeUInt32LE(nextReuseNo, 1);
  }

  static LoadAsReuse(pageNo) {
    return IndexPage.__loadRawPage(pageNo, (dataBuffer) => {
      const reuseIndexPage = new IndexPage().setType(PAGE_TYPE_REUSE);
      reuseIndexPage.nextReuseNo = dataBuffer.readUInt32LE(1);
      cache.set(pageNo, reuseIndexPage);
      return reuseIndexPage;
    });
  }

  // 只在type为PAGE_TYPE_REUSE时候才调用
  reUseNext() {
    const nextReuseNo = this.data.readUInt32LE(1);
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
  getCellByOffset(offset) {
    const dataSize = this.data.readUInt16LE(offset);
    const data = this.data.slice(
      offset + CELLDATA_BYTE_SIZE,
      dataSize + offset + CELLDATA_BYTE_SIZE,
    );
    const keySize = dataSize - ID_BYTES - PAGENO_BYTES;
    const keyBuffer = data.slice(0, keySize);
    const key = keyBuffer.toString();

    const idBuffer = data.slice(keySize, ID_BYTES + keySize);
    const id = { timeId: null, count: null };
    id.timeId = idBuffer.readUInt32LE(0);
    id.count = idBuffer.readUInt16LE(4);

    const childPageNoBuffer = data.slice(
      keySize + ID_BYTES, PAGENO_BYTES + ID_BYTES + keySize,
    );
    const childPageNo = childPageNoBuffer.readUInt32LE(0);

    return { key, id, childPageNo };
  }

  resortOffsetArray(offset, position) {
    assert(position >= 0 && this.size >= position);

    const dataCopy = Buffer.from(this.data);
    // this.data.slice share the same buffer with this.data
    const halfBuffer = dataCopy.slice(INDEXPAGE_HEADER_SIZE + position * 2,
      INDEXPAGE_HEADER_SIZE + this.size * 2);
    this.data.writeUInt16LE(offset, INDEXPAGE_HEADER_SIZE + position * 2);
    halfBuffer.copy(
      this.data,
      INDEXPAGE_HEADER_SIZE + position * 2 + 2,
      0,
      (this.size - position) * 2,
    );
    this.size += 1;
    this.setSize(this.size);
  }

  // position starts from 0
  __getOffsetByIndex(position) {
    return this.data.readUInt16LE(INDEXPAGE_HEADER_SIZE + position * 2);
  }

  getCellInfoByIndex(index) {
    const offset = this.__getOffsetByIndex(index);
    return this.getCellByOffset(offset, index);
  }

  __rewritePage(cells) {
    this.setSize(0);
    this.setOffset(PAGE_SIZE);

    for (const cell of cells) { // eslint-disable-line
      const { key, id, childPageNo } = cell;
      this.insertCell(key, id, childPageNo);
    }
  }

  /**
   *  判断没有key的情况下，该page是否是小于或者大于一半；
   *  由于key不是定长，而且page由一整page分裂成两半，
   *  所以这里判断是否大于一半，也是根据其和其相邻page能否被合并成一个page判断；
   *  如果能被合并成一个page页面，则返回true，否则返回false;
   *  这里做相应的简化：
   *  a: 最左的page和右侧的邻page匹配判断
   *  b: 其他的page和左侧的邻page匹配判断
   *  @param key
   */
  async isLessHalfWithoutKey(key) {
    const cellSize = IndexPage.oneCellBytes(key);

    let adjacentPageNo;
    if (this.isLeftMost()) {
      adjacentPageNo = this.getNextPageNo();
    } else {
      adjacentPageNo = this.getPrePageNo();
    }
    const adjacentPage = await IndexPage.LoadPage(adjacentPageNo);
    const adjacentCellsBytes = adjacentPage.cellsBytes();
    // todo: adjacentCellsBytes小于cellSize
    const needBytes = adjacentCellsBytes - cellSize;

    return this.hasRoomForBytes(needBytes);
  }

  deleteCellByIndex(index) {
    const filtered = [];
    for (let i = 0; i < this.size; i += 1) {
      if (i !== index) {
        filtered.push(this.getCellInfoByIndex(i));
      }
    }
    this.__rewritePage(filtered);
  }

  updateCellInfo(cellInfo, index) {
    const filtered = [];
    for (let i = 0; i < this.size; i += 1) {
      if (i === index) {
        filtered.push(cellInfo);
      } else {
        filtered.push(this.getCellInfoByIndex(i));
      }
    }

    this.__rewritePage(filtered);
  }

  // 返回所有的cells信息
  allCells() {
    const all = [];
    for (let i = 0; i < this.size; i += 1) {
      all.push(this.getCellInfoByIndex(i));
    }
    return all;
  }

  batchInsertCells(cells) {
    for (const cell of cells) { // eslint-disable-line
      const { key, id, childPageNo } = cell;
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
  // todo 跨page查找
  __theLeftMostEqual(start, key) {
    let cellInfo = this.getCellInfoByIndex(start);
    let beginIndex = start;
    while (compare(key, cellInfo.key) === 0) {
      if (beginIndex === 0) {
        return { ...cellInfo, cellIndex: 0 };
      }
      beginIndex -= 1;
      cellInfo = this.getCellInfoByIndex(beginIndex);
    }

    return { ...this.getCellInfoByIndex(beginIndex + 1), cellIndex: beginIndex + 1 };
  }

  // 找到最右和key一样大小的cellInfo
  // todo 跨page查找
  __theRightMostEqual(start, key) {
    let cellInfo = this.getCellInfoByIndex(start);
    let beginIndex = start;
    while (compare(key, cellInfo.key) === 0) {
      if (beginIndex === this.size - 1) {
        return { ...cellInfo, cellIndex: this.size - 1 };
      }
      beginIndex += 1;
      cellInfo = this.getCellInfoByIndex(beginIndex);
    }

    return { ...this.getCellInfoByIndex(beginIndex - 1), cellIndex: beginIndex - 1 };
  }

  /**
   * isRightMost为false时，获取左侧最接近的cell；
   * 如 cells如下： (key1 < key2 < key3 < ...)
   * ---------------------------------------------------------------------
   * [key1, id1], [key1, id2], [key2, id3], [key2, id4], [key3, id4]......
   * ---------------------------------------------------------------------
   * 如 __findNearestCellInfo(key1)则返回的为id1;
   *    __findNearestCellInfo(key2) 返回的为id3;
   * isRight为true时， 返回右侧最接近的cell
   * __findNearestCellInfo(key1, true) 返回为id2
   * __findNearestCellInfo(key2, true) 返回为id4
   * @param key, isRightMost 为true的时候
   * @returns {*}
   * @private
   */
  findNearestCellInfo(key, isRightMost) {
    if (this.size === 0) {
      return null;
    }
    if (this.size === 1) {
      const onlyCellInfo = this.getCellInfoByIndex(0);
      if (compare(key, onlyCellInfo.key) === 0) {
        return onlyCellInfo;
      }
    }

    let minIndex = 0; let
      maxIndex = this.size - 1;
    while (minIndex < maxIndex) {
      const minCellInfo = this.getCellInfoByIndex(minIndex);
      const maxCellInfo = this.getCellInfoByIndex(maxIndex);

      if (minIndex + 1 === maxIndex
        && compare(key, minCellInfo.key) > 0
        && compare(key, maxCellInfo.key) < 0
      ) {
        return { ...minCellInfo, cellIndex: minIndex };
      }

      const middle = (minIndex + maxIndex) >> 1;
      const middleCellInfo = this.getCellInfoByIndex(middle);
      if (compare(key, maxCellInfo.key) >= 0) {
        // return {... maxCellInfo, cellIndex: maxIndex};
        if (compare(key, maxCellInfo.key) > 0) {
          return { ...maxCellInfo, cellIndex: maxIndex };
        }
        return this.__theLeftMostEqual(maxIndex, key);
      }
      if (compare(key, minCellInfo.key) <= 0) {
        return { ...minCellInfo, cellIndex: 0 };
      }
      if (compare(middleCellInfo.key, key) > 0) {
        maxIndex = middle;
      } else if (compare(middleCellInfo.key, key) === 0) {
        return isRightMost ? this.__theRightMostEqual(middle, key)
          : this.__theLeftMostEqual(middle, key);
      } else {
        const middleNext = middle + 1;
        const middleNextCellInfo = this.getCellInfoByIndex(middleNext);
        if (compare(middleNextCellInfo.key, key) > 0) {
          return { ...middleCellInfo, cellIndex: middle };
        }
        minIndex = middle;
      }
    }
    return null;
  }

  collectAllEqualIds(key) {
    const result = [];
    const leftMost = this.findNearestCellInfo(key);
    if (leftMost) {
      let startCellInfo = leftMost;
      let startIndex = leftMost.cellIndex;
      do {
        result.push(startCellInfo);
        if (startIndex === this.size - 1) {
          return result;
        }
        startIndex += 1;
        startCellInfo = {
          ...this.getCellInfoByIndex(startIndex),
          cellIndex: startIndex,
        };
      } while (compare(key, startCellInfo.key) === 0);
    }

    return result;
  }

  isLastCellInfo(cellIndex) {
    return this.size === cellIndex;
  }

  static __loadRawPage(pageNo, fill) {
    const cachedPage = cache.get(pageNo);
    if (cachedPage) {
      return Promise.resolve(cachedPage);
    }

    return new Promise((resolve, reject) => {
      fs.open('js/js.index', 'r', (err, file) => {
        if (err) {
          reject(err);
        } else {
          const dataBuffer = Buffer.alloc(PAGE_SIZE);
          fs.read(file, dataBuffer, 0, PAGE_SIZE, pageNo * PAGE_SIZE,
            (error) => {
              if (error) {
                reject(err);
              } else {
                resolve(fill(dataBuffer));
              }
            });
        }
      });
    });
  }

  static LoadPage(pageNoN) {
    return this.__loadRawPage(pageNoN, (dataBuffer) => {
      const page = new IndexPage(null, null, null, dataBuffer);
      const type = dataBuffer.readUInt8(0);
      page.setType(type);
      const pageParent = dataBuffer.readUInt32LE(1);
      page.setParentPage(pageParent);
      const size = dataBuffer.readUInt16LE(1 + 4);
      page.setSize(size);
      const pageNo = dataBuffer.readUInt32LE(1 + 4 + 2);
      page.setPageNo(pageNo);
      const offset = dataBuffer.readUInt16LE(1 + 4 + 2 + 4);
      page.setOffset(offset);
      const prePageNo = dataBuffer.readUInt32LE(INDEXPAGE_HEADER_SIZE - 8);
      page.setPrePageNo(prePageNo);
      const nextPageNo = dataBuffer.readUInt32LE(INDEXPAGE_HEADER_SIZE - 4);
      page.setNextPageNo(nextPageNo);
      cache.set(pageNo, page);
      return page;
    });
  }

  getPageParent() {
    const parentNo = this.getParentPageNo();
    return IndexPage.LoadPage(parentNo);
  }

  insertCell(key, id, childPageNo) {
    const keyByteSize = ByteSize(key);
    const totalByteSize = CELLDATA_BYTE_SIZE + keyByteSize + ID_BYTES + PAGENO_BYTES;
    this.offset -= totalByteSize;
    // 先更新this.offset
    this.setOffset(this.offset);

    if (this.size > 20) {
      console.log(this);
    }
    this.data.writeUInt16LE(totalByteSize - CELLDATA_BYTE_SIZE,
      this.offset);
    this.data.write(key, this.offset + CELLDATA_BYTE_SIZE);
    // 依次写入id信息 timeId: 4b, count: 2b
    this.data.writeUInt32LE(id.timeId,
      this.offset + CELLDATA_BYTE_SIZE + keyByteSize);
    this.data.writeUInt16LE(id.count,
      this.offset + CELLDATA_BYTE_SIZE + keyByteSize + 4);
    this.data.writeUInt32LE(childPageNo,
      this.offset + CELLDATA_BYTE_SIZE + keyByteSize + ID_BYTES);

    if (this.size > 0) {
      if (this.size === 1) {
        const onlyKey = this.getCellInfoByIndex(0).key;
        if (compare(key, onlyKey) > 0) {
          this.resortOffsetArray(this.offset, 1);
        } else {
          this.resortOffsetArray(this.offset, 0);
        }
        return;
      }

      // let nearestCellInfo = this.__findNearestCellInfo(key);
      // console.log('nearestCellInfo', nearestCellInfo)
      // this.resortOffsetArray(this.offset, nearestCellInfo.cellIndex);

      let minIndex = 0; let
        maxIndex = (this.size - 1);
      while (maxIndex > minIndex) {
        const minKey = this.getCellInfoByIndex(minIndex).key;
        const maxKey = this.getCellInfoByIndex(maxIndex).key;
        if (compare(minKey, key) > 0) { // key is smaller than minKey
          this.resortOffsetArray(this.offset, minIndex);
          return;
        } if (compare(key, maxKey) >= 0) {
          this.resortOffsetArray(this.offset, maxIndex + 1);
          return;
        }
        const middleIndex = (minIndex + maxIndex) >> 1;
        const middleKey = this.getCellInfoByIndex(middleIndex).key;
        const nextKey = this.getCellInfoByIndex(middleIndex + 1).key;
        // find the correct position
        if (compare(nextKey, key) > 0
                        && compare(middleKey, key) <= 0) {
          this.resortOffsetArray(this.offset, middleIndex + 1);
          return;
        }

        if (compare(middleKey, key) > 0) {
          maxIndex = middleIndex;
        } else {
          minIndex = middleIndex;
        }
      }
    } else {
      this.size += 1;
      this.setSize(this.size);
      this.data.writeUInt16LE(this.offset, INDEXPAGE_HEADER_SIZE);
    }
  }

  flush(directory) {
    const filePath = path.join(directory, INDEXPATH);
    return new Promise((resolve, reject) => {
      fs.open(filePath, 'a', (err, file) => {
        if (err) {
          reject(err);
        }

        fs.write(file, this.data, 0, PAGE_SIZE, this.pageNo * PAGE_SIZE, (error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
        resolve(null);
      });
    });
  }

  static FlushPageToDisk(directory, pageBuffer, pageNo) {
    const filePath = path.join(directory, INDEXPATH);
    return new Promise((resolve, reject) => {
      fs.open(filePath, 'a', (err, file) => {
        if (err) {
          reject(err);
        } else {
          fs.write(file, pageBuffer, 0, PAGE_SIZE, pageNo * PAGE_SIZE, (error) => {
            if (error) {
              reject(error);
            } else {
              resolve(null);
            }
          });
        }
      });
    });
  }

  getNearestChildPage(key) {
    if (this.type & PAGE_TYPE_LEAF) {
      return Promise.resolve(this);
    }
    const cellInfo = this.findNearestCellInfo(key);
    if (!cellInfo) {
      console.log('cellinfo', key);
    }

    return IndexPage.LoadPage(cellInfo.childPageNo);
  }

  findCorrectCellInfo(key, id) {
    let startIndex = this.findNearestCellInfo(key);
    let startCellInfo;
    const startPage = this;
    // todo 跨page的查找
    do {
      startCellInfo = this.getCellInfoByIndex(startIndex);
      if (compare(startCellInfo.key, key) === 0
        && IdCompare(startCellInfo.id, id) === 0) {
        return Object.assign(startCellInfo, { cellIndex: startIndex });
      }
      startIndex += 1;
    } while (startIndex < startPage.getSize());
    return null;
  }

  // split into half by a indexPair
  half(insertCellInfo) {
    const { key } = insertCellInfo;
    const tempArray = [];
    for (let i = 0; i < this.size; i += 1) {
      const cellInfo = this.getCellInfoByIndex(i);
      if (i === 0) {
        if (compare(cellInfo.key, key) >= 0) {
          tempArray.push(insertCellInfo);
        }
      }
      tempArray.push(cellInfo);
      if (i < this.size - 1) {
        const nextInfo = this.getCellInfoByIndex(i + 1);
        if (compare(cellInfo.key, key) < 0
          && compare(nextInfo.key, key) >= 0) {
          tempArray.push(insertCellInfo);
        }
      }
      if (i === this.size - 1) {
        if (compare(cellInfo.key, key) <= 0) {
          tempArray.push(insertCellInfo);
        }
      }
    }

    const halfSize = (this.size + 1) >> 1;
    const splitInfo = [];
    for (let i = halfSize; i <= this.size; i += 1) {
      splitInfo.push(tempArray[i]);
    }

    // rewrite the cells from begining
    this.offset = PAGE_SIZE;
    this.size = 0;
    for (let i = 0; i < halfSize; i += 1) {
      const cellInfo = tempArray[i];
      this.insertCell(cellInfo.key, cellInfo.id,
        cellInfo.childPageNo);
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
