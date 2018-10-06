const fs = require('fs');
const path = require('path');

const {
  IdPage, IndexPage, PAGE_TYPE_ID, PAGE_TYPE_INDEX, PAGE_TYPE_ROOT, PAGE_TYPE_INTERNAL,
  PAGE_TYPE_LEAF, PAGE_SIZE, INDEXPATH,
} = require('./page.js');
const { MIN_KEY, MIN_ID } = require('./constants');
const {
  compare, IdCompare, hash, ByteSize,
} = require('./utils.js');

const REUSE_LIST_BYTES = 8;
const ID_BTREE_META_BYTES = 8;
const ID_BTREE_META_HEADER = 8 + 8 + 4 + 1 + 1 + 2;
const PAGE_NO_BYTES = 4;

/**
 * Btree tree的元信息， 占据索引文件的第一个page, 用户索引的key大小不能大于32b
 * 其结构为：
 *  -----------------------------------ID_BTREE_META_HEADER--------------------
 *          reuseList | idBtreeMeta | maxPageNo| btreeSize | slotSize | offset
 *         [ offset1, offset2, ] ...... [ meta1, meta2, ]
 *  ---------------------------------------------------------------------------
 *
 *  reuseList(索引页随着删除引起的合并操作，使得某些page被置空可被再次回收使用) 的数据结构:
 *  first(4b)+last(4b)
 *  每添加一个reusePage修改first, 修改first的next指向：
 *  如：
 *      first             last
         +                +
         v                v
        15+-->13+-->28+-->87
 新增一个reusePage(26)其结构变为：
       first                  last
         +                      +
         v                      v
        26+-->15+-->13+-->28+-->87

 *  idBreeMeta(8b)的数据结构：
 *    rootPageNo(4b)+workingPageNo(4b)
 *  maxPageNo(4b):
 *    标识索引文件最大的页码,新建btree Page的时候都会自增1;
 *  btreeSize(1b):
 *  存储btree索引树的size
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
    const reuseListFirst = pageBuffer.readUInt32LE(0);
    const reuseListLast = pageBuffer.readUInt32LE(4);

    this.reuseList = {
      first: reuseListFirst,
      last: reuseListLast,
    };

    const idBtreeMetaRootPageNo = pageBuffer.readUInt32LE(REUSE_LIST_BYTES);
    const idBtreeMetaWorkingPageNo = pageBuffer.readUInt32LE(REUSE_LIST_BYTES + 4);
    this.idBtreeMeta = {
      rootPageNo: idBtreeMetaRootPageNo,
      workingPageNo: idBtreeMetaWorkingPageNo,
    };
    this.maxPageNo = pageBuffer.readUInt32LE(REUSE_LIST_BYTES + ID_BTREE_META_BYTES);
    this.btreeSize = pageBuffer.readUInt8(REUSE_LIST_BYTES + ID_BTREE_META_BYTES
        + PAGE_NO_BYTES);
    this.slotSize = pageBuffer.readUInt8(ID_BTREE_META_HEADER - 3);
    if (!this.slotSize) {
      // 如果slotSize为空, 则置初始值 2^3
      this.slotSize = 8;
    }
    this.data.writeUInt8(this.slotSize, ID_BTREE_META_HEADER - 3);
    this.offset = this.data.readUInt16LE(ID_BTREE_META_HEADER - 2);
    if (this.offset === 0) {
      // 初始状态 offset为最大值
      this.offset = PAGE_SIZE;
      this.data.writeUInt16LE(PAGE_SIZE, ID_BTREE_META_HEADER - 2);
    }
  }

  static LoadFromDisk(directory) {
    // 读取首页
    const page0Buffer = Buffer.alloc(PAGE_SIZE);
    const filePath = path.join(directory, INDEXPATH);
    return new Promise((resolve, reject) => {
      fs.open(filePath, 'r', (err, file) => {
        if (err) {
          reject(err);
          return;
        }
        fs.read(file, page0Buffer, 0, PAGE_SIZE, 0,
          (error) => {
            if (error) {
              reject(err);
            } else {
              resolve(new BtreeMeta(page0Buffer));
            }
          });
      });
    });
  }

  addReuseList(pageNo) {
    let { first, last } = this.reuseList;
    if (!first) {
      first = pageNo;
      last = pageNo;
      this.reuseList = {
        first,
        last,
      };
      this.data.writeUInt32LE(first, 0);
      this.data.writeUInt32LE(last, 4);
    } else {
      first = pageNo;
      this.reuseList.first = first;
      this.data.writeUInt32LE(first, 0);
    }
  }

  getReuseListFirst() {
    const { first } = this.reuseList;
    return first;
  }

  updateReuseList(first, last) {
    this.reuseList = {
      first,
      last,
    };

    this.data.writeUInt32LE(first, 0);
    this.data.writeUInt32LE(last, 4);
  }

  async fetchReuse() {
    let { first } = this.reuseList;
    const { last } = this.reuseList;
    if (first === last) {
      // reuseList置空
      this.updateReuseList(0, 0);
      if (first === 0) {
        return Promise.resolve(false);
      }
      return IndexPage.LoadAsReuse(first);
    }

    const reusePage = await IndexPage.LoadAsReuse(first);
    this.reuseList.first = reusePage.reUseNext();
    // eslint-disable-next-line
    first = this.reuseList.first;
    this.updateReuseList(first, last);

    return reusePage;
  }

  // 返回所有的key
  allKeys() {
    let beginOffset = PAGE_SIZE;
    // 先粗暴的将所有的key先收集起来, 再重新添加
    const keys = [];
    while (beginOffset > this.offset) {
      const rootPageNo = this.data.readUInt32LE(beginOffset - 4);
      const keySize = this.data.readUInt8(beginOffset - 7);
      const key = this.data.slice(beginOffset - 7 - keySize,
        beginOffset - 7).toString();
      keys.push({
        key,
        rootPageNo,
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
    this.data.writeUInt8(size, REUSE_LIST_BYTES + ID_BTREE_META_BYTES + PAGE_NO_BYTES);
    return this;
  }

  setMaxPageNo(pageNo) {
    this.maxPageNo = pageNo;
    this.data.writeUInt32LE(pageNo, REUSE_LIST_BYTES + ID_BTREE_META_BYTES);
    return this;
  }

  increaseMaxPageNo() {
    this.maxPageNo = this.maxPageNo + 1;
    this.setMaxPageNo(this.maxPageNo);
    return this.maxPageNo;
  }

  setIdBtreeMeta(idBtreeMeta) {
    this.idBtreeMeta = idBtreeMeta;
    this.data.writeUInt32LE(idBtreeMeta.rootPageNo, REUSE_LIST_BYTES);
    this.data.writeUInt32LE(idBtreeMeta.workingPageNo, REUSE_LIST_BYTES + 4);
    return this;
  }

  setIdBtreeWorkingPage(pageNo) {
    this.idBtreeMeta.workingPageNo = pageNo;
    this.data.writeUInt32LE(pageNo, REUSE_LIST_BYTES + 4);
  }

  setRootPageNo(pageNo) {
    this.idBtreeMeta.rootPageNo = pageNo;
    this.data.writeUInt32LE(pageNo, REUSE_LIST_BYTES);
  }

  getMaxPageNo() {
    return this.maxPageNo;
  }

  //= =====================================================================
  // 以下实现一个简易的基于哈希索引的存储, 用来存储用户定义的索引信息
  // 根据key查找用户设置的btree的rootPageNo, 是hash表查询方式
  getIndexRootPageNo(key) {
    // eslint-disable-next-line
    const metaInfo = this.__getMetaInfoByKey(key);
    if (metaInfo) {
      return metaInfo.rootPageNo;
    }
    return null;
  }

  __getMetaInfoByKey(key) {
    // eslint-disable-next-line
    const offset = this.__slotOffset(key);
    if (!offset) {
      return null;
    }
    let metaInfo = this.getIndexMetaInfo(offset);
    if (compare(key, metaInfo.key) === 0) {
      return {
        ...metaInfo,
        offset,
      };
    }
    while (metaInfo.nextOffset) {
      metaInfo = this.getIndexMetaInfo(metaInfo.nextOffset);
      if (compare(key, metaInfo.key) === 0) {
        return {
          ...metaInfo,
          offset: metaInfo.nextOffset,
        };
      }
    }
    return null;
  }

  getIndexMetaInfo(offset) {
    const dataCopy = Buffer.from(this.data);
    const rootPageNo = dataCopy.readUInt32LE(offset - 4);
    const nextOffset = dataCopy.readUInt16LE(offset - 6);
    const size = dataCopy.readUInt8(offset - 7);
    const keyBuffer = dataCopy.slice(offset - 7 - size, offset - 7);
    return {
      rootPageNo,
      nextOffset,
      size,
      key: keyBuffer.toString(),
    };
  }

  __scale() {
    // eslint-disable-next-line
    this.slotSize <<= 1;
    // 先粗暴的将所有的key先收集起来, 再重新添加
    const keys = this.allKeys();

    this.setBtreeSize(0);
    this.offset = PAGE_SIZE;
    keys.forEach((k) => {
      this.addIndexRootPage(k.key, k.rootPageNo);
    });
  }

  __rewriteNextOffset(offset, newNextOffset) {
    this.data.writeUInt16LE(newNextOffset, offset - PAGE_NO_BYTES - 2);
  }

  __free() {
    return this.offset - this.slotSize * 2 - ID_BTREE_META_HEADER;
  }

  // 根据key获取offset槽的value, 返回的offset 大于0,则代表该槽已经被占,否则为空
  __slotOffset(key) {
    const keyCode = hash(key);
    // eslint-disable-next-line
    const slotIndex = keyCode & (this.slotSize - 1);
    const offset = this.data.readUInt16LE(ID_BTREE_META_HEADER + slotIndex * 2);
    return offset;
  }

  __writeOffsetInSlot(index, offset) {
    this.data.writeUInt16LE(offset, ID_BTREE_META_HEADER + index * 2);
  }

  // 在冲突链中查找最后一个节点, 返回该metaInfo
  __findLastMetaInChain(startOffset) {
    let findedOffset = startOffset;
    let metaInfo = this.getIndexMetaInfo(startOffset);
    while (metaInfo.nextOffset) {
      findedOffset = metaInfo.nextOffset;
      metaInfo = this.getIndexMetaInfo(metaInfo.nextOffset);
    }
    return { offset: findedOffset, ...metaInfo };
  }

  __writeOneIndexMeta(key, rootPageNo, nextOffset) {
    const keySize = ByteSize(key);
    // key的长度 + rootPage 的长度 + nextOffset 长度 + size
    const oneIndexMetaSize = keySize + PAGE_NO_BYTES + 2 + 1;
    const start = this.offset;
    this.data.writeUInt32LE(rootPageNo, start - 4);
    this.data.writeUInt16LE(nextOffset, start - 6);
    this.data.writeUInt8(keySize, start - 7);
    this.data.write(key, start - oneIndexMetaSize);
    this.setBtreeSize(this.btreeSize + 1);
    this.offset -= (7 + keySize);
    this.data.writeUInt16LE(this.offset, ID_BTREE_META_HEADER - 2);
  }

  addIndexRootPage(key, rootPageNo) {
    const keyBytes = ByteSize(key);
    // rootPageNo + nextOffset + size + keyRaw
    const needRoom = 4 + 1 + 1 + keyBytes;
    if (this.__free() > needRoom) {
      if (this.btreeSize >= this.slotSize) {
        // 先扩容
        this.__scale();
        const keyCode = hash(key);
        const index = keyCode & (this.slotSize - 1);
        this.__writeOffsetInSlot(index, this.offset);
        this.__writeOneIndexMeta(key, rootPageNo, 0);
      } else {
        const keyCode = hash(key);
        const index = keyCode & (this.slotSize - 1);
        const offset = this.__slotOffset(key);
        if (offset) { // 产生冲突
          const metaInfo = this.__findLastMetaInChain(offset);
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
    this.data.writeUInt32LE(rootPageNo, offset - 4);
  }

  // 更新btree索引的rootPageNo, btree每当rootPage裂变的时候, 都得更新;
  updateIndexRootPage(rootPageNo, key) {
    const metaInfo = this.__getMetaInfoByKey(key);
    if (!metaInfo) {
      throw new Error(`BtreeMeta cannot update key(${key}) not exists!`);
    }
    this.__changeRootPageNo(metaInfo.offset, rootPageNo);
  }
  //= ======================================================================
}
exports.BtreeMeta = BtreeMeta;

class IdBtree {
  constructor(btreeMeta) {
    this.btreeMeta = btreeMeta;
    // IdBtree为空，整个表为空
    if (!this.btreeMeta || this.btreeMeta.isEmpty()) {
      if (!this.btreeMeta) {
        const pageBuffer = Buffer.alloc(PAGE_SIZE);
        this.btreeMeta = new BtreeMeta(pageBuffer);
      }
      const idPage = new IdPage(PAGE_TYPE_ID | PAGE_TYPE_ROOT | PAGE_TYPE_LEAF,
        0, 1);
      idPage.setPageNo(1);
      this.btreeMeta.setMaxPageNo(1)
        .setIdBtreeMeta({
          rootPageNo: 1,
          workingPageNo: 1,
        });
      this.workingPage = idPage;
      this.rootPage = idPage;
      return Promise.resolve(this);
    }
    const rootPageNo = btreeMeta.idRootPageNo();
    const workingPageNo = btreeMeta.idWorkingOnPageNo();
    return Promise.all([
      IdPage.Load(rootPageNo),
      IdPage.Load(workingPageNo),
    ]).then((pages) => {
      [this.rootPage, this.workingPage] = pages;
      return this;
    });
  }

  static LoadFromScratch() {
    const buffer = Buffer.alloc(PAGE_SIZE);
    const btreeMeta = new BtreeMeta(buffer);

    return new IdBtree(btreeMeta);
  }

  getBtreeMeta() {
    return this.btreeMeta;
  }

  async __diveIntoLeaf(idInfo) {
    let startPage = this.rootPage;
    while (!startPage.isLeaf()) {
      const childPageNo = startPage.getChildPageNo(idInfo);
      if (childPageNo) {
        startPage = await IdPage.Load(childPageNo);
      } else {
        return null;
      }
    }

    return startPage;
  }

  async insertRecursily(page, insertInfo) {
    const { id, childPageNo } = insertInfo;
    const insertResult = page.insertCell(id, childPageNo);
    if (insertResult) {
      return page.getPageNo();
    }
    let maxPageNo = this.btreeMeta.getMaxPageNo();
    let pageType = PAGE_TYPE_ID;
    if (page.isLeaf()) {
      pageType |= PAGE_TYPE_LEAF;
    } else {
      pageType |= PAGE_TYPE_INTERNAL;
    }
    if (page.isRoot()) {
      page.setType(pageType);
      maxPageNo = this.btreeMeta.increaseMaxPageNo();
      const idRootPage = new IdPage(PAGE_TYPE_ID | PAGE_TYPE_ROOT, 0,
        maxPageNo);
      page.setPageParent(maxPageNo, true);
      this.rootPage = idRootPage;
      // 记录idBtree的rootPage
      this.btreeMeta.setRootPageNo(maxPageNo);

      const minIdInfo = page.getMinIdInfo();
      idRootPage.insertCell(minIdInfo.id, page.getPageNo());

      maxPageNo = this.btreeMeta.increaseMaxPageNo();
      const nextPage = new IdPage(pageType, idRootPage.getPageNo(),
        maxPageNo);
      idRootPage.insertCell(id, maxPageNo);
      page.setNextPage(maxPageNo, true);
      nextPage.insertCell(id, childPageNo);
      nextPage.setPrePage(page.getPageNo(), true);

      return nextPage.getPageNo();
    }
    maxPageNo = this.btreeMeta.increaseMaxPageNo();
    const parentPage = await IdPage.Load(page.getPageParent());
    const pageNo = await this.insertRecursily(parentPage, {
      id,
      childPageNo: maxPageNo,
    });
    const nextPage = new IdPage(pageType, pageNo, maxPageNo);
    // 如果是叶节点,则需要记录workingPage
    if (page.isLeaf()) {
      // maxPageNo为nextPage的页码
      this.btreeMeta.setIdBtreeWorkingPage(maxPageNo);
      this.workingPage = nextPage;
    }
    page.setNextPage(maxPageNo, true);
    nextPage.setPrePage(page.getPageNo(), true);
    nextPage.insertCell(id, childPageNo);
    return nextPage.getPageNo();
  }

  async insertId(idInfo, dataPageNo) {
    if (typeof dataPageNo !== 'number') {
      throw new Error('the argument dataPageNo in idBtree.insertId must be number!');
    }
    await this.insertRecursily(this.workingPage, {
      id: idInfo,
      childPageNo: dataPageNo,
    });
  }

  // 根据id查找DataPage的pageNo
  async findPageNo(idInfo) {
    const leafPage = await this.__diveIntoLeaf(idInfo);
    if (!leafPage) {
      return null;
    }
    return leafPage.getChildPageNo(idInfo);
  }
}

exports.IdBtree = IdBtree;

class IndexBtree {
  constructor(btreeMeta, key, fromDisk) {
    this.btreeMeta = btreeMeta;
    this.key = key;
    if (fromDisk) {
      const rootPageNo = btreeMeta.getIndexRootPageNo(key);
      this.rootPageNo = rootPageNo;
      return;
    }
    this.rootPageNo = btreeMeta.increaseMaxPageNo();
    const indexPage = new IndexPage(
      PAGE_TYPE_INDEX | PAGE_TYPE_ROOT | PAGE_TYPE_LEAF, 0, this.rootPageNo,
    );
    btreeMeta.addIndexRootPage(key, this.rootPageNo);
    this.rootPage = indexPage;
  }

  async loadRootPage() {
    const { rootPageNo } = this;
    const page = await IndexPage.LoadPage(rootPageNo);
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

  /**
   * 删除策略：
   * a: 删除的page为left-most,和其next-page匹配平衡
   * b:在删除项的cellIndex=0时，要先更新其parentPage的cellInfo
   * c: 删除的page为between，则和其pre-page匹配平衡
   * @param page
   * @param cellInfo
   * @returns {Promise.<void>}
   * @private
   */
  async __deleteRec(page, cellInfo) {
    if (!page.isRoot()) {
      let adjacentPage;
      const isLeftMost = page.isLeftMost();

      if (isLeftMost) {
        adjacentPage = await page.getNextPage();
      } else {
        adjacentPage = await page.getPrePage();
      }
      // b：
      if (cellInfo.cellIndex === 0) {
        const parentPage = await this.getRootPage();
        // 这里是假定必有大于一个的索引项
        const thisCellInfo = page.getCellInfoByIndex(0);
        const nextCellInfo = page.getCellInfoByIndex(1);
        thisCellInfo.key = nextCellInfo.key;

        parentPage.updateCellInfo(thisCellInfo, 0);
      }
      // 可以合并相邻page
      if (page.isLessHalfWithoutKey(cellInfo.key)) {
        const adjacentPageCells = adjacentPage.allCells();
        page.batchInsertCells(adjacentPageCells);

        // 将空Page标记为可reuse
        let reusePage; let
          appendPage;
        if (isLeftMost) {
          reusePage = adjacentPage;
          appendPage = page;
        } else {
          reusePage = page;
          appendPage = await page.getPrePage();
        }
        const reuseListFirst = this.btreeMeta.getReuseListFirst();
        reusePage.transformToReuse(reuseListFirst);

        this.btreeMeta.addReuseList(reusePage.getPageNo());
        // the cells need to be appended to the appendPage
        const reusePageCells = reusePage.allCells();
        appendPage.batchInsertCells(reusePageCells);

        // 删除nextPage父page的nextPage信息
        const parent = await reusePage.getPageParent();
        const { key, id } = reusePageCells[0];
        const pendingDeleteCell = parent.findCorrectCellInfo(key, id);
        await this.__deleteRec(parent, pendingDeleteCell);
      } else {
        page.deleteCellByIndex(cellInfo.cellIndex);
      }
    }
  }

  async deleteKey(key, id) {
    const deepestPage = await this.walkDeepest(key);
    const cellInfo = deepestPage.findCorrectCellInfo(key, id);
    if (IdCompare(id, cellInfo.id) === 0) {
      await this.__deleteRec(deepestPage, cellInfo);
    }
  }

  async insertKey(key, id) {
    const deepestPage = await this.walkDeepest(key);

    const hasRoom = deepestPage.hasRoomFor(key);
    if (hasRoom) {
      deepestPage.insertCell(key, id, 0);
      return;
    }
    await this.rebalance(deepestPage, { key, id, childPageNo: 0 });
  }

  async rangePage(key, isRightMost) {
    const deepestPage = await this.walkDeepest(key);
    const cellInfo = deepestPage.findNearestCellInfo(key, isRightMost);
    return {
      ...cellInfo,
      page: deepestPage,
    };
  }

  // 根据key查找一个idInfo
  async findId(key) {
    const deepestPage = await this.walkDeepest(key);
    const cellInfo = deepestPage.findNearestCellInfo(key);
    if (cellInfo && compare(cellInfo.key, key) === 0) {
      return cellInfo.id;
    }
    return null;
  }

  // 根据key查找多个idInfo
  async findIds(key) {
    const deepestPage = await this.walkDeepest(key);
    // 带有cellIndex的cellInfo
    const cellInfos = deepestPage.collectAllEqualIds(key);
    // todo 处理跨页面相等key的情况
    return cellInfos.map(cell => cell.id);
  }

  async walkDeepest(key) {
    let startPage = this.rootPage;
    while (!(startPage.getType() & PAGE_TYPE_LEAF)) {
      startPage = await startPage.getNearestChildPage(key);
    }
    return startPage;
  }

  async rebalance(startPage, indexInfo) {
    if (startPage.hasRoomFor(indexInfo.key)) {
      startPage.insertCell(
        indexInfo.key,
        indexInfo.id,
        indexInfo.childPageNo,
      );
      return startPage.getPageNo();
    }
    const splices = startPage.half(indexInfo);
    const middleCellInfo = Object.assign({}, splices[0]);
    let pageType = PAGE_TYPE_INDEX;
    if (startPage.isLeaf()) {
      pageType |= PAGE_TYPE_LEAF;
    } else {
      pageType |= PAGE_TYPE_INTERNAL;
    }

    if (startPage.isRoot()) {
      const splitPageNo = this.btreeMeta.increaseMaxPageNo();
      const rootPageNo = this.btreeMeta.increaseMaxPageNo();
      startPage.setType(pageType);
      startPage.setParentPage(rootPageNo);
      const splitPage = new IndexPage(pageType, rootPageNo,
        splitPageNo);

      // 将同一level的节点链接起来，形成双向链表，用来range查询
      startPage.setNextPageNo(splitPage.getPageNo());
      splitPage.setPrePageNo(startPage.getPageNo());

      const rootNewPage = new IndexPage((PAGE_TYPE_INDEX | PAGE_TYPE_ROOT), null, rootPageNo);

      for (let i = 0; i < splices.length; i += 1) {
        const s = splices[i];
        splitPage.insertCell(s.key, s.id, s.childPageNo);
      }
      if (!startPage.isLeaf()) {
        for (let i = 0; i < splices.length; i += 1) {
          const s = splices[i];
          // 同时修改childPage的parentPage
          const childPage = await IndexPage.LoadPage(s.childPageNo);
          childPage.setParentPage(splitPageNo);
        }
      }
      rootNewPage.insertCell(MIN_KEY, MIN_ID, startPage.getPageNo());
      middleCellInfo.childPageNo = splitPageNo;
      rootNewPage.insertCell(
        middleCellInfo.key,
        middleCellInfo.id,
        middleCellInfo.childPageNo,
      );
      this.rootPage = rootNewPage;
      this.btreeMeta.updateIndexRootPage(rootNewPage.getPageNo(), this.key);
      if (compare(indexInfo.key, middleCellInfo.key) >= 0) {
        return splitPageNo;
      }
      return startPage.getPageNo();
    }
    const parentPage = await startPage.getPageParent();
    const splitPageNo = this.btreeMeta.increaseMaxPageNo();
    const splitPage = new IndexPage(pageType, null, splitPageNo);
    middleCellInfo.childPageNo = splitPageNo;
    const splitParentNo = await this.rebalance(parentPage, middleCellInfo);
    splitPage.setParentPage(splitParentNo);

    // 将splitPage和startPage组成双向链表
    const nextPageNo = startPage.getNextPageNo();
    if (nextPageNo) {
      const nextPage = await startPage.getNextPage();
      splitPage.setNextPageNo(nextPageNo);
      nextPage.setPrePageNo(splitPage.getPageNo());
    }
    splitPage.setPrePageNo(startPage.getPageNo());
    startPage.setNextPageNo(splitPage.getPageNo());

    for (let i = 0; i < splices.length; i += 1) {
      const s = splices[i];
      splitPage.insertCell(s.key, s.id, s.childPageNo);
    }
    if (!startPage.isLeaf()) {
      for (let i = 0; i < splices.length; i += 1) {
        const s = splices[i];
        // 同时修改childPage的parentPage
        const childPage = await IndexPage.LoadPage(s.childPageNo);
        childPage.setParentPage(splitPageNo);
      }
    }
    if (compare(indexInfo.key, middleCellInfo.key) >= 0) {
      return splitPageNo;
    }
    return startPage.getPageNo();
  }
}

exports.IndexBtree = IndexBtree;
