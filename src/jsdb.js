const rimraf = require('rimraf');
const {
  DataPage, IdPage, IndexPage,
} = require('./page.js');
const { IdGen, jsonStringify, jsonParse } = require('./utils');
const { IdBtree, IndexBtree, BtreeMeta } = require('./btree.js');


class JSDB {
  constructor(directory, btreeMeta, ...keys) {
    this.directory = directory;
    this.keys = keys;
    // 获取dataPage的最大值
    this.maxDataPage = DataPage.MaxPageNo(directory);
    if (!btreeMeta) {
      this.currentDataPage = new DataPage(0);
      DataPage.InitFile(directory);
      IdPage.InitFile(directory);

      // 先假定从零开始写
      return IdBtree.LoadFromScratch().then((idBtree) => {
        this.idBtree = idBtree;
        this.keysMap = {};
        const btreeMetaLoaded = idBtree.getBtreeMeta();
        this.btreeMeta = btreeMetaLoaded;
        for (const key of keys) { // eslint-disable-line
          this.keysMap[key] = new IndexBtree(btreeMetaLoaded, key);
        }
        return this;
      });
    }
    // 代表从disk里读取
    // dataPage 中有数据,读取最后一页的数据;
    return DataPage.Load(directory, this.maxDataPage).then((data) => {
      this.currentDataPage = data;
      this.btreeMeta = btreeMeta;
      this.keysMap = {};
      for (const key of keys) { // eslint-disable-line
        // 这里的indexBtree没有真正从磁盘读取；
        // 在Connect 函数里要从新load from disk
        this.keysMap[key] = new IndexBtree(btreeMeta, key, true);
      }
      return this;
    });
  }

  static async Create(directory, ...keys) {
    const db = await new JSDB(directory, null, ...keys);
    return db;
  }

  __setCurrentPage(pageNo) {
    this.currentDataPage = new DataPage(pageNo);
  }

  // 这里只有在实例一个jsDB才会调用，并且是在从disk里读取
  setIdBtree(idBtree) {
    this.idBtree = idBtree;
    return this;
  }

  async put(jsonData) {
    const id = IdGen();
    // 检查jsonData的索引项不能为空
    for (const key of this.keys) { // eslint-disable-line
      if (!jsonData[key]) {
        return new Error(`索引项：${key} 不能为空！`);
      }
    }

    const jsonString = jsonStringify(jsonData);
    // 插入数据页
    const result = this.currentDataPage.insertCell(id, jsonString);
    let pendingRecordPageNo = this.currentDataPage.getPageNo();
    if (!result) {
      this.__setCurrentPage(pendingRecordPageNo + 1);
      this.currentDataPage.insertCell(id, jsonString);
      pendingRecordPageNo += 1;
    }
    // 依次写入btree索引页
    // 1. 先写id索引树
    await this.idBtree.insertId(id, pendingRecordPageNo);
    for (const key of this.keys) { // eslint-disable-line
      const indexBtree = this.keysMap[key];
      // eslint-disable-next-line
      await indexBtree.insertKey(jsonData[key], id);
    }
    return id;
  }

  async findById(id) {
    // 先获取dataPageNo
    const pageNo = await this.idBtree.findPageNo(id);
    // load PageNo
    const dataPage = await DataPage.Load(this.directory, pageNo);
    return dataPage.getCellData(id);
  }

  async findByKey(key, value) {
    // todo 校验没有该key的索引
    const indexBtree = this.keysMap[key];
    try {
      const id = await indexBtree.findId(value);
      const result = await this.findById(id);
      return result;
    } catch (err) {
      return null;
    }
  }

  async findAllByKey(key, value) {
    const indexBtree = this.keysMap[key];

    const result = [];
    const ids = await indexBtree.findIds(value);
    for (let i = 0; i < ids.length; i += 1) {
      // eslint-disable-next-line
      const rawData = await this.findById(ids[i]);
      result.push(rawData);
    }

    return result;
  }

  /**
     * 返回一个范围内的数据
     * @param keyName
     * @param option
     * @returns {Promise.<void>}
     */
  // todo 校验option，处理其他情况
  async range(keyName, option) {
    const indexBtree = this.keysMap[keyName];
    const { lt, gt } = option;

    const ltRangeInfo = await indexBtree.rangePage(lt);
    const gtRangeInfo = await indexBtree.rangePage(gt, true);

    let cells = [];
    let startPage = ltRangeInfo.page;
    const endPage = gtRangeInfo.page;
    if (startPage.getPageNo() === endPage.getPageNo()) {
      for (let i = ltRangeInfo.cellIndex; i <= gtRangeInfo.cellIndex; i += 1) {
        cells.push(startPage.getCellInfoByIndex(i));
      }
      return Promise.resolve({
        cells,
        total: () => cells.length,
        fetch: async () => {
          const results = [];
          for (let i = 0; i < cells.length; i += 1) {
            const result = await this.findById(cells[i].id); // eslint-disable-line
            results.push(result);
          }

          return results;
        },
      });
    }

    for (let i = ltRangeInfo.cellIndex; i < startPage.getSize(); i += 1) {
      cells.push(startPage.getCellInfoByIndex(i));
    }
    startPage = await startPage.getNextPage();
    while (startPage.getPageNo() !== endPage.getPageNo()) {
      cells = cells.concat(startPage.allCells());
      startPage = await startPage.getNextPage(); // eslint-disable-line
    }

    for (let i = 0; i < gtRangeInfo.cellIndex; i += 1) {
      cells.push(endPage.getCellInfoByIndex(i));
    }

    return Promise.resolve({
      cells,
      total: () => cells.length,
      fetch: async () => {
        const results = [];
        for (let i = 0; i < cells.length; i += 1) {
          const result = await this.findById(cells[i].id); // eslint-disable-line
          results.push(result);
        }

        return results;
      },
    });
  }

  async flush() {
    // 先写page0 也即是btreeMeta的data
    await IndexPage.FlushPageToDisk(this.directory, this.btreeMeta.data, 0);

    let keyPages = IndexPage.CachePage().values();
    keyPages = keyPages.sort(
      (next, pre) => next.getPageNo() - pre.getPageNo(),
    );
    for (const page of keyPages) { // eslint-disable-line
      await page.flush(this.directory); // eslint-disable-line
    }

    let dataPages = DataPage.CachePage().values();
    dataPages = dataPages.sort(
      (next, pre) => next.getPageNo() - pre.getPageNo(),
    );
    for (const page of dataPages) { // eslint-disable-line
      await page.flush(this.directory); //eslint-disable-line
    }
  }

  static async Connect(directory) {
    const btreeMeta = await BtreeMeta.LoadFromDisk(directory);
    // todo 这里可以将rootPage传给后面
    const keys = btreeMeta.allKeys().map(k => k.key);
    const db = await new JSDB(directory, btreeMeta, ...keys);
    // 先实例idBtree
    const idBtree = await new IdBtree(btreeMeta);
    db.setIdBtree(idBtree);
    // 从磁盘里load rootPage数据到索引树
    for (const key of keys) { // eslint-disable-line
      db.keysMap[key] = await db.keysMap[key].loadRootPage(); // eslint-disable-line
    }

    return db;
  }

  static Parse(data) {
    return jsonParse(data);
  }

  static Clean(directory, cb) {
    rimraf(directory, cb);
  }
}
module.exports = JSDB;
