const path = require('path');

const {DataPage, IdPage, IndexPage,
    PAGE_TYPE_ID, PAGE_TYPE_INTERNAL, PAGE_TYPE_INDEX,
    PAGE_TYPE_ROOT, PAGE_TYPE_LEAF,
    PAGE_SIZE, INDEXPATH, FILEPATH} = require('./page.js');
const {IdGen, jsonStringify, jsonParse} = require('./utils');
const {IdBtree, IndexBtree, BtreeMeta} = require('./btree.js');


class jsDB {
    constructor(directory, btreeMeta, ... keys) {
        this.directory = directory;
        this.keys = keys;
        // 获取dataPage的最大值
        this.maxDataPage = DataPage.MaxPageNo(directory);
        if(!btreeMeta) {
            this.currentDataPage = new DataPage(0);
            DataPage.InitFile(directory);
            IdPage.InitFile(directory);

            // 先假定从零开始写
            return IdBtree.LoadFromScratch().then(idBtree=> {
                this.idBtree = idBtree;
                this.keysMap = {};
                let btreeMeta = idBtree.getBtreeMeta();
                this.btreeMeta = btreeMeta;
                for (let key of keys) {
                    this.keysMap[key] = new IndexBtree(btreeMeta, key);
                }
                return this;
            });
        } else {// 代表从disk里读取
            // dataPage 中有数据,读取最后一页的数据;
            this.currentDataPage = DataPage.Load(directory,
                    this.maxDataPage );
            this.btreeMeta = btreeMeta;
            this.keysMap = {};
            for(let key of keys) {
                console.log('keys', keys);
                console.log('key', key);
                // 这里的indexBtree没有真正从磁盘读取；
                // 在Connect 函数里要从新load from disk
                this.keysMap[key] = new IndexBtree(btreeMeta, key, true);
            }
        }
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
        let id = IdGen();
        //检查jsonData的索引项不能为空
        for(let key of this.keys) {
            if(!jsonData[key]) {
                return new Error(`索引项：${key} 不能为空！`);
            }
        }

        let jsonString = jsonStringify(jsonData);
        // 插入数据页
        let result = this.currentDataPage.insertCell(id, jsonString);
        let pendingRecordPageNo = this.currentDataPage.getPageNo();
        if(!result) {
            this.__setCurrentPage(pendingRecordPageNo + 1);
            this.currentDataPage.insertCell(id, jsonString);
            pendingRecordPageNo = pendingRecordPageNo + 1;
        }
        // 依次写入btree索引页
        // 1. 先写id索引树
        let idResult = await this.idBtree.insertId(id, pendingRecordPageNo);
        for(let key of this.keys) {
            let indexBtree = this.keysMap[key];
            await indexBtree.insertKey(jsonData[key], id);
        }
        return id;
    }

    async findById(id) {
        // 先获取dataPageNo
        let pageNo = await this.idBtree.findPageNo(id);
        // load PageNo
        let dataPage = await DataPage.Load(this.directory, pageNo);
        return dataPage.getCellData(id);
    }

    async findByKey(key, value) {
        //todo 校验没有该key的索引
        let indexBtree = this.keysMap[key];
        try {
            let id = await indexBtree.findId(value);
            console.log('id',id);
            let result = await this.findById(id);
            return result;
        } catch(err) {
            console.log('err', err)
        }
    }

    async findAllByKey(key, value) {
        let indexBtree = this.keysMap[key];

        let result = [];
        let ids = await indexBtree.findIds(value);
        for(let i = 0; i < ids.length; i ++) {
            let rawData = await this.findById(ids[i]);
            result.push(rawData);
        }

        return result;
    }

    async flush() {
        // 先写page0 也即是btreeMeta的data
        await IndexPage.FlushPageToDisk(this.directory, this.btreeMeta.data, 0);

        let keyPages = IndexPage.CachePage().values();
        keyPages = keyPages.sort(
            (next, pre)=> next.getPageNo() - pre.getPageNo());
        for(let page of keyPages) {
            await page.flush(this.directory);
        }

        let dataPages = DataPage.CachePage().values();
        dataPages = dataPages.sort(
            (next, pre)=> next.getPageNo() - pre.getPageNo());
        for(let page of dataPages) {
            await page.flush(this.directory);
        }
    }

    static async Connect(directory) {
        let btreeMeta = await BtreeMeta.LoadFromDisk(directory);
        // todo 这里可以将rootPage传给后面
        let keys = btreeMeta.allKeys().map(k=> k.key);
        let db = new jsDB(directory, btreeMeta, ... keys);
        // 先实例idBtree
        let idBtree = await new IdBtree(btreeMeta);
        db.setIdBtree(idBtree);
        // 从磁盘里load rootPage数据到索引树
        for(let key of keys) {
            db.keysMap[key] = await db.keysMap[key].loadRootPage();
        }

        return db;
    }
}



async function test() {
    let db = await new jsDB('js', null, 'name');
    for(let i = 0; i < 50000; i ++) {
        let id = await db.put({name: 'funer80900090009' + i, className: 'super' + i});
        // await db.put({name: 'nameSex' + i, className: 'superrman' + i});
    }

    await db.flush();
    // let result = await db.findByKey('name', 'name9');
    // console.log('result', result)
}

async function connect() {
    let db = await jsDB.Connect('js');
    // await db.put({name: 'namessss2', className: 'superman'});
    for(let i =10000; i < 20000; i ++ ) {
        let result = await db.findByKey('name', 'funer80900090009' + i);
        console.log('conecctttttttt', result);
        if(!result) {
            throw new Error('error!')
        }
    }
}
// test();
connect();