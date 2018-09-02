const {DataPage, IdPage, IndexPage,
    PAGE_TYPE_ID, PAGE_TYPE_INTERNAL, PAGE_TYPE_INDEX,
    PAGE_TYPE_ROOT, PAGE_TYPE_LEAF} = require('./page.js');
const {IdGen, jsonStringify, jsonParse} = require('./utils');
const {IdBtree, IndexBtree} = require('./btree.js');


class jsDB {
    constructor(directory, ... keys) {
        this.directory = directory;
        this.keys = keys;
        // 获取dataPage的最大值
        this.maxDataPage = DataPage.MaxPageNo(directory);
        if(this.maxDataPage === 0) {
            this.currentDataPage = new DataPage(0);
            DataPage.InitFile(directory);
            IdPage.InitFile(directory);
        } else {
            // dataPage 中有数据,读取最后一页的数据;
            this.currentDataPage = DataPage.load(
                    this.maxDataPage - 1);
        }
        // 先假定从零开始写
        return IdBtree.LoadFromScratch().then(idBtree=> {
            this.idBtree = idBtree;
            this.keysMap = {};
            let btreeMeta = idBtree.getBtreeMeta();
            for (let key of keys) {
                this.keysMap[key] = new IndexBtree(btreeMeta, key);
            }
            return this;
        });
    }

    __setCurrentPage(pageNo) {
        this.currentDataPage = new DataPage(pageNo);
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
        // todo 2.再写用户定义的索引树;
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
        let dataPage = await DataPage.load(this.directory, pageNo);
        return dataPage.getCellData(id);
    }

    // findByKey({name: 'funer'})
    async findByKey(key, value) {
        //todo 校验没有该key的索引
        let indexBtree = this.keysMap[key];
        let id = await indexBtree.findId(value);
        let result = await this.findById(id);
        return result;
    }
}



async function test() {
    let db = await new jsDB('js', 'name');
    for(var i = 0; i < 10; i ++) {
        let id = await db.put({name: 'name' + i});
        console.log('id', id);
    }
    let result = await db.findByKey('name', 'name1');
    console.log('result', result)
}
test();