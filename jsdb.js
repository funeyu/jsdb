const {DataPage} = require('./page.js');
const {IdGen, jsonStringify, jsonParse} = require('./utils');
const {IdBtree} = require('./btree.js');

let currentDataPageNo = 0;
let dataPage = new DataPage(currentDataPageNo);

class jsDB {
    constructor(directory, ... keys) {
        this.directory = directory;
        this.keys = keys;
        // 先假定从零开始写
        return IdBtree.LoadFromScratch().then(idBtree=> {
            this.idBtree = idBtree;
            return this;
        });
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
        let result = dataPage.insertCell(id, jsonString);
        console.log('result', result)
        // 依次写入btree索引页
        // 1. 先写id索引树
        let idResult = await this.idBtree.insertId(id, jsonString);
        // todo 2.再写用户定义的索引树;

        return id;
    }

    async get(id) {
        // 先获取dataPageNo
        let pageNo = await this.idBtree.findPageNo(id);
        // load PageNo
        return dataPage.getCellData(id);
    }
}



async function test() {
    let db = new jsDB('js');
    db = await db;
    let id = await db.put({name: 'name'});
    console.log('id', id);
    let result = await db.get(id);
    console.log('result', result)
}
test();