const {DataPage} = require('./page.js');
const {IdGen} = require('./utils');

let currentDataPageNo = 0;
let dataPage = new DataPage(currentDataPageNo);

class jsDB {
    constructor(directory) {

    }
    put(rawData) {
        let id = IdGen();
        let result = dataPage.insertCell(id, rawData);
        if(result) {
            return id;
        }
        return false;
    }

    get(id) {
        return dataPage.getCell(id);
    }
}

let db = new jsDB();
let id = db.put("javaaa");
console.log("Jsdb data:", db.get(id));