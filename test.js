const {IdBtree} = require('./btree.js');

let idBtree = new IdBtree();
let id;
let idArray = [];
let test = async (btree)=> {
    for(let i = 0; i < 101; i ++) {
        id = IdGen();
        idArray.push(id);
        if(i === 100) {
            await btree.insertId(id, i);
        } else {
            await btree.insertId(id, i);
        }
    }
    console.log('finished')
}
idBtree.then(btree=> {
    // test(btree).then(()=> {
    // 	console.log('19', idArray[1])
    //    btree.findPageNo(idArray[47]).then(data=> {
    //        console.log('data', data);
    //    })
    // });

    btree.btreeMeta.addIndexRootPage('java', 12345);
    btree.btreeMeta.addIndexRootPage('javanodejs',234);
    btree.btreeMeta.addIndexRootPage('odejs', 78);
    btree.btreeMeta.addIndexRootPage('hello', 89);
    btree.btreeMeta.addIndexRootPage('javahello', 7);
    btree.btreeMeta.addIndexRootPage('jeeee', 78);
    btree.btreeMeta.addIndexRootPage('jjajf;a', 7897);
    btree.btreeMeta.addIndexRootPage('ja;ajff',453);
    btree.btreeMeta.addIndexRootPage('nodjes', 88773);
    btree.btreeMeta.addIndexRootPage(';asfj;af', 988);
    btree.btreeMeta.addIndexRootPage('quiet', 7897);
    console.log('java', btree.btreeMeta.getIndexRootPageNo('nodjes'))
});



// let page = new DataPage(1);
//
// let id1 = IdGen();
// let id2 = IdGen();
// let id3 = IdGen();
//
// page.insertCell(id1, 'stringDataPage1');
// page.insertCell(id2, 'nodejsDataPage1');
// page.insertCell(id3, 'javaDataPage1123');
// console.log(id1)
// console.log(id2)
// console.log(id3)
// page.flush().then(data=> {
//     DataPage.load(1, (err, dataPage)=> {
//     	console.log('errror', err)
// 	    console.log('id3', id3)
//         let cellInfo = dataPage.getCell(id3);
//     	console.log('cellInfo', cellInfo)
//     });
// })