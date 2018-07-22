const {DataPage, IdPage} = require('./page.js');

/*
**	return 1 when key1 > key2; return 0 key1 = key2; otherwise false
*/
const compare= function(key1, key2) {
	if(typeof key1 !== 'string' || typeof key2 !== 'string') {
		throw new Error('can not compare key not string!')
	}

	let cur = 0;
	for(;;) {
		if(key1[cur] && !key2[cur]) {
			return 1;
		}
		if(!key1[cur] && key2[cur]) {
			return -1
		}
		if(key1[cur] > key2[cur]) {
			return 1;
		}
		if(key2[cur] > key1[cur]) {
			return -1;
		}
		if(cur === key2.length && cur == key1.length) {
			return 0
		}
		cur ++;
	}
}

console.log(compare('key21', 'key2'));


const insertData = function(data) {
	
}


const reblance = function() {

}
