const path = require('path');
const fs = require('fs');
let b = fs.existsSync(path.join('js', 'js.index'));
console.log('b', b)
