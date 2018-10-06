const JSDB = require('../src/jsdb');

function cleanDB(dir, cb) {
  JSDB.Clean(dir, cb);
}

// eslint-disable-next-line
beforeAll(() => new Promise((resolve, reject) => {
  cleanDB('../js', (error) => {
    if (error) {
      reject(error);
    } else {
      resolve(null);
    }
  });
}));

async function create(itemNum) {
  const db = await JSDB.Create('js', 'name');
  for (let i = 0; i < itemNum; i += 1) {
    await db.put({
      name: `funer80900090009${i}`,
      sex: `${{ 0: 'female', 1: 'male', 2: 'shemale' }[i % 3]}`,
      className: `super${i}`,
    }); // eslint-disable-line
  }
  await db.flush();
}

// eslint-disable-next-line
test('create a db and put 100 data', done => {
  create(100).then(done);
});

// eslint-disable-next-line
test('findByKey method', done => {
  create(100).then(async () => {
    const db = await JSDB.Connect('js');
    for (let i = 0; i < 100; i += 1) {
      const result = await db.findByKey('name', `funer80900090009${i}`);
      if (!result) {
        throw new Error('error!');
      }
    }
    done();
  });
});
