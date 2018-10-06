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
test('ranges method', done => {
  create(100).then(async () => {
    const db = await JSDB.Connect('js');
    for (let i = 0; i < 100; i += 1) {
      const result = await db.findByKey('name', `funer80900090009${i}`);
      if (!result) {
        throw new Error('error!');
      }
    }

    db.range('name', { lt: 'funer809000900091', gt: 'funer809000900091' }).then((data) => {
      // eslint-disable-next-line
      expect(data.total()).toBe(1);
      data.fetch().then((details) => {
        const detail = JSDB.Parse(details[0]);
        // eslint-disable-next-line
        expect(detail.name === 'funer809000900091');

        done();
      });
    });
  });
});
