const fs = require('fs');
const readline = require('readline');
const { MongoClient } = require('mongodb');
const { Buffer } = require('buffer');
const { mainModule } = require('process');
const url = 'mongodb://127.0.0.1:27017';
const client = new MongoClient(url);
const dbName = 'sort';
const db = client.db(dbName);
const collection = db.collection('strings');
let count = 0;
let rl = readline.createInterface({
    input: fs.createReadStream('./file4.txt', {
        highWaterMark: 8
    }),
    crlfDelay: Infinity
})

let temp = '';
console.time('upload')
console.time('all')
const getData = async () => {
    let loop = 1;
    await client.connect();
    // сортировка по нужному ключу
    let gotData = await collection.find().limit(200).sort({ randNum: 1 }).toArray();
    loop = gotData.length
    if (loop === 0) {        
        console.timeEnd('all')
        console.log('I did it')
        db.dropDatabase();
        process.exit()
    };
    const forRemove = gotData.map(a => {
        return a._id
    });
    //Можно мапнуть массив, чтобы убрать id записи
    let temp2 = gotData.map(ev => {
        let y = {
            'number': ev.number,
            'randNum': ev.randNum
        }
        return y;
    })
    temp2.forEach(a => {
        fs.writeFileSync('./result.txt',
            `${JSON.stringify(a)}\n`,
            { flag: 'a' },
            (err) => {
                console.error(err)
            })
    })
    const deleted = await collection.deleteMany({ _id: { $in: forRemove } })
    // console.log(gotData.length)
    console.log(deleted)

}
async function main() {
    let str = [];

    console.log('Connected to MDB');
    rl
        .on('line', (data) => {
            count++
            if (count % 200 === 0) {
                const addBase = async (count, str) => {
                    await client.connect();
                    const insertString = await collection.insertMany(str);
                    if (insertString) console.log(`Добавлено в БД ${count}`)
                };
                // можно поставить rl.pause() если БД не успевает глотать запросы и нода складывает в оперативку
                addBase(count, str);
                str = [];
            }
            temp = JSON.parse(data)
            str.push(temp)
        })
        .on('close', () => {
            setInterval(() => {
                getData()
                    .then('Добавлено')
            }, 1000)
            console.timeEnd('upload')
            console.time('download')
        })




}

main()
    .then(console.log)
    .catch(console.error)
    .finally(() => client.close())



