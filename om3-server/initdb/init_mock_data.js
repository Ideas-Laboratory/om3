const gaussian = require("gaussian");
const fs=require("fs")
const { Pool } = require('pg');

const dbConfig=JSON.parse(fs.readFileSync("./dbconfig.json").toString());
console.log(dbConfig)
if(!dbConfig['username']||!dbConfig['hostname']||!dbConfig['password']||!dbConfig['db']){
    throw new Error("db config error");
}
const pool = new Pool({
    user: dbConfig['username'],
    host: dbConfig["hostname"],
    database: dbConfig['db'],
    password: dbConfig['password'],
});

function x_plus_sinx() {
    const distribution = gaussian(0, 1);
    return (x) => {
        return x / 2000 + 500 * Math.sin(x / 240000) + 50 * distribution.ppf(Math.random());
    }
}
async function generateAndInsertMockData() {
    const maxPow = 23;
    await pool.query("create table raw_data.mock_guassian_sin_8m(t integer,v double precision)")
    const tableNames = ["raw_data.mock_guassian_sin_8m"];
    let fun = null;
    for (let i = 0; i < tableNames.length; i++) {
        let data = [];

        fun = x_plus_sinx()

        for (let i = 0; i < 2 ** maxPow; i++) {
            data.push({ t: i, v: fun(i) });
        }
        let sum = 0;
        while (sum + 200000 < 2 ** maxPow) {
            await insertToDB(data.slice(sum, sum + 200000), tableNames[i]);
            sum += 200000;
        }
        await insertToDB(data.slice(sum, 2 ** maxPow), tableNames[i]);
    }
    pool.end();
}

async function insertToDB(data, tableName) {
    let sql = `insert into ${tableName}(t,v) values `
    sql = data.reduce((pre, cur, i) => {
        if (i === 0) {
            return pre + `(${cur.t},${cur.v})`;
        } else {
            return pre + `,(${cur.t},${cur.v})`;
        }
    }, sql);
    try {
        await pool.query(sql);
    } catch (error) {
        pool.end();
        throw error
    }
    return
}

generateAndInsertMockData()
