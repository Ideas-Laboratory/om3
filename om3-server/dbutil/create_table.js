const fs = require('fs')
const { Pool } = require('pg');

const dbConfig = JSON.parse(fs.readFileSync("../initdb/dbconfig.json").toString());
console.log(dbConfig)
if (!dbConfig['username'] || !dbConfig['hostname'] || !dbConfig['password'] || !dbConfig['db']) {
    throw new Error("db config error");
}
const pool = new Pool({
    user: dbConfig['username'],
    host: dbConfig["hostname"],
    database: dbConfig['db'],
    password: dbConfig['password'],
});

async function createTable() {
    const tableQuerySql = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema') and table_schema||'.'||table_name like '%sktr_4m%'`;
    try {
        const result = await pool.query(tableQuerySql)
        for (let i = 0; i < result.rows.length; i++) {
            const v = result.rows[i]
            const curTableName = "stock_" + v.table_fullname.split(".")[1].split("_")[1] + "_4m"
            const createTableSQl = `create table raw_data_multi.${curTableName}(t integer,v double precision)`;
            await pool.query(createTableSQl);
            const waveltName = "stock_" + v.table_fullname.split(".")[1].split("_")[1] + "_om3" + "_4m"
            const createWaveletSql = `create table om3_multi.${waveltName}(i integer primary key,minvd double precision,maxvd double precision )`;
            await pool.query(createWaveletSql);
        }
    } catch (err) {
        pool.end()
        throw err
    }
}
async function cleanOM3Table() {

}
//createTable()

async function syncData() {
    const tableQuerySql = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema') and table_schema||'.'||table_name like '%sktr_4m%'`;
    try {
        const result = await pool.query(tableQuerySql)
        for (let i = 0; i < result.rows.length; i++) {
            const v = result.rows[i]
            if (v.table_fullname.includes("m4rawdata")) {
                const curTableName = "raw_data_multi.stock_" + v.table_fullname.split(".")[1].split("_")[1] + "_4m"
                const curSQL = `insert into ${curTableName} select t,v from ${v.table_fullname}`
                console.log(curSQL)
                await pool.query(curSQL);
            }
        }
    } catch (err) {

        pool.end()
        throw err
    }
}
//syncData()



async function computeTableFlag(data, targetTableName) {
    const maxT = data.rows[data.rows.length - 1]['t'];

    const bufLen = 2 ** Math.ceil(Math.log2(maxT));
    const tempArray = new Array(bufLen);
    data.rows.forEach(item => {
        tempArray[item['t']] = item['v'];
    });
    const arrayBuffer = new Buffer.alloc(bufLen);

    for (let j = 0; j < tempArray.length; j += 2) {
        if (tempArray[j] === undefined && tempArray[j + 1] === undefined) {
            continue;
        } else if (tempArray[j] === undefined) {
            arrayBuffer[j] = 1;
            arrayBuffer[j + 1] = 0;
            continue;
        } else if (tempArray[j + 1] === undefined) {
            arrayBuffer[j] = 1;
            arrayBuffer[j + 1] = 1;
            continue
        }
        if (tempArray[j] < tempArray[j + 1]) {
            arrayBuffer[j] = 0;
            arrayBuffer[j + 1] = 0;
        } else {
            arrayBuffer[j] = 0;
            arrayBuffer[j + 1] = 1;
        }
    }
    fs.writeFileSync(`../flags/${targetTableName.split(".")[1]}.flagz`, arrayBuffer);
    console.log("compute ordering flag finished")
    //pool.end()
}

async function nonuniformMinMaxEncode(fromTableName, targetTableName) {
    const querySQL = `SELECT t,v FROM ${fromTableName} ORDER by t ASC`
    const queryData = await pool.query(querySQL);
    computeTableFlag(queryData, targetTableName);
    // return
    let data = queryData.rows;

    let min = data[0]['v'];
    let max = data[0]['v'];
    let maxTime = data[0]['t'];
    for (let i = 0; i < data.length; i++) {
        if (data[i]['v'] < min) {
            min = data[i]['v'];
        }
        if (data[i]['v'] > max) {
            max = data[i]['v'];
        }
        if (data[i]['t'] > maxTime) {
            maxTime = data[i]['t'];
        }
    }
    const realLen = 2 ** Math.ceil(Math.log2(maxTime));
    const maxL = Math.ceil(Math.log2(maxTime));
    const dataArray = new Array(realLen)
    data.forEach((v, i) => {
        dataArray[v['t']] = v['v'];
    });
    let curL = 1;
    let minV = dataArray
    let maxV = dataArray
    for (let l = curL; l <= maxL; l++) {

        console.log(fromTableName, "compute level:", l)

        let curMinVDiff = new Array(2 ** (maxL - l));
        let curMaxVDiff = new Array(2 ** (maxL - l));

        let curMinV = new Array(2 ** (maxL - l));
        let curMaxV = new Array(2 ** (maxL - l));


        for (let i = 0; i < 2 ** (maxL - l + 1); i += 2) {

            //Min
            if (minV[i] === undefined && minV[i + 1] !== undefined) {
                curV = minV[i + 1]
                curDif = undefined;
            } else if (minV[i] !== undefined && minV[i + 1] === undefined) {
                curV = minV[i];
                curDif = 0;
            } else if (minV[i] === undefined && minV[i + 1] === undefined) {
                curV = undefined;
                curDif = undefined;
            } else {
                curV = Math.min(minV[i], minV[i + 1]);
                curDif = minV[i] - minV[i + 1];
            }
            curMinV[i / 2] = curV;
            curMinVDiff[i / 2] = curDif;

            //Max
            if (maxV[i] === undefined && maxV[i + 1] !== undefined) {
                curV = maxV[i + 1];
                curDif = 0;
            } else if (maxV[i] !== undefined && maxV[i + 1] === undefined) {
                curV = maxV[i];
                curDif = undefined;
            } else if (maxV[i] === undefined && maxV[i + 1] === undefined) {
                curV = undefined;
                curDif = undefined;
            } else {
                curV = Math.max(maxV[i], maxV[i + 1]);
                curDif = maxV[i] - maxV[i + 1];
            }
            curMaxV[i / 2] = curV;
            curMaxVDiff[i / 2] = curDif;
        }
        minV = curMinV;
        maxV = curMaxV;

        if (l === 1) {
            continue
            // console.log(curMinT, curMinV, curMaxV, curMaxT);
        }

        let sqlStr = `insert into ${targetTableName}(i,minvd,maxvd) values `
        let i = 0;
        while (i < curMaxVDiff.length) {
            const usedL = maxL - l
            let tempStr = ''
            if (i + 10000 < curMaxVDiff.length) {
                for (let j = i; j < i + 10000; j++) {
                    if (curMinVDiff[j] === undefined && curMaxVDiff[j] === undefined) {
                        continue;
                    }

                    if (tempStr === '') {
                        tempStr += ` (${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]})`;
                    } else {
                        tempStr += `,(${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]})`;
                    }
                }

            } else {
                for (let j = i; j < curMaxVDiff.length; j++) {
                    if (curMinVDiff[j] === undefined && curMaxVDiff[j] === undefined) {
                        continue;
                    }

                    if (tempStr === '') {
                        tempStr += ` (${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]})`;
                    } else {
                        tempStr += `,(${(2 ** usedL) + j},${curMinVDiff[j] === undefined ? "NULL" : curMinVDiff[j]},${curMaxVDiff[j] === undefined ? "NULL" : curMaxVDiff[j]})`;
                    }
                }
            }
            i += 10000
            if (tempStr === '') {
                continue
            }
            let sql = sqlStr + tempStr;
            try {
                await pool.query(sql)
            } catch (err) {
                console.log(sql)
                pool.end();
                throw err
            }
        }
    }
    if (min !== undefined && max !== undefined) {
        const l0Sql = `insert into ${targetTableName}(i,minvd,maxvd) values(${-1},${min},${max})`
        await pool.query(l0Sql);
        //pool.end()
    }

}

async function encodeData() {
    const tableQuerySql = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema') and table_schema||'.'||table_name like '%sktr_4m%'`;
    try {
        const result = await pool.query(tableQuerySql)
        for (let i = 0; i < result.rows.length; i++) {
            const v = result.rows[i]
            if (v.table_fullname.includes("m4rawdata")) {
                const curTableName = "raw_data_multi.stock_" + v.table_fullname.split(".")[1].split("_")[1] + "_4m"

                const waveltName = "om3_multi.stock_" + v.table_fullname.split(".")[1].split("_")[1] + "_om3" + "_4m"
                await nonuniformMinMaxEncode(curTableName, waveltName)
            }
        }
    } catch (err) {
        pool.end()
        throw err
    }
    pool.end()
}
encodeData()

async function cleanWaveletData() {
    const tableQuerySql = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema') and table_schema||'.'||table_name like '%sktr_4m%'`;
    try {
        const result = await pool.query(tableQuerySql)
        for (let i = 0; i < result.rows.length; i++) {
            const v = result.rows[i]
            if (v.table_fullname.includes("m4rawdata")) {
                const waveltName = "om3_multi.stock_" + v.table_fullname.split(".")[1].split("_")[1] + "_om3" + "_4m"
                const curSQL = `delete from ${waveltName}`
                console.log(curSQL)
                await pool.query(curSQL);
            }
        }
    } catch (err) {

        pool.end()
        throw err
    }
}
//cleanWaveletData()