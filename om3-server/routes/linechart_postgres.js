const express = require('express');
const router = express.Router();
const fs = require("fs")

const { Pool } = require('pg');
const { generateSingalLevelSQLWithSubQueryMinMaxMiss, generateSingalLevelSQLMinMaxMissQuery, generateSingalLevelSQLWithSubQuery } = require('../helper/generate_sql');
const { getTableLevel, generateOM3TableName, customDBPoolMap, computeLevelFromT, getPool } = require('../helper/util');
const { randomUUID } = require('crypto');
const { nonuniformMinMaxEncode } = require('../compute/om_compute');

const stockTableMap = [];
const mockTableMap = [];
const drinkTableMap = [];
function init() {
    const fileContent = fs.readFileSync("./allstocktable.json");
    const objs = JSON.parse(fileContent);
    objs.forEach(v => {
        stockTableMap.push(v['table_fullname']);
    });
    const mockFileContent = fs.readFileSync("./mocktableinfo.json");
    const mockObjs = JSON.parse(mockFileContent);
    mockObjs.forEach(v => {
        mockTableMap.push(v["table_fullname"]);
    })
    const drinkFileContent = fs.readFileSync("./drinktableinfo.json");
    const drinkObjs = JSON.parse(drinkFileContent);
    drinkObjs.forEach(v => {
        drinkTableMap.push(v["table_fullname"]);
    })
    //console.log(stockTableMap);
}



const pool = getPool()
const levelMap = {
    "1t": 10,
    "2t": 11,
    "4t": 12,
    "8t": 13,
    "2ht": 14,
    "3ht": 15,
    "6ht": 16,
    "12ht": 17,
    "25ht": 18,
    "50ht": 19,
    "1m": 20,
    "2m": 21,
    "4m": 22,
    "8m": 23,
    "16m": 24,
    "32m": 25,
    "64m": 26,
    "1b": 27,
    "3b": 28,
    "10b": 30,
    "test": 4,
}
//const customDBPoolMap() = new Map();



let allTimes = [];

router.get('/m4_bench', m4BenchmarkHandler);
//
router.get('/init_wavelet_bench_min_max_miss', initWaveletBenchMinMaxMissHandler)
//
router.post("/batchLevelDataProgressiveWaveletMinMaxMiss", batchLevelDataProgressiveWaveletMinMaxMissPostHandler)
router.get('/init_multi_timeseries', init_multi_timeseries);
router.get("/getAllFlags", getAllFlags);

router.get('/getAllTables', getAllTables);

router.get('/getAllMultiLineClassInfo', getAllMulitLineClassInfo);
router.get('/getAllFlagNames', getAllFlagNames);
router.get('/getSingleFlag', getSingleFlag);
router.post('/batchLoadMinMaxMissWithPostForMultiLineType', queryMinMaxMissData)
router.post('/testDBConnection', testDBConnection);
router.post("/createCustomDBConn", createCustomDBConn);
router.get("/initOM3DBEnv", initOM3DBEnv);
router.get('/clearOM3Table', clearOM3Table);
router.get('/getAllCustomTables', getAllCustomTables);
router.get('/performTransformForSingeLine', performTransformForSingeLine);
router.get('/getAllCustomTableAndInfo', getAllCustomTableAndInfo);
router.get('/getAllDefaultTableAndInfo', getAllDefaultTableAndInfo);
router.get('/performTransformForMultiLine', performTransformForMultiLine)
//router.options('/batchLevelDataProgressiveWavelet',batchLevelDataProgressiveWaveletPostHandler)



let queryTime = [];
function m4BenchmarkHandler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    const query = req.query;
    query.width = query.width;
    let sqlStr = '';
    const splitArray = query.table_name.split("_");
    const level = levelMap[splitArray[splitArray.length - 1]];
    if (query.table_name.includes("non")) {
        console.log(query)
        //query.start_time = parseInt(query.start_time)//parseInt();

        sqlStr = `with Q as (select t,v from m4nonrawdata.sp_gaussiansin_24000000_non_32m where t>=$1 and t<=$2) select t,v from Q join (select floor(($3::double precision)/($5::double precision)*((t-$4)::double precision)) as k,min(v) as v_min,max(v) as v_max,min(t) as t_min,max(t) as t_max from Q group by k) as QA on k=floor(($6::double precision)/($8::double precision)*((t-$7)::double precision)) and (v=v_min or v=v_max or t=t_min or t=t_max) order by t asc`
        //sqlStr = `with Q as (select t,v from m4nonrawdata.${splitArray[0] + "_" + splitArray[1] + "_" + splitArray[2] + "_" + splitArray[4] + "_" + splitArray[5]} where t>=$1 and t<=$2) select t,v from Q join (select floor(($3::double precision)/($5::double precision)*((t-$4)::double precision)) as k,min(v) as v_min,max(v) as v_max,min(t) as t_min,max(t) as t_max from Q group by k) as QA on k=floor(($6::double precision)/($8::double precision)*((t-$7)::double precision)) and (v=v_min or v=v_max or t=t_min or t=t_max) order by t asc`
        //sqlStr = `with Q as (select t,v from ${query.table_name} where t>=$1 and t<=$2) select t,v from Q join (select floor(($3::double precision)/($5::double precision)*((t-$4)::double precision)) as k,min(v) as v_min,max(v) as v_max,min(t) as t_min,max(t) as t_max from Q group by k) as QA on k=floor(($6::double precision)/($8::double precision)*((t-$7)::double precision)) and (v=v_min or v=v_max or t=t_min or t=t_max) order by t asc`

    }
    else if ((query.table_name.includes("8m") && query.table_name.includes("sin")) || query.table_name.includes("guassian_sin") || query.table_name.includes("guassian_r")) {

        //splitArray.length = splitArray.length - 2;
        let tableName = splitArray.slice(0, splitArray.length - 2);
        tableName = tableName.join("_") + "_" + splitArray[splitArray.length - 1];
        // if(query.tableName==="mock_gaussian_x_plus_sinx_8m"){
        //     tableName=query.tableName;
        // }
        sqlStr = `with Q as (select t,v from m4mockrawdata.${tableName} where t>=$1 and t<=$2) select t,v from Q join (select floor(($3::double precision)/($5::double precision)*((t-$4)::double precision)) as k,min(v) as v_min,max(v) as v_max,min(t) as t_min,max(t) as t_max from Q group by k) as QA on k=floor(($6::double precision)/($8::double precision)*((t-$7)::double precision)) and (v=v_min or v=v_max or t=t_min or t=t_max) order by t asc`

    } else {
        sqlStr = `with Q as (select t,v from m4rawdata.${splitArray[0] + "_" + splitArray[1] + "_" + splitArray[3]} where t>=$1 and t<=$2) select t,v from Q join (select floor(($3::double precision)/($5::double precision)*((t-$4)::double precision)) as k,min(v) as v_min,max(v) as v_max,min(t) as t_min,max(t) as t_max from Q group by k) as QA on k=floor(($6::double precision)/($8::double precision)*((t-$7)::double precision)) and (v=v_min or v=v_max or t=t_min or t=t_max) order by t asc`

    }

    query.start_time = parseInt(query.start_time)//parseInt();
    query.end_time = parseInt(query.end_time);
    //query.end_time=23999999
    if (query.table_name === 'sensor8') {
        query.end_time = 8388607;
    }
    query.width = parseInt(query.width);
    console.log(query.table_name, query.width, query.start_time, query.end_time)

    const startT = new Date().getTime();
    const params = [query.start_time, query.end_time, query.width, query.start_time, (query.end_time - query.start_time + 1), query.width, query.start_time, (query.end_time - query.start_time + 1)];
    //console.log(params)
    const sqlQuery = {
        text: sqlStr,
        values: params
    }
    //console.log(sqlStr,params)
    pool.query(sqlQuery, function (err, result) {
        if (err) {
            //pool.end();
            //throw err;
            console.log(err)
            return
        }
        let qTime = new Date().getTime() - startT
        console.log("query time:", new Date().getTime() - startT);
        queryTime.push(qTime);
        console.log(queryTime);

        if (queryTime.length === 6) {
            queryTime = [];
        }
        res.send(result.rows);
    });
}


function initWaveletBenchMinMaxMissHandler(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");

    const query = req.query;
    const userCookie = req.headers['authorization'];
    let currentPool = pool
    if (query.mode === 'Custom') {
        if (userCookie === '' || userCookie === null || userCookie === undefined) {
            res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
            return
        }
        if (!customDBPoolMap().has(userCookie)) {
            res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
            return
        }
        currentPool = customDBPoolMap().get(userCookie);
    }
    const splitArray = query.table_name.split("_");
    let maxLevel = levelMap[splitArray[splitArray.length - 1]];
    console.log(maxLevel)
    let sqlStr = '';
    sqlStr = `select i,minvd,maxvd from  om3.${query.table_name} where i<$1 order by i asc`;
    const params = [];
    console.log(query.width)
    params.push(2 ** Math.ceil(Math.log2(query.width)));
    const sqlQuery = {
        text: sqlStr,
        values: params
    }
    const startT = new Date().getTime();
    try {
        currentPool.query(sqlQuery, function (err, result) {
            if (err) {
                //currentPool.end();
                console.log(sqlStr)
                throw err;
            }
            console.log(sqlStr)
            const finalRes = []

            for (let i = 1; i < result.rows.length; i++) {
                const tempVal = result.rows[i];
                const tempL = Math.floor(Math.log2(tempVal['i']));
                const tempI = tempVal['i'] - 2 ** tempL;
                finalRes.push({ l: maxLevel - tempL, i: tempI, minvd: tempVal['minvd'], maxvd: tempVal['maxvd'] });
            }
            console.log(finalRes.length)
            if (result.rows.length > 0) {
                finalRes.push({ l: -1, i: 0, minvd: result.rows[0]['minvd'], maxvd: result.rows[0]['maxvd'] });
            }
            // printT(startT)
            console.log("w i t", new Date().getTime() - startT);
            res.send({ code: 200, msg: 'success', data: { result: finalRes } });
        });
    } catch (err) {
        res.send({ code: 500, msg: err });
        console.log(err)
    }

}



function batchLevelDataProgressiveWaveletMinMaxMissPostHandler(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
    res.setHeader('Access-Control-Allow-Credentials', true);


    const query = { table_name: req.body.table_name, line_type: req.body.line_type, mode: req.body.mode };
    const userCookie = req.headers['authorization'];
    let currentPool = pool
    if (query.mode === 'Custom') {
        if (userCookie === '' || userCookie === null || userCookie === undefined) {
            res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
            return
        }
        if (!customDBPoolMap().has(userCookie)) {
            res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
            return
        }
        currentPool = customDBPoolMap().get(userCookie);
    }

    let maxLevel = getTableLevel(query.table_name)//levelMap[splitArray[splitArray.length - 1]];


    const strObj = req.body.losedDataInfo;
    const tempArray1 = JSON.parse(strObj);


    if (tempArray1['data'].length === 0) {
        res.send({ code: 400, msg: " query null" })
        return
    }
    const needRangeArray = tempArray1['data'];
    console.log(query.table_name)
    let tName = '';
    if (query.line_type === 'Single') {
        tName = `${"om3"}.${query.table_name}`;
    } else {
        tName = `${"om3_multi"}.${query.table_name}`;
    }

    const condition = generateSingalLevelSQLWithSubQueryMinMaxMiss(needRangeArray, maxLevel, tName);

    if (condition === null) {
        console.log(needRangeArray);
        res.send({ code: 400, msg: "level error" })
        return
    }
    // sqlStr+=condition +" order by l desc,i";
    let sqlStr = condition + " order by i asc";

    const startT = new Date().getTime();
    currentPool.query(sqlStr, function (err, result) {
        if (err) {
            console.log(sqlStr);
            currentPool.end();
            throw err;
        }
        // printT(startT)


        const minV = [];
        const maxV = [];
        const l = [];
        const idx = [];

        result.rows.forEach((v, i) => {
            const tempVal = v;
            const tempL = Math.floor(Math.log2(tempVal['i']));
            const tempI = tempVal['i'] - 2 ** tempL;
            l.push(tempL);
            idx.push(tempI);
            minV.push(v['minvd']);
            maxV.push(v['maxvd']);
        });

        res.send({ code: 200, msg: "success", data: [l, idx, minV, maxV] });
    });
}





let allWaveletTables = []
function getAllTables(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");


    const sqlStr = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema');`
    pool.query(sqlStr, (err, result) => {
        if (err) {
            pool.end();
            throw err
        }

        allWaveletTables = result.rows.filter((v) => {
            if (v.table_fullname.includes("om3.")) {//|| v.table_fullname.includes("m4waveletrawminmaxdata.")
                return true
            } else {
                return false;
            }
        });

        res.send(allWaveletTables)
    });
}
let allTables = [];
let lineType = ''
let maxLevel = 20;


let allMultiSeriesTables = [];
let multiSeriesClass = 'stock'
let multiSeriesMaxLevel = 20;

function init_multi_timeseries(req, res) {
    res.setHeader("Access-Control-Allow-Origin", "*");
    const query = req.query;
    const lineClassName = query['class_name'];
    const retureRes = [];
    const userCookie = req.headers['authorization'];
    let currentPool = pool
    if (query.mode === 'Custom') {
        if (userCookie === '' || userCookie === null || userCookie === undefined) {
            res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
            return
        }
        if (!customDBPoolMap().has(userCookie)) {
            res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
            return
        }
        currentPool = customDBPoolMap().get(userCookie);
    }
    new Promise((resolve, reject) => {
        if (allMultiSeriesTables.length < 1 || lineClassName !== multiSeriesClass) {
            allMultiSeriesTables = [];
            const sqlStr = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema') and table_schema||'.'||table_name  like '%om3_multi.${lineClassName}%';`
            currentPool.query(sqlStr, (err, result) => {
                if (err) {
                    currentPool.end();
                    throw err
                }
                for (let i = 0; i < result.rows.length; i++) {
                    const curTableClass = result.rows[i].table_fullname.split(".")[1].split("_")[0];
                    if (curTableClass == lineClassName) {
                        allMultiSeriesTables.push(result.rows[i].table_fullname);
                    }
                }
                multiSeriesClass = lineClassName;
                resolve();
            });
        } else {
            resolve();
        }
    }).then(() => {
        const allPromises = new Array();
        let amout = allMultiSeriesTables.length


        for (let i = 0; i < amout; i++) {
            allPromises.push(new Promise((resolve, reject) => {
                const curTableLevel = getTableLevel(allMultiSeriesTables[i]);
                const timeSeriresRes = {
                    tn: allMultiSeriesTables[i],
                    d: [],
                    l: curTableLevel,
                }

                const sqlStr = `select i,minvd,maxvd from ${allMultiSeriesTables[i]} where i<$1 order by i asc`;
                const params = [];
                params.push(2 ** Math.ceil(Math.log2(query.width)));
                const sqlQuery = {
                    text: sqlStr,
                    values: params
                }
                currentPool.query(sqlQuery, (err, result) => {
                    if (err) {
                        console.log(sqlStr)
                        currentPool.end();
                        throw err;
                    }
                    const finalRes = []

                    for (let i = 1; i < result.rows.length; i++) {
                        const tempVal = result.rows[i];
                        const tempL = Math.floor(Math.log2(tempVal['i']));
                        const tempI = tempVal['i'] - 2 ** tempL;
                        finalRes.push({ l: curTableLevel - tempL, i: tempI, minvd: tempVal['minvd'], maxvd: tempVal['maxvd'] });
                    }
                    if (result.rows.length > 0) {
                        finalRes.push({ l: -1, i: 0, minvd: result.rows[0]['minvd'], maxvd: result.rows[0]['maxvd'] });
                    }
                    timeSeriresRes.d = finalRes;
                    //console.log(timeSeriresRes)
                    retureRes.push(timeSeriresRes)
                    resolve();
                });
            }));
        }
        Promise.all(allPromises).then(() => {
            //console.log(finalRes)
            res.send(retureRes);
        });
    });
}

function queryMinMaxMissData(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
    res.setHeader('Access-Control-Allow-Credentials', true);
    const query = { table_name: req.body.table_name, line_type: req.body.line_type, mode: req.body.mode };
    const userCookie = req.headers['authorization'];
    let currentPool = pool
    if (query.mode === 'Custom') {
        if (userCookie === '' || userCookie === null || userCookie === undefined) {
            res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
            return
        }
        if (!customDBPoolMap().has(userCookie)) {
            res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
            return
        }
        currentPool = customDBPoolMap().get(userCookie);
    }

    const splitArray = query.table_name.split("_");
    let maxLevel = levelMap[splitArray[splitArray.length - 1]];
    const strObj = req.body.losedDataInfo;
    //console.log(req.body)

    const needRangeArray = strObj["data"];

    const curLevel = needRangeArray[0][0];
    if (curLevel === undefined) {
        console.log(needRangeArray);
        ws.send({ code: 400, msg: "level error" })
        return
    }

    let schema = "";
    if (query.line_type === 'Single') {
        schema = "om3";
    } else {
        schema = "om3_multi";
    }
    const tName = `${schema}.${query.table_name}`;
    console.log(tName)
    const condition = generateSingalLevelSQLMinMaxMissQuery(needRangeArray, maxLevel, tName);
    if (condition === null) {
        console.log(needRangeArray);
        ws.send({ code: 400, msg: "level error" })
        return
    }
    let sqlStr = condition + " order by i asc";


    currentPool.query(sqlStr, function (err, result) {
        if (err) {
            console.log(sqlStr);
            currentPool.end();
            throw err;
        }
        const minV = [];
        const maxV = [];
        const l = [];
        const idx = [];
        result.rows.forEach((v) => {
            const curI = v['i'];

            l.push(curLevel);
            idx.push(curI - 2 ** curLevel);
            minV.push(v['minvd']);
            maxV.push(v['maxvd']);
        });
        res.send({ code: 200, msg: "success", data: [l, idx, minV, maxV] });
    });
}



function getAllFlags(req, res) {
    const allSingleFlagNames = fs.readdirSync("./flags/single_line");
    const allMultiFlagNames = fs.readFileSync("./flags/single_line");
    const allFlagNames = [];
    allSingleFlagNames.map((v => {
        allFlagNames.push("sing_" + v);
    }))
    allMultiFlagNames.map((v => {
        allFlagNames.push("multi_" + v);
    }))
    const resData = {};
    for (let i = 0; i < allFlagNames.length; i++) {

        const curName = allFlagNames[i];
        let filePath = "./flags/";
        if (curName.startsWith("sing_")) {
            filePath = filePath + "single_line/";
        } else {
            filePath = filePath + "multi_line/"
        }
        let buf = fs.readFileSync(filePath + curName).buffer
        buf = new Uint8Array(buf);
        resData[curName.split(".")[0]] = buf;
    }
    res.send(resData);
}
function getSingleFlag(req, res) {
    let curName = req.query['name'];
    let lineType = req.query['line_type'];
    res.set({
        'Content-Type': 'application/octet-stream',
    });
    let filePath = "./flags/";
    if (lineType === 'Single') {
        filePath = filePath + "single_line/";
    } else {
        filePath = filePath + "multi_line/"
    }
    let buf = fs.readFileSync(filePath + curName)
    res.send(buf)
}


function getAllMulitLineClassInfo(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    const sql = "select * from om3_setting.multi_line_class_info"
    const query = req.query;
    const userCookie = req.headers['authorization'];
    let currentPool = pool
    if (query.mode === 'Custom') {
        if (userCookie === '' || userCookie === null || userCookie === undefined) {
            res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
            return
        }
        if (!customDBPoolMap().has(userCookie)) {
            res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
            return
        }
        currentPool = customDBPoolMap().get(userCookie);
    }
    try {
        currentPool.query(sql, (err, result) => {
            if (err) {
                throw err;
            }
            res.send({
                code: 200,
                msg: "success",
                data: result.rows
            })
        })
    } catch (err) {
        console.log(err)
        res.send({
            code: 400,
            msg: err,
        })
    }

}

function getAllFlagNames(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    const lineType=req.query['line_type'];
    let filePath = "./flags/";
    if (lineType === 'Single') {
        filePath = filePath + "single_line";
    } else {
        filePath = filePath + "multi_line"
    }
    const allFlagNames = fs.readdirSync(filePath);
    res.send({ code: 200, msg: "success", data: allFlagNames });
}

async function testDBConnection(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
    res.setHeader('Access-Control-Allow-Credentials', true);
    const query = { host_name: req.body.host_name, password: req.body.password, db_name: req.body.db_name, user_name: req.body.user_name };
    console.log("cookie:", req.headers['authorization'])
    const testPool = new Pool({
        user: query.user_name,
        host: query.host_name,
        database: query.db_name,
        password: query.password,
    });

    try {
        const testRes = await testPool.query("select 1 as num");
        if (testRes.rowCount > 0) {
            res.send({ code: 200, msg: "success", data: { result: "success" } })
        } else {
            res.send({ code: 200, msg: err, data: { result: "fail" } })
        }


    } catch (err) {

        res.send({ code: 500, msg: err, data: { result: "fail" } })
        console.log(err)
    }
    testPool.end()

}

function createCustomDBConn(req, res) {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
    res.setHeader('Access-Control-Allow-Credentials', true);

    const query = { host_name: req.body.host_name, password: req.body.password, db_name: req.body.db_name, user_name: req.body.user_name };
    const userCookie = req.headers['authorization'];
    console.log("create conn:", userCookie)
    const testPool = new Pool({
        user: query.user_name,
        host: query.host_name,
        database: query.db_name,
        password: query.password,
    });
    if (customDBPoolMap().has(userCookie)) {
        customDBPoolMap().get(userCookie).end();
    }
    customDBPoolMap().set(userCookie, testPool);
    res.send({ code: 200, msg: "success", data: { result: "success" } })
}

function initOM3DBEnv(req, res) {
    const userCookie = req.headers['authorization'];
    if (userCookie === '' || userCookie === null || userCookie === undefined) {
        res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
        return
    }
    if (!customDBPoolMap().has(userCookie)) {
        res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
        return
    }
    const tempPool = customDBPoolMap().get(userCookie);
    const createSchemaSql = `create schema IF NOT EXISTS om3;create schema om3_multi;create schema IF NOT EXISTS raw_data;create schema IF NOT EXISTS raw_data_multi;create schema IF NOT EXISTS om3_setting;`
    const createClassTableSql = `create table IF NOT EXISTS om3_setting.multi_line_class_info(name character varying primary key,level integer not null,amount integer not null,max_len integer,start_time timestamp not null,end_time timestamp,interval integer);`
    const createSingleInfoSql = `create table IF NOT EXISTS om3_setting.single_line_info(name character varying primary key,max_len integer not null,level integer not null,start_time timestamp not null,end_time timestamp,interval integer);`
    try {
        tempPool.query(createSchemaSql + createClassTableSql + createSingleInfoSql, (err, resu) => {
            if (err) {
                res.send({ code: 400, msg: err, data: { result: "fail" } })
            }
            res.send({ code: 200, msg: 'success', data: { result: "success" } })

        })
    } catch (err) {
        console.log(err)
    }
}

function clearOM3Table(req, res) {
    const userCookie = req.headers['authorization'];
    if (userCookie === '' || userCookie === null || userCookie === undefined) {
        res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
        return
    }
    if (!customDBPoolMap().has(userCookie)) {
        res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
        return
    }
    const tempPool = customDBPoolMap().get(userCookie);
    const dropSql = `drop schema om3 cascade; drop schema om3_multi cascade; drop schema raw_data_multi cascade; drop schema om3_setting cascade;`
    try {
        tempPool.query(dropSql, (err, resu) => {
            if (err) {
                res.send({ code: 400, msg: err, data: { result: "fail" } })
                return
            }
            res.send({ code: 200, msg: 'success', data: { result: "success" } })
            return
        })

    } catch (err) {
        res.send({ code: 400, msg: err, data: { result: "fail" } })
        return
    }
}

function getAllCustomTables(req, res) {
    const userCookie = req.headers['authorization'];
    if (userCookie === '' || userCookie === null || userCookie === undefined) {
        res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
        return
    }
    if (!customDBPoolMap().has(userCookie)) {
        res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
        return
    }
    const tempPool = customDBPoolMap().get(userCookie);
    const sqlStr = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema');`
    try {
        tempPool.query(sqlStr, (err, result) => {
            if (err) {
                console.log(err);
                res.send({ code: 400, msg: err, data: {} })
                return
            }
            res.send({ code: 200, msg: "success", data: { result: result.rows.map(v => v['table_fullname']).filter((v => v.includes("om3"))) } })
        });

    } catch (err) {
        console.log(err);
        res.send({ code: 400, msg: err, data: {} })
    }
}


async function performTransformForSingeLine(req, res) {
    const userCookie = req.headers['authorization'];
    if (userCookie === '' || userCookie === null || userCookie === undefined) {
        res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
        return
    }
    if (!customDBPoolMap().has(userCookie)) {
        res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
        return
    }
    try {
        const tempPool = customDBPoolMap().get(userCookie);
        const query = { startTime: req.query['start_time'], endTime: req.query['end_time'], tableName: req.query['table_name'], mode: req.query['mode'] };
        const maxTSql = `select max(t) as max_t from ${query.tableName}`;
        let maxT = await new Promise((resolve, reject) => {
            tempPool.query(maxTSql, (err, result) => {
                if (err) {
                    reject(err)
                    return
                }
                resolve(result.rows[0]['max_t']);
            })
        }).catch((err) => {
            throw err
        })
        if (maxT === undefined || maxT <= 0) {
            throw new Error("origin table t illegal")
        }
        const newTableName = generateOM3TableName(query.tableName, maxT);
        const createOm3TableSql = `DROP TABLE IF EXISTS ${newTableName};create table ${newTableName}(i integer primary key,minvd double precision,maxvd double precision)`;
        await new Promise((resolve, reject) => {
            tempPool.query(createOm3TableSql, (err, result) => {
                if (err) {
                    console.log(createOm3TableSql);
                    reject(err)
                    return
                }
                resolve()

            })
        }).catch((err) => {
            throw err
        })
        const computeRes = await nonuniformMinMaxEncode(tempPool, query.tableName, newTableName, query.mode === undefined ? "Custom" : query.mode)
        const createInfoSql = `insert into om3_setting.single_line_info(name,max_len,level,start_time,end_time,interval) values('${computeRes.name}',${computeRes.maxLen},${computeRes.maxLevel},'${query.startTime}','${query.endTime}',${0}) ON CONFLICT (name) DO UPDATE SET max_len=EXCLUDED.max_len,level=EXCLUDED.level,start_time=EXCLUDED.start_time,end_time=EXCLUDED.end_time,interval=EXCLUDED.interval;`;
        await new Promise((resolve, reject) => {
            tempPool.query(createInfoSql, (err, result) => {
                if (err) {
                    console.log(createInfoSql)
                    reject(err)
                    return
                }
                resolve()
            })
        }).catch((err) => {
            throw err
        })
    } catch (err) {
        res.send({ code: 500, msg: err })
        return
    }
    res.send({ code: 200, msg: "success", data: { result: "success" } })
}

async function performTransformForMultiLine(req, res) {
    const userCookie = req.headers['authorization'];
    if (userCookie === '' || userCookie === null || userCookie === undefined) {
        res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
        return
    }
    if (!customDBPoolMap().has(userCookie)) {
        res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
        return
    }
    try {
        const tempPool = customDBPoolMap().get(userCookie);
        const query = { startTime: req.query['start_time'], endTime: req.query['end_time'], tableName: req.query['table_name'].split(","), mode: req.query['mode'], lineClass: req.query['line_class'] };

        let globalLevel = 0;
        const maxTArray = [];
        let globalMaxT = 0;

        // check table level
        for (let i = 0; i < query.tableName.length; i++) {
            const maxTSql = `select max(t) as max_t from ${query.tableName[i]}`;
            let maxT = await new Promise((resolve, reject) => {
                tempPool.query(maxTSql, (err, result) => {
                    if (err) {
                        console.log(maxTSql)
                        reject(err)
                        return
                    }
                    resolve(result.rows[0]['max_t']);
                })
            }).catch((err) => {
                throw err
            })
            if (maxT === undefined || maxT <= 0) {
                res.send({ code: 400, msg: "origin table t illegal:" + query.tableName[i], data: { result: "fail" } })
                return
            }
            const curL = computeLevelFromT(maxT);
            if (i === 0) {
                globalLevel = curL;
            }
            if (curL !== globalLevel) {
                res.send({ code: 400, msg: "tables have different length", data: { result: "fail" } })
                return
            }
            maxTArray.push(maxT)
            globalMaxT = Math.max(globalMaxT, maxT);
        }

        const newTableNames = []

        // crate om3 table
        for (let i = 0; i < query.tableName.length; i++) {
            const newTableName = generateOM3TableName(query.tableName[i], maxTArray[i], query.lineClass);
            const createOm3TableSql = `DROP TABLE IF EXISTS ${newTableName};create table ${newTableName}(i integer primary key,minvd double precision,maxvd double precision)`;
            await new Promise((resolve, reject) => {
                tempPool.query(createOm3TableSql, (err, result) => {
                    if (err) {
                        console.log(createOm3TableSql);
                        reject(err)
                        return
                    }
                    resolve()

                })
            }).catch((err) => {
                throw err
            })
            newTableNames.push(newTableName);
        }
        console.log("create table finish")

        console.log("start transfom data")
        // do om3 transform
        for (let i = 0; i < query.tableName.length; i++) {
            console.log("transform data name:", query.tableName[i])
            await nonuniformMinMaxEncode(tempPool, query.tableName[i], newTableNames[i], 'Custom');
        }
        console.log("transform data finish")
        console.log("start create info table")
        const createInfoSql = `insert into om3_setting.multi_line_class_info(name,level,amount,max_len,start_time,end_time,interval) values('${query.lineClass}',${globalLevel},${query.tableName.length},${globalMaxT},'${query.startTime}','${query.endTime}',${0}) ON CONFLICT (name) DO UPDATE SET amount=EXCLUDED.amount,max_len=EXCLUDED.max_len,level=EXCLUDED.level,start_time=EXCLUDED.start_time,end_time=EXCLUDED.end_time,interval=EXCLUDED.interval;`;
        await new Promise((resolve, reject) => {
            tempPool.query(createInfoSql, (err, result) => {
                if (err) {
                    console.log(createInfoSql)
                    reject(err)
                    return
                }
                resolve()
            })
        }).catch((err) => {
            throw err
        })
        console.log("create info table finish")
    } catch (err) {
        res.send({ code: 500, msg: err })
        return
    }
    res.send({ code: 200, msg: "success", data: { result: "success" } })
}


async function getAllCustomTableAndInfo(req, res) {
    const userCookie = req.headers['authorization'];
    if (userCookie === '' || userCookie === null || userCookie === undefined) {
        res.send({ code: 400, msg: "cookie not found", data: { result: "fail" } })
        return
    }
    if (!customDBPoolMap().has(userCookie)) {
        res.send({ code: 400, msg: "custom db not create connection", data: { result: "fail" } })
        return
    }
    try {
        const tempPool = customDBPoolMap().get(userCookie);

        const sqlStr = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema') and table_schema||'.'||table_name like 'om3.%';`
        const allTables = await new Promise((resolve, reject) => {
            tempPool.query(sqlStr, (err, result) => {
                if (err) {
                    throw err
                }
                resolve(result.rows.map((v => v['table_fullname'])));
            })
        })
        const infoQuerySql = `select * from om3_setting.single_line_info`
        const allTableInfo = await new Promise((resolve, reject) => {
            tempPool.query(infoQuerySql, (err, result) => {
                if (err) {
                    console.log(err)
                    reject(err)
                    return
                }
                resolve(result.rows)
            })
        })
        res.send({ code: 200, msg: "success", data: { table_name: allTables, table_info: allTableInfo } })
    } catch (err) {
        res.send({ code: 400, msg: err })
    }
}

async function getAllDefaultTableAndInfo(req, res) {
    try {
        const sqlStr = `select table_schema||'.'||table_name as table_fullname from information_schema."tables" where table_type = 'BASE TABLE' and table_schema not in ('pg_catalog', 'information_schema') and table_schema||'.'||table_name like 'om3.%';`
        const allTables = await new Promise((resolve, reject) => {
            pool.query(sqlStr, (err, result) => {
                if (err) {
                    throw err
                }
                resolve(result.rows.map((v => v['table_fullname'])));
            })
        })
        const infoQuerySql = `select * from om3_setting.single_line_info`
        const allTableInfo = await new Promise((resolve, reject) => {
            pool.query(infoQuerySql, (err, result) => {
                if (err) {
                    console.log(err)
                    reject(err)
                    return
                    //throw err
                }
                resolve(result.rows)
            })
        })
        //console.log(allTableInfo);
        //console.log(allTables)

        res.send({ code: 200, msg: "success", data: { table_name: allTables, table_info: allTableInfo } })
    } catch (err) {
        res.send({ code: 400, msg: err })
    }
}




module.exports = router;
