const {generateSingalLevelSQLMinMaxMissQuery} = require('../helper/generate_sql');
const {customDBPoolMap, getPool} =require('../helper/util')

const routerMap = new Map()
const levelMap = {
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
    "test":4,
}

function Init() {
    routerMap.set("level_load_data_min_max_miss",queryMinMaxMissData)
}

const pool =getPool()

function queryMinMaxMissData(req, ws){
    const query = { table_name: req.tn ,line_type:req.line_type,mode:req.mode,user_cookie:req.user_cookie};
   
    console.log(query);
    let currentPool=pool
    const userCookie=req.user_cookie;
    if(query.mode==='Custom'){
        if (userCookie === '' || userCookie === null || userCookie === undefined) {
            ws.send(JSON.stringify({ code: 400, msg: "cookie not found", data: { result: "fail" } }))
            return
        }
        if (!customDBPoolMap().has(userCookie)) {
            ws.send(JSON.stringify({ code: 400, msg: "custom db not create connection", data: { result: "fail" } }))
            return
        }
        currentPool=customDBPoolMap().get(userCookie);
    }

    const splitArray = query.table_name.split("_");
    let maxLevel = levelMap[splitArray[splitArray.length - 1]];
    const strObj = req.data;

    const needRangeArray = JSON.parse(strObj)["data"];

    const curLevel=needRangeArray[0][0];
    if(curLevel===undefined){
        console.log(needRangeArray);
        ws.send({ code: 400, msg: "level error" })
        return
    }
    let schema="";
    if(query.line_type==='Single'){
        schema="om3";
    }else{
        schema="om3_multi";
    }
    const tName=`${schema}.${query.table_name}`;
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
            const curI=v['i'];
            
            l.push(curLevel);
            idx.push(curI-2**curLevel);
            minV.push(v['minvd']);
            maxV.push(v['maxvd']);
        });
        ws.send(JSON.stringify({ code: 200, msg: "success", data: [l, idx, minV, maxV] }));
    });
}

module.exports = { Init, routerMap }