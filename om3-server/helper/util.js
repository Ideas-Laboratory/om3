const { Pool } = require("pg");
const fs=require('fs')


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

const levelNameMap = {
    10: "1t",
    11: "2t",
    12: "4t",
    13: "8t",
    14: "2ht",
    15: "3ht",
    16: "6ht",
    17: "12ht",
    18: "25ht",
    19: "50ht",
    20: "1m",
    21: "2m",
    22: "4m",
    23: "8m",
    24: "16m",
    25: "32m",
    26: "64m",
    27: "1b",
    28: "3b",
    30: "10b",
    4: "test",
}

function getTableLevel(tableName) {
    let processName = tableName
    if (tableName.includes('.')) {
        processName = tableName.split(".")[1]
    }
    let tableSplitArray = processName.split("_");
    const tableRowAmount = tableSplitArray[tableSplitArray.length - 1];
    return levelMap[tableRowAmount]
}

function generateOM3TableName(rawName, maxT, className) {
    let tempName = rawName;
    if (rawName.includes(".")) {
        tempName = tempName.split(".")[1];
    }
    const levelNum = Math.ceil(Math.log2(maxT));
    const levelStr = levelNameMap[levelNum];
    if (levelStr === undefined) {
        return ""
    }
    if (className) {
        return `om3_multi.${className}_${tempName}_om3_${levelStr}`
    } else {
        return `om3.${tempName}_om3_${levelStr}`
    }
}

function computeLevelFromT(t) {
    return Math.ceil(Math.log2(t))
}

const customDBPoolConfigMap = new Map()
function customDBPoolMap() {
    return customDBPoolConfigMap
}


function getPool() {
    const isDocker = process.env.DOCKER_ENV
    if (isDocker !== undefined && isDocker.length > 0) {
        const dbHost = process.env.POSTGRES_HOST;
        const dbUser = process.env.POSTGRES_USER;
        const dbPassword = process.env.POSTGRES_PASSWORD;
        const dbName = process.env.POSTGRES_DB;
        console.log(dbHost,dbUser,dbPassword,dbName);
        if (dbHost === undefined || dbUser === undefined || dbPassword === undefined || dbName === undefined) {
            throw new Error("cannot create db connection")
        }
        return new Pool({
            user: dbUser,
            host: dbHost,
            database: dbName,
            password: dbPassword,
            port:5432,
        });
    } else {
        const dbConfig = JSON.parse(fs.readFileSync("./initdb/dbconfig.json").toString());
        console.log(dbConfig)
        if (!dbConfig['username'] || !dbConfig['hostname'] || !dbConfig['password'] || !dbConfig['db']) {
            throw new Error("db config error");
        }
        return pool = new Pool({
            user: dbConfig['username'],
            host: dbConfig["hostname"],
            database: dbConfig['db'],
            password: dbConfig['password'],
        });
        
    }
}



module.exports = { getTableLevel, generateOM3TableName, customDBPoolMap, computeLevelFromT ,getPool}