const fs = require('fs')
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

async function initTable(){
    try{
        const schemaSQL="create schema raw_data;create schema om3";
        await pool.query(schemaSQL)
        const table1SQL="create table raw_data.mock_guassian_sin_8m(t integer,v double precision);"
        await pool.query(table1SQL);
        const table2SQL="create table om3.mock_guassian_sin_om3_8m(i integer primary key,minvd double precision,maxvd double precision);"
        await pool.query(table2SQL);
    }catch(err){
        pool.end()
        throw err
    }
    pool.end();
   
}
initTable()


