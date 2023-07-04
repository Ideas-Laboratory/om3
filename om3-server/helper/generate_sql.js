const fs = require("fs")
const tableMap = new Map();
function initMap() {
    const fileContent = fs.readFileSync("./allstocktable.json");
    const objs = JSON.parse(fileContent);
    for (let i = 0; i < objs.length; i++) {
        tableMap.set(objs[i]['table_fullname'], i);
    }
    console.log("map_init")
    // console.log(tableMap)
}
//initMap();

function generateMultiLevelSQL(losedDataInfo, maxLevel) {
    let sqlStr = '';
    for (let i = 0; i < losedDataInfo.length; i++) {
        const curL = maxLevel - losedDataInfo[i][0] + 1;
        if (curL < 1) {
            return null
        }
        if (i === 0) {
            sqlStr += `(l=${curL} and i>=${Math.floor(losedDataInfo[i][1] / 2)} and i<=${Math.floor(losedDataInfo[i][2] / 2)})`;
        } else {
            sqlStr += ` or (l=${curL} and i>=${Math.floor(losedDataInfo[i][1] / 2)} and i<=${Math.floor(losedDataInfo[i][2] / 2)})`;
        }
    }
    return sqlStr;
}
function generateAllLevelSQLQuery(losedDataInfo, maxLevel, tableName) {
    let sqlStr = `select l,i,maxvd,maxid from ${tableName} where `;
    for (let lIndex = 0; lIndex < losedDataInfo.length; lIndex++) {
        const curL = maxLevel - losedDataInfo[lIndex][0][0];
        if (curL < 1) {
            return null;
        }

        let tempStr = ` (l=${curL} and i in (`;
        if (lIndex !== 0) {
            tempStr = " or" + tempStr
        }
        for (let i = 0; i < losedDataInfo[lIndex].length - 1; i++) {
            if (losedDataInfo[lIndex][i][2] === losedDataInfo[lIndex][i + 1][1] || (losedDataInfo[lIndex][i][2] + 1) === losedDataInfo[lIndex][i + 1][1]) {
                losedDataInfo[lIndex][i][2] = losedDataInfo[lIndex][i + 1][2];
                losedDataInfo[lIndex].splice(i + 1, 1)
                i--;
            }
        }
        for (let i = 0; i < losedDataInfo[lIndex].length; i++) {
            const first = losedDataInfo[lIndex][i][1];
            const second = losedDataInfo[lIndex][i][2];
            if (first === second) {
                tempStr += `${first},`
            } else {
                for (let j = first; j <= second; j++) {
                    tempStr += `${j},`
                }
            }
        }
        tempStr = tempStr.substring(0, tempStr.length - 1);
        tempStr = tempStr + "))";
        sqlStr = sqlStr + tempStr;
    }
    return sqlStr;
}
function generateAllLevelParallelSQLQuery(losedDataInfo, maxLevel, tableName) {
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from ${tableName} where `;
    const sqlStrs = [];
    for (let lIndex = 0; lIndex < losedDataInfo.length; lIndex++) {
        let sqlStr = `select l,i,minvd,maxvd from ${tableName} where `;
        const curL = maxLevel - losedDataInfo[lIndex][0][0];
        if (curL < 1) {
            return null;
        }

        let tempStr = ` l=${curL} and i in (`;
        for (let i = 0; i < losedDataInfo[lIndex].length - 1; i++) {
            if (losedDataInfo[lIndex][i][2] === losedDataInfo[lIndex][i + 1][1] || (losedDataInfo[lIndex][i][2] + 1) === losedDataInfo[lIndex][i + 1][1]) {
                losedDataInfo[lIndex][i][2] = losedDataInfo[lIndex][i + 1][2];
                losedDataInfo[lIndex].splice(i + 1, 1)
                i--;
            }
        }
        for (let i = 0; i < losedDataInfo[lIndex].length; i++) {
            const first = losedDataInfo[lIndex][i][1];
            const second = losedDataInfo[lIndex][i][2];
            if (first === second) {
                tempStr += `${first},`
            } else {
                for (let j = first; j <= second; j++) {
                    tempStr += `${j},`
                }
            }
        }
        tempStr = tempStr.substring(0, tempStr.length - 1);
        tempStr = tempStr + ")";
        sqlStr = sqlStr + tempStr;
        sqlStrs.push(sqlStr);
    }
    return sqlStrs;
}

function generateSingalLevelSQL(losedDataInfo, maxLevel) {
    const curL = maxLevel - losedDataInfo[0][0] + 1;
    if (curL < 1) {
        return null;
    }
    let sqlStr = `l=${curL} and `;
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = Math.floor(losedDataInfo[i][1] / 2);
        const second = Math.floor(losedDataInfo[i][2] / 2);
        if (i === 0) {
            sqlStr += `( ${first === second ? `i=${first}` : `(i>=${first} and i<=${second})`} `;
        } else {
            sqlStr += `or ${first === second ? `i=${first}` : `(i>=${first} and i<=${second})`} `;
        }
    }
    return sqlStr + ')';
}
function generateSingalLevelSQLWithSubQuery(losedDataInfo, maxLevel, tableName) {

    const curL = maxLevel - losedDataInfo[0][0] + 1;
    if (curL < 1) {
        return null;
    }
    const firstT = Math.floor(losedDataInfo[0][1] / 2);
    const lastT = Math.floor(losedDataInfo[losedDataInfo.length - 1][2] / 2);
    //let withQuery = `with wavelet_table as(select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and  i>=${firstT} and i<=${lastT}) `
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from wavelet_table where l=${curL} and `;
    let sqlStr = `select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and i in (`;
    //console.log(sqlStr)
    for (let i = 0; i < losedDataInfo.length; i++) {
        losedDataInfo[i][1] = Math.floor(losedDataInfo[i][1] / 2);
        losedDataInfo[i][2] = Math.floor(losedDataInfo[i][2] / 2);
    }
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}
function generateSingalLevelSQLWithSubQueryMinMaxMiss(losedDataInfo, maxLevel, tableName) {

    const curL =losedDataInfo[0][0] - 1;
    if (curL < 1) {
        return null;
    }
    const firstT = Math.floor(losedDataInfo[0][1] / 2);
    const lastT = Math.floor(losedDataInfo[losedDataInfo.length - 1][2] / 2);
    //let withQuery = `with wavelet_table as(select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and  i>=${firstT} and i<=${lastT}) `
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from wavelet_table where l=${curL} and `;
    let sqlStr = `select i,minvd,maxvd from ${tableName} where  i in (`;
    //console.log(sqlStr)
    for (let i = 0; i < losedDataInfo.length; i++) {
        losedDataInfo[i][1] = Math.floor(losedDataInfo[i][1] / 2);
        losedDataInfo[i][2] = Math.floor(losedDataInfo[i][2] / 2);
    }
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${2**curL+first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${2**curL+j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}
function generateSingalLevelSQLWithSubQueryRawMinMax(losedDataInfo, maxLevel, tableName) {

    const curL = maxLevel - losedDataInfo[0][0] ;
    if (curL < 1) {
        return null;
    }
    const firstT = Math.floor(losedDataInfo[0][1] / 2);
    const lastT = Math.floor(losedDataInfo[losedDataInfo.length - 1][2] / 2);
    //let withQuery = `with wavelet_table as(select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and  i>=${firstT} and i<=${lastT}) `
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from wavelet_table where l=${curL} and `;
    let sqlStr = `select l,i,mint,minv,maxv,maxt from m4waveletrawminmaxdata.${tableName} where l=${curL} and i in (`;
    //console.log(sqlStr)
    // for (let i = 0; i < losedDataInfo.length; i++) {
    //     losedDataInfo[i][1] = Math.floor(losedDataInfo[i][1] / 2);
    //     losedDataInfo[i][2] = Math.floor(losedDataInfo[i][2] / 2);
    // }
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}

function generateSingalLevelSQLWithSubQuery21test(losedDataInfo, maxLevel, tableName) {

    const curL = maxLevel - losedDataInfo[0][0];
    if (curL < 0) {
        return null;
    }
    const firstT = Math.floor(losedDataInfo[0][1] / 2);
    const lastT = Math.floor(losedDataInfo[losedDataInfo.length - 1][2] / 2);
    //let withQuery = `with wavelet_table as(select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and  i>=${firstT} and i<=${lastT}) `
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from wavelet_table where l=${curL} and `;
    let sqlStr = `select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and (`;
    //console.log(sqlStr)
    // for (let i = 0; i < losedDataInfo.length; i++) {
    //     losedDataInfo[i][1] =losedDataInfo[i][1];
    //     losedDataInfo[i][2] =losedDataInfo[i][2];
    // }
    for (let i = 0; i < losedDataInfo.length-1 ; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if(first!==second){
            for(let j=first;j<=second;j++){
                sqlStr += `i=${j} or `
            }
            //sqlStr += `(i>=${first} and i<=${second}) or `
        }else{
            sqlStr += `i=${first} or `
        }
            
    }
    return sqlStr.substring(0, sqlStr.length - 3) + ") "
}
function generateSingalLevelSQLWithSubQuery21(losedDataInfo, maxLevel, tableName) {

    const curL = maxLevel - losedDataInfo[0][0];
    if (curL < 0) {
        return null;
    }
    const firstT = Math.floor(losedDataInfo[0][1] / 2);
    const lastT = Math.floor(losedDataInfo[losedDataInfo.length - 1][2] / 2);
    //let withQuery = `with wavelet_table as(select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and  i>=${firstT} and i<=${lastT}) `
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from wavelet_table where l=${curL} and `;
    let sqlStr = `select l,i,minid,minvd,maxvd,maxid from ${tableName} where l=${curL} and i in (`;
    //console.log(sqlStr)
    // for (let i = 0; i < losedDataInfo.length; i++) {
    //     losedDataInfo[i][1] =losedDataInfo[i][1];
    //     losedDataInfo[i][2] =losedDataInfo[i][2];
    // }
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}
function generateSingalLevelSQLWithSubQuery21RawMinMax(losedDataInfo, maxLevel, tableName) {

    const curL = maxLevel - losedDataInfo[0][0]-1;
    if (curL < 0) {
        return null;
    }
  
  
    let sqlStr = `select l,i,mint,minv,maxv,maxt from m4waveletrawminmaxdata.${tableName} where l=${curL} and i in (`;
    for(let i=0;i<losedDataInfo.length;i++){
        losedDataInfo[i][1]=losedDataInfo[i][1]*2;
        losedDataInfo[i][2]=losedDataInfo[i][2]*2+1
    }
  
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}

function generateSingalLevelSQLMinMaxMissQuery(losedDataInfo, maxLevel, tableName){

    const curL = losedDataInfo[0][0];
    let sqlStr = `select i,minvd,maxvd from ${tableName} where  i in (`;
    
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${2**curL+first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${2**curL+j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}

function generateSingalLevelSQLWithSubQueryMinMax(losedDataInfo, maxLevel, tableName) {
    const curL = maxLevel - losedDataInfo[0][0];
    if (curL < 1) {
        return null;
    }
    let sqlStr = `select l,i,minvd,maxvd from ${tableName} where l=${curL} and i in (`;
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}

function generateSingalLevelSQLWithSubQuery1(losedDataInfo, maxLevel, tableName) {
    if (losedDataInfo.length <= 0) {
        return null
    }
    const curL = maxLevel - losedDataInfo[0][0];
    if (curL < 1) {
        return null;
    }
    // const firstT = Math.floor(losedDataInfo[0][1] / 2);
    // const lastT = Math.floor(losedDataInfo[losedDataInfo.length - 1][2] / 2);
    //let withQuery = `with wavelet_table as(select * from public.stockwavelettable where l=${curL} and  i>=${firstT} and i<=${lastT}) `
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from wavelet_table where l=${curL} and `;
    let sqlStr = `select * from  ${tableName} where l=${curL} and i in (`;
    //console.log(sqlStr)
    // for (let i = 0; i < losedDataInfo.length; i++) {
    //     losedDataInfo[i][1] = losedDataInfo[i][1]);
    //     losedDataInfo[i][2] = losedDataInfo[i][2];
    // }
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }

    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${j},`
            }
        }
    }

    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}
function generateSingalLevelSQLWithSubQuery11(losedDataInfo, maxLevel, tableName) {

    const curL = maxLevel - losedDataInfo[0][0] - 1;
    console.log(maxLevel, curL, "fjd")
    if (curL < 1) {
        return null;
    }
    const firstT = Math.floor(losedDataInfo[0][1] / 2);
    const lastT = Math.floor(losedDataInfo[losedDataInfo.length - 1][2] / 2);
    //let withQuery = `with wavelet_table as(select * from public.stockwavelettable where l=${curL} and  i>=${firstT} and i<=${lastT}) `
    //let sqlStr = `select l,i,minid,minvd,maxvd,maxid from wavelet_table where l=${curL} and `;
    let sqlStr = `select * from  ${tableName} where l=${curL} and i in (`;
    //console.log(sqlStr)
    for (let i = 0; i < losedDataInfo.length; i++) {
        losedDataInfo[i][1] = Math.floor(losedDataInfo[i][1] / 2);
        losedDataInfo[i][2] = Math.floor(losedDataInfo[i][2] / 2);
    }
    for (let i = 0; i < losedDataInfo.length - 1; i++) {
        if (losedDataInfo[i][2] === losedDataInfo[i + 1][1] || (losedDataInfo[i][2] + 1) === losedDataInfo[i + 1][1]) {
            losedDataInfo[i][2] = losedDataInfo[i + 1][2];
            losedDataInfo.splice(i + 1, 1)
            i--;
        }
    }
    for (let i = 0; i < losedDataInfo.length; i++) {
        const first = losedDataInfo[i][1];
        const second = losedDataInfo[i][2];
        if (first === second) {
            sqlStr += `${first},`
        } else {
            for (let j = first; j <= second; j++) {
                sqlStr += `${j},`
            }
        }
    }
    return sqlStr.substring(0, sqlStr.length - 1) + ")"
}



module.exports = {generateSingalLevelSQLWithSubQueryMinMaxMiss,generateSingalLevelSQLWithSubQuery21RawMinMax,generateSingalLevelSQLWithSubQueryRawMinMax,generateSingalLevelSQLMinMaxMissQuery, generateAllLevelSQLQuery, generateSingalLevelSQLWithSubQuery1, generateSingalLevelSQLWithSubQuery11, generateMultiLevelSQL, generateSingalLevelSQL, generateSingalLevelSQLWithSubQuery, generateSingalLevelSQLWithSubQuery21, generateAllLevelParallelSQLQuery, generateSingalLevelSQLWithSubQueryMinMax ,generateSingalLevelSQLWithSubQuery21test}