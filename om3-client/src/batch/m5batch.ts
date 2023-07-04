
import TrendTree from "@/helper/tend-query-tree";
import LevelDataManager from "@/model/level-data-manager";
import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import store, { MultiTimeSeriesObj } from "../store";
import { formatToRenderDataForTrend, getIndexTime } from "../helper/format-data"
import { computeLosedDataRange, canCut, getLevelData, computeTimeFilterBaseLevelInfo, computeColTimeRange, computeLosedDataRangeV1 } from "@/helper/util";
import getAllTableName, { getMockTableName } from "../constant"
import axios from "axios";
import { batchLoadDataForRangeLevelForMinMaxMiss } from "@/api/build_tree";

type AllInteraction = "pan" | "zoom_in" | "zoom_out" | "resize" | "time";

function batchComputeTimeSE(currentLevel: number, width: number, timeRange: Array<number>, dataNum: number, dataManagers: Array<LevelDataManager>) {
    //const startT=new Date().getTime()
    const allColumnInfos = new Array<Array<NoUniformColObj>>();
    for (let j = 0; j < dataManagers.length; j++) {

        allColumnInfos[j] = [];
        dataManagers[j].columnInfos=allColumnInfos[j];
    }
    for (let i = 0; i < width; i++) {
        for (let j = 0; j < dataManagers.length; j++) {
            allColumnInfos[j].push(new NoUniformColObj(i, timeRange[0], timeRange[1], currentLevel, width, dataNum, dataManagers[0].maxLevel,dataManagers[j].dataName));
        }
    }
    const tR = timeRange[1] - timeRange[0] + 1;
    const tS = timeRange[0];
    for (let i = timeRange[0] + 1; i <= timeRange[1]; i++) {
        const pre = Math.floor(width * ((i - 1) - tS) / tR);
        const next = Math.floor(width * (i - tS) / tR);
        if (pre !== next) {
            for (let j = 0; j < dataManagers.length; j++) {
                allColumnInfos[j][pre].setTEnd(i - 1);
                allColumnInfos[j][next].setTStart(i);
            }
        }
    }
    //console.log(allColumnInfos)
    // console.log("compute time:",new Date().getTime());
    return allColumnInfos;
}

function rebuildColObj(currentLevel: number, width: number, timeRange: Array<number>, dataNum: number, dataManagers: Array<LevelDataManager>, allColumnInfos: Array<Array<NoUniformColObj>>) {
    for (let i = 0; i < width; i++) {
        for (let j = 0; j < dataManagers.length; j++) {
            allColumnInfos[j][i].rebuild(i, timeRange[0], timeRange[1], currentLevel, width, dataNum, dataManagers[0].maxLevel,dataManagers[j].dataName);
        }
    }
    const tR = timeRange[1] - timeRange[0] + 1;
    const tS = timeRange[0];
    for (let i = timeRange[0] + 1; i <= timeRange[1]; i++) {
        const pre = Math.floor(width * ((i - 1) - tS) / tR);
        const next = Math.floor(width * (i - tS) / tR);
        if (pre !== next) {
            for (let j = 0; j < dataManagers.length; j++) {
                allColumnInfos[j][pre].setTEnd(i - 1);
                allColumnInfos[j][next].setTStart(i);
            }
        }
    }
    // console.log("compute time:",new Date().getTime());
    return allColumnInfos;
}



export async function batchViewChange(multiTimeSeriesObj: MultiTimeSeriesObj, params: { inter: AllInteraction }) {
    const dataManagers = multiTimeSeriesObj.dataManagers;
    const managerMap = new Map<string, LevelDataManager>();
    const lineNum = dataManagers.length;
    const width = multiTimeSeriesObj.width;
    const maxLevel = multiTimeSeriesObj.maxLevel;
    const currentLevel = multiTimeSeriesObj.currentLevel;
    const timeRange = multiTimeSeriesObj.timeRange;
    console.log(multiTimeSeriesObj)

    const allPromises=[];
    for(let i=0;i<multiTimeSeriesObj.columnInfos.length;i++){
        const dataManager=multiTimeSeriesObj.dataManagers[i];
        if(!dataManager.isShow){
            continue;
        }
        allPromises.push(new Promise((resolve,rej)=>{
            dataManager.viewChangeInteractionFinal1(10,width,timeRange,null,null).then((nonCol)=>{
                multiTimeSeriesObj.columnInfos[i]=nonCol;
                resolve(null)
            })
        }))
    }
    await Promise.all(allPromises);
    return multiTimeSeriesObj.columnInfos;

    
}

async function batchLoadDataForMultiLine1(allLoedData: Array<{ tn: string, lr: Array<Array<number>> }>, maxLevel: number, dataManagers: Map<string, LevelDataManager>) {
    if (allLoedData.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/multi_series_batch_load_data1`, {
        multi_series_load_data: allLoedData,
    });

    const result = data.data;
    const loseDataMap = new Map<string, Array<Array<number>>>();
    for (let i = 0; i < allLoedData.length; i++) {
        loseDataMap.set(allLoedData[i].tn, allLoedData[i].lr);
    }
    //console.log(result)
    if (result && result.length) {
        for (let i = 0; i < result.length; i++) {
            const currentLineRes = result[i]['d'];
            const resultArray = [];
            for (let j = 0; j < currentLineRes[0].length; j++) {
                resultArray.push({ l: maxLevel - currentLineRes[0][j], i: currentLineRes[1][j], dif: [currentLineRes[2][j], currentLineRes[3][j], currentLineRes[4][j], currentLineRes[5][j]] });
            }
            dataManagers.get(result[i]['tn'])?.constructTreeForBatchLoad(loseDataMap.get(result[i]['tn'])!, resultArray)
        }
    }
    //console.log("line1")
}

async function batchLoadDataForMultiLineV1(allLoedData: Array<{ tn: string, lr: Array<Array<number>> }>, maxLevel: number, dataManagers: Map<string, LevelDataManager>) {
    if (allLoedData.length === 0) {
        return [];
    }

    const { data } = await axios.post(`postgres/line_chart/multi_series_batch_load_data1`, {
        multi_series_load_data: allLoedData,
    });
    const result = data.data;
    let func:any=null;
    if(store.state.controlParams.currentTimeBoxType === "stock"){
        func=getAllTableName;
    }else if(store.state.controlParams.currentTimeBoxType === "mock"){
        func=getMockTableName
    }
    


    const { tableMap } = func();
    for (let i = 0; i < allLoedData.length; i++) {
        const colNum = tableMap.get(allLoedData[i].tn);
        if (colNum === undefined) {
            throw new Error("cannot find table index");
        }
        const minidf = `minid${colNum}`;
        const minvdf = `minvd${colNum}`;
        const maxvdf = `maxvd${colNum}`;
        const maxidf = `maxid${colNum}`;

        const tempRes: Array<{ l: number, i: number, dif: Array<number> }> = [];
        result.forEach((v: any, i: number) => {
            tempRes.push({ l: maxLevel - v['l'], i: v['i'], dif: [v[minidf], v[minvdf], v[maxvdf], v[maxidf]] });
        })
        dataManagers.get(allLoedData[i]['tn'])?.constructTreeForBatchLoad1(allLoedData[i].lr, tempRes)
    }
}

async function batchLoadDataForMultiLine(allLoedData: Array<{ tn: string, lr: Array<Array<number>> }>, maxLevel: number, dataManagers: Map<string, LevelDataManager>) {
    if (allLoedData.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/multi_series_batch_load_data`, {
        multi_series_load_data: allLoedData,
    });
    const result = data.data;
    const { tableMap } = store.state.controlParams.currentTimeBoxType === "stock" ? getAllTableName() : getMockTableName();
    for (let i = 0; i < allLoedData.length; i++) {
        const colNum = tableMap.get(allLoedData[i].tn);
        if (colNum === undefined) {
            throw new Error("cannot find table index");
        }
        const minidf = `minid${colNum}`;
        const minvdf = `minvd${colNum}`;
        const maxvdf = `maxvd${colNum}`;
        const maxidf = `maxid${colNum}`;

        const tempRes: Array<{ l: number, i: number, dif: Array<number> }> = [];

        result.forEach((v: any, i: number) => {
            tempRes.push({ l: maxLevel - v['l'], i: v['i'], dif: [v[minidf], v[minvdf], v[maxvdf], v[maxidf]] });
        })
        dataManagers.get(allLoedData[i]['tn'])?.constructTreeForBatchLoad1(allLoedData[i].lr, tempRes)
    }
}

export async function batchGetData(dataManagers: Array<LevelDataManager>, level: number, start: number, end: number, maxLevel: number, width: number, params: { inter: AllInteraction, noRet: boolean }) {
    
    if (level > maxLevel) {
        return []
    }
    const allPromises=[];
    if (params.inter !== "zoom_out") {
        for(let i=0;i<dataManagers.length;i++){
            allPromises.push(new Promise((resolve,reject)=>{
                dataManagers[i].getDataMinMaxMiss(level,start,end,true).then(()=>{
                    resolve(null);
                })
            }))
        }
    }
    const renderDatas = [];
    if (!params.noRet) {
        for (let i = 0; i < dataManagers.length; i++) {
            const data = dataManagers[i].levelIndexObjs[level].getDataByIndex(start, end);
            data.start = getIndexTime(level, data.start, maxLevel).startT;
            data.end = getIndexTime(level, data.end, maxLevel).endT;
            const { renderData, minv, maxv } = formatToRenderDataForTrend([maxLevel, level], data, width);

            renderDatas.push({ renderData, minv, maxv });
        }

    }

    return await Promise.all(allPromises);
}

export async function viewChangeInteraction(multiTimeSeriesObj: MultiTimeSeriesObj, callback: any) {
    const allPromises = [];
    const allColumnInfos = new Array(multiTimeSeriesObj.dataManagers.length);
    for (let i = 0; i < multiTimeSeriesObj.dataManagers.length; i++) {
        allPromises.push(new Promise((resolve, reject) => {
            multiTimeSeriesObj.dataManagers[i].viewChangeInteraction(multiTimeSeriesObj.currentLevel, multiTimeSeriesObj.width, multiTimeSeriesObj.timeRange, null).then(uniformOobj => {
                allColumnInfos[i] = uniformOobj;
                resolve(null);
            })
        }))
    }
    Promise.all(allPromises).then((res) => {
        multiTimeSeriesObj.columnInfos = allColumnInfos;
        callback()
    })
}

export async function loadData(multiTimeSeriesObj: MultiTimeSeriesObj, level: number, start: number, end: number, width: number, callback: any) {
    const allPromises = [];
    const allColumnInfos = new Array(multiTimeSeriesObj.dataManagers.length);
    allPromises.push(new Promise((resolve, reject) => {
        multiTimeSeriesObj.dataManagers.forEach((manager, i) => {
            manager.getData(level, start, end, true).then(() => {
                manager.viewChangeInteraction(level, width, multiTimeSeriesObj.timeRange, null).then((uniformOobj) => {
                    allColumnInfos[i] = uniformOobj;
                    resolve(null);
                }

                )
            })
        })
    }))
    Promise.all(allPromises).then(() => {
        multiTimeSeriesObj.columnInfos = allColumnInfos;
        callback()
    })


}



export async function zoomInIneraction(multiTimeSeriesObj: MultiTimeSeriesObj, callback: any) {
    const allPromises = [];
    const allColumnInfos = new Array(multiTimeSeriesObj.dataManagers.length);
    const baseInfo = computeTimeFilterBaseLevelInfo(multiTimeSeriesObj.timeRange, multiTimeSeriesObj.width, multiTimeSeriesObj.maxLevel);
    const computeColInfoT = computeColTimeRange(multiTimeSeriesObj.currentLevel, multiTimeSeriesObj.width, multiTimeSeriesObj.timeRange, 2 ** multiTimeSeriesObj.maxLevel, multiTimeSeriesObj.maxLevel)
    multiTimeSeriesObj.currentLevel = baseInfo[0];
    for (let i = 0; i < multiTimeSeriesObj.dataManagers.length; i++) {
        allPromises.push(new Promise((resolve, reject) => {
            multiTimeSeriesObj.dataManagers[i].getData(baseInfo[0], baseInfo[1], baseInfo[2], true).then(() => {
               console.log("data")
            })

        }))
    }
    Promise.all(allPromises).then((res) => {
        multiTimeSeriesObj.columnInfos = allColumnInfos;
        //multiTimeSeriesObj.currentLevel=baseInfo[0];
        callback()
    })
}

export async function panInIneraction(multiTimeSeriesObj: MultiTimeSeriesObj, callback: any) {
    const allPromises = [];
    const allColumnInfos = new Array(multiTimeSeriesObj.dataManagers.length);
    const baseInfo = computeTimeFilterBaseLevelInfo(multiTimeSeriesObj.timeRange, multiTimeSeriesObj.width, multiTimeSeriesObj.maxLevel);
    const computeColInfoT = computeColTimeRange(multiTimeSeriesObj.currentLevel, multiTimeSeriesObj.width, multiTimeSeriesObj.timeRange, 2 ** multiTimeSeriesObj.maxLevel, multiTimeSeriesObj.maxLevel)
    for (let i = 0; i < multiTimeSeriesObj.dataManagers.length; i++) {
        allPromises.push(new Promise((resolve, reject) => {
            multiTimeSeriesObj.dataManagers[i].getData(baseInfo[0], baseInfo[1], baseInfo[2], true).then(() => {
                console.log("getdata")
            })

        }))
    }
    Promise.all(allPromises).then((res) => {
        multiTimeSeriesObj.columnInfos = allColumnInfos;
        multiTimeSeriesObj.currentLevel = baseInfo[0];
        callback()
    })
}