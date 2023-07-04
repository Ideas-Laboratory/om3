import store, { GlobalState, LineChartObj, emitter, SimpleLineChartObj, WaveletLineChartObj, SimpleBrushChartObj, TrendQueryLineChartObj, TimeBoxQueryLineChartObj, ViewChangeLineChartObj, AngularQueryLineChartObj, MultiTimeSeriesObj, MultiTimeSeriesM4Obj, MultiHaarTimeSeriesObj } from ".";
import { v4 as uuidv4 } from 'uuid';
import * as d3 from 'd3';

import { constructTrendTree } from '../helper/wavlet-decoder'
import { getLevelData } from "@/helper/util";
import { formatToRenderDataForTrend, formatRenderDataForViewChange, formatNonPowDataForViewChange, formatDataForMultiM4 } from "@/helper/format-data";
import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import LevelDataManager from "@/model/level-data-manager";
import TrendTree from "@/helper/tend-query-tree";
import { setFlagMap } from "@/global_state/state";


function formatData(data: Array<any>, params: { startTime: string, endTime: string, width: number, height: number, algroithm: string }) {
    const tStart = new Date(params.startTime).getTime();
    const tEnd = new Date(params.endTime).getTime();
    const timeRange = tEnd - tStart;

    const vMin = d3.min(data, d => d.v);
    const vMax = d3.max(data, d => d.v);
    //const sortedArray=data;

    const sortedArray = data.sort((a, b) => {
        return new Date(a.t).getTime() - new Date(b.t).getTime();
    });

    sortedArray.forEach(item => {
        item.x = Math.round(params.width * (new Date(item.t).getTime() - tStart) / timeRange),
            item.y = Math.round(params.height * (item.v - vMin) / (vMax - vMin))
    });
    let resultArray = sortedArray;
    if (params.algroithm.toLocaleLowerCase() === 'rdp') {
        const threshold = store.state.rdpThreshold;
        // resultArray = simple.simplify(sortedArray, threshold);

    }
    return {
        val: resultArray, min: vMin, max: vMax
    }
}


function addSimpleBrushLineChartObj(state: GlobalState, info: { data: Array<any>, rowData: Array<any>, url: string, startTime: number, endTime: number, algorithm: string, width: number, height: number }) {
    const sampleBrushLineChartObj: SimpleBrushChartObj = {
        id: uuidv4(),
        width: info.width,
        height: info.height,
        x: Math.random() * 60,
        y: Math.random() * 60,
        data: { rowData: [[0, 0], ...info.rowData], val: info.data, min: d3.min(info.data, d => d.v), max: d3.max(info.data, d => d.v) },
        timeRange: [info.startTime, info.endTime],
        algorithm: info.algorithm,
        isSample: true,
        isChoosed: false,
    }
    //console.log(sampleBrushLineChartObj);
    state.simpleBrushLineChartObjs.set(sampleBrushLineChartObj.id, sampleBrushLineChartObj);
    emitter.emit("add_brush_line_chart_obj", sampleBrushLineChartObj);
}

function updateBrushLineChartObj(state: GlobalState, info: SimpleBrushChartObj) {
    state.simpleBrushLineChartObjs.set(info.id, info);
}



function addViewChangeQueryNoPowLineChartObj(state: GlobalState, info: { dataManager: LevelDataManager, trendTree: TrendTree, data: Array<NoUniformColObj>, url: string, startTime: number, endTime: number, algorithm: string, width: number, height: number }) {
    const nonUniformRenderData = formatNonPowDataForViewChange(info.data, info.width, 2 ** info.dataManager.maxLevel, null);
    let maxV = nonUniformRenderData[0].v;
    let minV = nonUniformRenderData[0].v
    nonUniformRenderData.forEach((v) => {
        //@ts-ignore
        maxV = Math.max((v.v === Infinity) ? maxV : v.v, maxV!);
        //@ts-ignore
        minV = Math.min((v.v === -Infinity) ? minV : v.v, minV!);
    });
    const viewChangeQueryObj: ViewChangeLineChartObj = {
        id: uuidv4(),
        width: info.width,
        height: info.height,
        x: Math.random() * 60,
        y: Math.random() * 60,
        root: info.trendTree,
        data: { powRenderData: [], noPowRenderData: nonUniformRenderData, minv: minV!, maxv: maxV! },
        timeRange: [0, info.width - 1],
        algorithm: info.algorithm,
        dataManager: info.dataManager,
        params: [0, 0],
        historyQueryStack: [[info.dataManager.levelIndexObjs.length - 1, 0, info.width - 1]],
        currentLevel: Math.ceil(Math.log2(info.width)),
        isPow: false,
        nonUniformColObjs: info.data,
        maxLen:0,
        startTime:0,
        endTime:0
    }
    emitter.emit("add_view_change_query_obj", viewChangeQueryObj);
}

function addTimeBoxQueryLineChartObjs(state: GlobalState, info: { data: Array<any>, url: string, startTime: number, endTime: number, algorithm: string, width: number, height: number }) {
    const data = [];
    const dataManagers = [];
    for (let i = 0; i < info.data.length; i++) {
        const { trendTree, dataManager } = constructTrendTree(info.data[i]);
        const { renderData, minv, maxv } = formatToRenderDataForTrend([dataManager.maxLevel, Math.floor(Math.log2(info.width))], getLevelData(dataManager.levelIndexObjs[dataManager.levelIndexObjs.length - 1].firstNodes[0]), info.width);
        data.push({ renderData, minv, maxv });
        dataManagers.push(dataManager);
    }
    const timeBoxQueryObj: TimeBoxQueryLineChartObj = {
        id: uuidv4(),
        width: info.width,
        height: info.height,
        x: Math.random() * 60,
        y: Math.random() * 60,
        data,
        timeRange: [0, info.width - 1],
        algorithm: info.algorithm,
        dataManagers,
        params: [0, 0],
        historyQueryStack: [[dataManagers[0].levelIndexObjs.length - 1, 0, info.width - 1]],
        currentLevel: dataManagers[0].levelIndexObjs.length
    }
    if (data.length > 0) {
        emitter.emit("add_time_box_query_obj", timeBoxQueryObj);
    } else {
        throw new Error("cannot load time box data from server");
    }
}

function addMultiTimeSeriesObj(state: GlobalState, info: {
    className: string,
    lineAmount: number,
    startTimeStamp: number,
    endTimeStamp: number,
    timeIntervalMs: number, dataManagers: Array<LevelDataManager>, powRenderData: Array<{ renderData: Array<any>, minv: number, maxv: number }>, columnInfos: Array<Array<NoUniformColObj>>, url: string, startTime: number, endTime: number, algorithm: string, width: number, height: number, pow: boolean, minv: number, maxv: number, maxLevel: number
}) {
    const multiTImeSeriesObj: MultiTimeSeriesObj = {
        id: uuidv4(),
        width: info.width,
        height: info.height,
        x: Math.random() * 60,
        y: Math.random() * 60,
        powRenderData: info.powRenderData,
        columnInfos: info.columnInfos,
        timeRange: [0, info.endTime],
        algorithm: info.algorithm,
        dataManagers: info.dataManagers,
        params: [0, 0],
        currentLevel: Math.ceil(Math.log2(info.width)),
        pow: info.pow,
        minv: info.minv,
        maxv: info.maxv,
        maxLevel: info.maxLevel,
        className: info.className,
        lineAmount: info.lineAmount,
        startTimeStamp: info.startTimeStamp,
        endTimeStamp: info.endTimeStamp,
        timeIntervalMs: info.timeIntervalMs
    }
    if (info.dataManagers.length > 0) {
        emitter.emit("add_multi_timeseries_obj", multiTImeSeriesObj);
    } else {
        throw new Error("cannot load time box data from server");
    }
}


function updateMultiTimeSeriesM4Obj(state: GlobalState, info: { width: number, height: number, data: Array<{ tn: string, res: Array<{ t: number, v: number }> }>, maxLevel: number, type: string, timeRange: [number, number] }) {
    const allRenderData = [];
    let maxV = -Infinity;
    let minV = Infinity;
    for (let i = 0; i < info.data.length; i++) {
        const { res, minv, maxv } = formatDataForMultiM4(info.data[i].res, info.width, 0, 2 ** info.maxLevel - 1);
        allRenderData.push({ tn: info.data[i].tn, data: res });
        maxV = Math.max(maxv, maxV);
        minV = Math.min(minv, minV);
    }
    const multiTImeSeriesObj: MultiTimeSeriesM4Obj = {
        id: uuidv4(),
        width: info.width,
        height: info.height,
        x: Math.random() * 60,
        y: Math.random() * 60,
        timeRange: info.timeRange,
        algorithm: "multiTimeSeriesM4",
        renderData: allRenderData,
        params: [0, 0],
        minv: minV,
        maxv: maxV,
        maxLevel: info.maxLevel,
    }
    if (allRenderData.length > 0) {
        emitter.emit("update_multi_timeseries_m4_obj", multiTImeSeriesObj);
    } else {
        throw new Error("cannot get m4 time series data");
    }
}

function alterAlgorithm(state: GlobalState, algoritem: string) {
    state.currentAlgorithm = algoritem;
}

function alterSampleMethod(state: GlobalState, method: string) {
    state.controlParams.currentSampleMethod = method;
}



function alterMode(state: GlobalState, mode: 'Default' | 'Custom') {
    state.controlParams.currentMode = mode
}

function alterTable(state: GlobalState, table: string) {
    //@ts-ignore
    state.controlParams.currentTable = table._value;

    //@ts-ignore
    const splitArray = table._value.split("_")
    state.controlParams.tableMaxLevel = store.state.tableMaxLevels[splitArray[splitArray.length - 1]];
}

function alterCustomTable(state: GlobalState, table: string){

    state.controlParams.currentTable = table;
    const splitArray = table.split("_")
    state.controlParams.tableMaxLevel = store.state.tableMaxLevels[splitArray[splitArray.length - 1]];
}

function alterLineType(state: GlobalState, lineType: 'Single' | 'Multi') {
    state.controlParams.currentLineType = lineType;
}





function updateDisplayChanel(state: GlobalState, chanel: string) {
    state.controlParams.currentChanel = chanel;
}
function updateDenoiseMethod(state: GlobalState, method: string) {
    state.controlParams.currentDenoiseMethod = method;
}

function updateAllTables(state: GlobalState, info: { tables: Array<string> }) {
    state.allTables = info.tables;
    if (info.tables.length > 0) {
        state.controlParams.currentTable = info.tables[0];
    }
}

function updateAllFlags(state: GlobalState, info: { flags: any }) {
    // state.allFlags = info.flags;
    setFlagMap(info.flags)
    console.log("load flags finish");
}

function updateMultiLineClassInfo(state: GlobalState, info: { info: any }) {
    const multiClassInfoMap = new Map<string, any>();
    for (let i = 0; i < info.info['data'].length; i++) {
        multiClassInfoMap.set(info.info['data'][i]['name'], info.info['data'][i]);
    }
    if(state.controlParams.currentMode==='Default'){
        state.allMultiLineClassInfoMap = multiClassInfoMap;
    }else{
        state.allCustomMultiLineClassInfoMap=multiClassInfoMap;
    }
    
}

function alterCurrentMulitLineClass(state: GlobalState, className: string) {
    state.controlParams.currentMultiLineClass = className
    //console.log("load flags finish");
}

function updateCustomTableAndInfo(state: GlobalState, info: { customTables: Array<string>,customTableInfo:Array<any> }){
    const tableInfoMap=new Map<string,any>()
    for(let i=0;i<info.customTableInfo.length;i++){
        tableInfoMap.set(info.customTableInfo[i]['name'],info.customTableInfo[i]);
    }
    state.customTableMap=tableInfoMap;
    state.allCustomTables=info.customTables.map(v=>v.split(".")[1]);
}

function updateDefaultTableAndInfo(state: GlobalState, info: { tables: Array<string>,tableInfo:Array<any> }){
    const tableInfoMap=new Map<string,any>()
    for(let i=0;i<info.tableInfo.length;i++){
        tableInfoMap.set(info.tableInfo[i]['name'],info.tableInfo[i]);
    }
    state.defaultTableMap=tableInfoMap;
    state.allDefaultTables=info.tables.map(v=>v.split(".")[1]);
    if(state.allDefaultTables.length>0){
        state.controlParams.currentTable=state.allDefaultTables[0];
    }
    
}

function alterProgressive(state: GlobalState, progresive: boolean){
    state.controlParams.progressive=progresive;
}

export {
    alterAlgorithm,
    alterMode,
    alterLineType,
    alterSampleMethod,
    alterTable,
    updateDisplayChanel,
    updateDenoiseMethod,
    addSimpleBrushLineChartObj,
    updateBrushLineChartObj,
    addTimeBoxQueryLineChartObjs,
    updateAllTables,
    addViewChangeQueryNoPowLineChartObj,
    addMultiTimeSeriesObj,
    updateMultiTimeSeriesM4Obj,
    updateAllFlags,
    updateMultiLineClassInfo,
    alterCurrentMulitLineClass,
    alterCustomTable,
    updateCustomTableAndInfo,
    updateDefaultTableAndInfo,
    alterProgressive,
}

