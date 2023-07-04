import store, { emitter, getAvgTime, GlobalState, MultiTimeSeriesObj, ViewChangeLineChartObj, ws } from ".";
import { Commit, ActionContext, ActionHandler } from 'vuex'
import axios from "axios";
import { constructMinMaxMissTrendTree, constructTrendTree } from '../helper/wavlet-decoder';
import { v4 as uuidv4 } from 'uuid';
import * as d3 from "d3";
import LevelDataManager from "@/model/level-data-manager";
import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import { formatToRenderDataForTrend, getGlobalMinMaxInfo } from "@/helper/format-data";
import md5 from "md5"
import { arrayBufferToBase64, base64ToArrayBuffer, getLevelData, openLoading } from "@/helper/util";
import { ElLoading } from 'element-plus'
import { drawViewChangeLineChart } from "@/application/line-interaction";
import { indexGetData, indexPutData, initIndexDB } from "@/indexdb";


async function get(state: GlobalState, url: string) {

    url = 'postgres' + url;

    //const loading = openLoading();
    const { data } = await axios.get(url);
    //loading.close();
    return data;
}



async function getBuffer(state: GlobalState, url: string) {

    url = 'postgres' + url;
    // localStorage.removeItem(url)
    try {
        const timeGetCache = new Date().getTime()
        const cacheFlag = await indexGetData(url)

        if (cacheFlag && cacheFlag !== '' && cacheFlag !== undefined && cacheFlag !== null) {
            //@ts-ignore
            const flagBuffer = base64ToArrayBuffer(cacheFlag!);
            console.log(url, "use flag cache:", flagBuffer.byteLength);
            return flagBuffer
        }

    } catch (err) {
        console.error(err)
    }


    //const loading = openLoading();
    const { data } = await axios.get(url, { responseType: 'arraybuffer' });
    if (data) {
        indexPutData(url, arrayBufferToBase64(data));
        console.log(url, " store in indexdb")
    }
    // loading.close();
    return data;
}



const loadViewChangeQueryWSMinMaxMissDataInitData: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { startTime: number, endTime: number, width: number, height: number }) => {
    let maxLevel = 0
    const currentTable = context.state.controlParams.currentTable;
    let lineInfo: any = null
    if (context.state.controlParams.currentMode === 'Default') {
        lineInfo = context.state.defaultTableMap.get(currentTable);
    } else {
        lineInfo = context.state.customTableMap.get(currentTable);
    }

    if (lineInfo === undefined) {
        throw new Error("cannot get class info");
    }
    maxLevel = lineInfo['level'];
    const startTimeStamp = new Date(lineInfo.start_time).getTime();
    let endTimeStamp = 0
    if (lineInfo.end_time !== '') {
        endTimeStamp = new Date(lineInfo.end_time).getTime();
    }
    let timeInterval = 0;
    if (lineInfo.interval !== 0) {
        timeInterval = lineInfo.interval;
    }
    //@ts-ignore
    const combinedUrl = `/line_chart/init_wavelet_bench_min_max_miss?width=${2 ** (Math.ceil(Math.log2(payload.width)))}&table_name=${context.state.controlParams.currentTable}&mode=${context.state.controlParams.currentMode}`;
    const data = get(context.state, combinedUrl);
    data.then(tempRes => {
        if (tempRes['code'] === 200) {
            const finalRes = tempRes['data']['result'];
            const currentLevel = Math.ceil(Math.log2(payload.width));

            const { trendTree, dataManager } = constructMinMaxMissTrendTree(finalRes, payload.width);
            dataManager.maxLevel = maxLevel
            dataManager.realDataRowNum = lineInfo['max_len']
            const { minv, maxv } = getGlobalMinMaxInfo(getLevelData(dataManager.levelIndexObjs[dataManager.levelIndexObjs.length - 1].firstNodes[0]));

            const viewChangeQueryObj: ViewChangeLineChartObj = {
                id: uuidv4(),
                width: payload.width,
                height: payload.height,
                x: Math.random() * 60,
                y: Math.random() * 60,
                root: trendTree,
                data: { powRenderData: [], noPowRenderData: [], minv: minv!, maxv: maxv! },
                timeRange: [0, lineInfo['max_len']],
                startTime: startTimeStamp,
                endTime: endTimeStamp,
                algorithm: "",
                dataManager: dataManager,
                params: [0, 0],
                currentLevel: Math.ceil(Math.log2(payload.width)),
                isPow: false,
                nonUniformColObjs: [],
                maxLen: lineInfo['max_len']
            }
            const drawer = drawViewChangeLineChart(viewChangeQueryObj)
            dataManager.getDataMinMaxMiss(currentLevel + 1, 0, 2 ** (currentLevel + 1) - 1).then(() => {
                const minV = dataManager.levelIndexObjs[0].firstNodes[0].yArray[1];
                const maxV = dataManager.levelIndexObjs[0].firstNodes[0].yArray[2];
                const yScale = d3.scaleLinear().domain([minV, maxV]).range([payload.height, 0]);

                dataManager.viewChangeInteractionFinal1(Math.ceil(Math.log2(payload.width)), payload.width, [0, lineInfo['max_len'] - 1], yScale, drawer).then(res => {
                    drawer(res)
                    //context.commit("addViewChangeQueryNoPowLineChartObj", { trendTree, dataManager, data: res, startTime: payload.startTime, endTime: payload.endTime, algorithm: "trendtree", width: payload.width, height: payload.height });
                });
            });

        } else {
            console.error(tempRes['msg'])
        }

    });
}

const loadMultiTimeSeriesInitData: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { width: number, height: number, type: string }) => {
    const currentLevel = Math.ceil(Math.log2(payload.width));
    let maxLevel = 0
    const currentMulitLineClass = context.state.controlParams.currentMultiLineClass;
    let lineClassInfo: any = null
    if (context.state.controlParams.currentMode === 'Default') {
        lineClassInfo = context.state.allMultiLineClassInfoMap.get(currentMulitLineClass);
    } else {
        lineClassInfo = context.state.allCustomMultiLineClassInfoMap.get(currentMulitLineClass);
    }

    if (lineClassInfo === undefined) {
        throw new Error("cannot get class info");
    }
    maxLevel = lineClassInfo['level'];
    const combinedUrl = `/line_chart/init_multi_timeseries?width=${2 ** currentLevel}&class_name=${currentMulitLineClass}&mode=${context.state.controlParams.currentMode}`;
    const data = get(context.state, combinedUrl);

    data.then(res => {
        let dataManagers: Array<LevelDataManager> = [];
        let globalMaxV = -Infinity;
        let globalMinV = Infinity;
        for (let i = 0; i < res.length; i++) {
            const { dataManager } = constructMinMaxMissTrendTree(res[i].d, payload.width, res[i].tn);

            dataManager.maxLevel = maxLevel;
            dataManager.realDataRowNum = lineClassInfo['max_len'];

            const { minv, maxv } = getGlobalMinMaxInfo(getLevelData(dataManager.levelIndexObjs[dataManager.levelIndexObjs.length - 1].firstNodes[0]));
            globalMaxV = Math.max(maxv!, globalMaxV);
            globalMinV = Math.min(minv!, globalMinV);
            dataManager.md5Num = parseInt("0x" + md5(dataManager.dataName).slice(0, 8))
            dataManagers.push(dataManager);
        }

        dataManagers = dataManagers.sort((a, b) => {
            return parseInt("0x" + md5(a.dataName).slice(0, 8)) - parseInt("0x" + md5(b.dataName).slice(0, 8))
        })

        const allPromises = [];
        const columnsInfoArray: Array<Array<NoUniformColObj>> = new Array(dataManagers.length);
        for (let i = 0; i < dataManagers.length; i++) {
            allPromises.push(new Promise((resolve, reject) => {
                dataManagers[i].viewChangeInteractionFinal1(currentLevel, payload.width, [0, dataManagers[i].realDataRowNum - 1], null, null).then((noUniformColObjs: Array<NoUniformColObj>) => {
                    columnsInfoArray[i] = noUniformColObjs;
                    dataManagers[i].columnInfos = noUniformColObjs
                    resolve(null);
                }).catch((error) => {
                    reject(error);
                });
            }));
        }
        Promise.all(allPromises).then(() => {
            const startTimeStamp = new Date(lineClassInfo.start_time).getTime();
            let endTimeStamp = 0
            if (lineClassInfo.end_time !== '') {
                endTimeStamp = new Date(lineClassInfo.end_time).getTime();
            }
            let timeInterval = 0;
            if (lineClassInfo.interval !== 0) {
                timeInterval = lineClassInfo.interval;
            }

            context.commit("addMultiTimeSeriesObj", {
                className: lineClassInfo.name,
                lineAmount: lineClassInfo.amount,
                startTimeStamp: startTimeStamp,
                endTimeStamp: endTimeStamp,
                timeIntervalMs: timeInterval, dataManagers: dataManagers, columnInfos: columnsInfoArray, startTime: 0, endTime: dataManagers[0].realDataRowNum - 1, algorithm: "multitimeseries", width: payload.width, height: payload.height, pow: false, minv: globalMinV, maxv: globalMaxV, maxLevel
            })
        }).catch(error => {
            throw error
        });

    });
}

const getAllTables: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllTables`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        const allTables = res.map((v: any) => v['table_fullname'].split(".")[1]);
        context.commit("updateAllTables", { tables: allTables });
    })
}



async function getAllFlagsFunc(context: ActionContext<GlobalState, GlobalState>, lineType: string, isLoading: boolean) {

    const combinedUrl1 = `/line_chart/getAllFlagNames?line_type=${lineType}`;
    const allFlagNames = await get(context.state, combinedUrl1);
    const flagMap: any = {}

    let loading = null;
    if (isLoading) {
        loading = openLoading("Loading Order Coefficients. First load may take a long time, Please Wait!")
    }
    for (let i = 0; i < allFlagNames['data'].length; i++) {
        const combinedUrl2 = `/line_chart/getSingleFlag?name=${allFlagNames['data'][i]}&line_type=${lineType}`
        const tempFlagInfo = await getBuffer(context.state, combinedUrl2);

        //@ts-ignore
        flagMap[allFlagNames['data'][i].split(".")[0]] = Buffer.from(tempFlagInfo)
    }
    if (loading) {
        loading.close()
    }

    return flagMap;
}

const getAllFlags: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    initIndexDB().then(() => {
        console.log("indexdb init success");
    }).catch(() => {
        console.error("indexdb init error");
    })
    const lineType = store.state.controlParams.currentLineType
    getAllFlagsFunc(context, lineType, true).then(res => {
        context.commit("updateAllFlags", { flags: res });
        getAllFlagsFunc(context, "Multi", false).then(res => {
            context.commit("updateAllFlags", { flags: res });
        })
    })


}




const getAllMultiLineClassInfo: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllMultiLineClassInfo?mode=${store.state.controlParams.currentMode}`;
    const data = get(context.state, combinedUrl);
    data.then(res => {
        context.commit("updateMultiLineClassInfo", { info: res });
    });
}


const testCustomDBConn: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return axios.post("postgres/line_chart/testDBConnection", {
        host_name: payload.hostName,
        user_name: payload.userName,
        password: payload.possword,
        db_name: payload.dbName,
    })
}

const createCustomDBConn: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return axios.post("postgres/line_chart/createCustomDBConn", {
        host_name: payload.hostName,
        user_name: payload.userName,
        password: payload.possword,
        db_name: payload.dbName,
    })
}
const initOM3DB: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return get(context.state, "/line_chart/initOM3DBEnv")
}
const clearOM3Table: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { hostName: string, possword: string, dbName: string, userName: string }) => {
    return get(context.state, "/line_chart/clearOM3Table")
}

const getAllCustomTables: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllCustomTables`;
    return get(context.state, combinedUrl);
}

const performTransformForSingeLine: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { startTime: string, endTime: string, tableName: string }) => {
    const combinedUrl = `/line_chart/performTransformForSingeLine?start_time=${payload.startTime}&end_time=${payload.endTime}&table_name=${payload.tableName}`;
    return get(context.state, combinedUrl);
}
const performTransformForMultiLine: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>, payload: { startTime: string, endTime: string, tableNames: Array<string>, multiLineClassName: string }) => {
    const combinedUrl = `/line_chart/performTransformForMultiLine?start_time=${payload.startTime}&end_time=${payload.endTime}&table_name=${payload.tableNames}&line_class=${payload.multiLineClassName}`;
    return get(context.state, combinedUrl);
}

const loadCustomTableAndInfo: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllCustomTableAndInfo?mode=${context.state.controlParams.currentMode}`;
    get(context.state, combinedUrl).then((res) => {
        if (res['code'] === 200) {
            context.commit("updateCustomTableAndInfo", { customTables: res['data']['table_name'], customTableInfo: res['data']['table_info'] })
        } else {
            console.log(res['msg'])
        }
    })
}

const loadDefaultTableAndInfo: ActionHandler<GlobalState, GlobalState> = (context: ActionContext<GlobalState, GlobalState>) => {
    const combinedUrl = `/line_chart/getAllDefaultTableAndInfo?mode=${context.state.controlParams.currentMode}`;
    get(context.state, combinedUrl).then((res) => {
        if (res['code'] === 200) {
            context.commit("updateDefaultTableAndInfo", { tables: res['data']['table_name'], tableInfo: res['data']['table_info'] })
        } else {

            console.log(res['msg'])
        }
    })
}



export {
    getAllTables,
    getAllCustomTables,
    getAllFlags,
    loadMultiTimeSeriesInitData,
    loadViewChangeQueryWSMinMaxMissDataInitData,//final method
    getAllMultiLineClassInfo,
    testCustomDBConn,
    createCustomDBConn,
    initOM3DB,
    clearOM3Table,
    performTransformForSingeLine,
    loadCustomTableAndInfo,
    performTransformForMultiLine,
    loadDefaultTableAndInfo,
}



