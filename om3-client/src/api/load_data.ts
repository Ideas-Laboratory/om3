import axios from "axios";
import store, { ws } from "../store"
import { getUserCookie } from "@/helper/util";
export async function batchLoadDif(losedDataInfo: Array<Array<number>>,manager:any) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.get(`postgres/line_chart/batchLevelDataProgressiveWavelet?table_name=${manager.dataName}&losedDataInfo[]=${losedDataInfo}`);
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l: manager.maxLevel - result[0][i], i: result[1][i], dif: [result[2][i], result[3][i], result[4][i], result[5][i]] });
        }
    }

    return resultArray;
}
export async function batchLoadDifWidthPost(losedDataInfo: Array<Array<number>>,manager:any) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/batchLevelDataProgressiveWavelet`, {
        table_name: manager.dataName.includes(".") ? manager.dataName.split(".")[1] : manager.dataName,//store.state.controlParams.currentTable,
        losedDataInfo: JSON.stringify({ data: losedDataInfo }),
        line_type:store.state.controlParams.currentLineType
    });
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l: manager.maxLevel - result[0][i], i: result[1][i], dif: [result[2][i], result[3][i], result[4][i], result[5][i]] });
        }
    }
    return resultArray;
}

export async function batchLoadDifWidthPostForMinMaxMiss(losedDataInfo: Array<Array<number>>,manager:any) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/batchLevelDataProgressiveWaveletMinMaxMiss`, {
        table_name: manager.dataName.includes(".") ? manager.dataName.split(".")[1] : manager.dataName,//store.state.controlParams.currentTable,
        losedDataInfo: JSON.stringify({ data: losedDataInfo }),
        line_type:store.state.controlParams.currentLineType
    });
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l: manager.maxLevel - result[0][i], i: result[1][i], dif: [0,result[2][i], result[3][i],0] });
        }
    }
    return resultArray;
}
export async function batchLoadDifWidthPostRawMinMax(losedDataInfo: Array<Array<number>>,manager:any) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/batchLevelDataProgressiveWaveletRawMinMax`, {
        table_name: manager.dataName.includes(".") ? manager.dataName.split(".")[1] : manager.dataName,//store.state.controlParams.currentTable,
        losedDataInfo: JSON.stringify({ data: losedDataInfo }),
    });
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l: manager.maxLevel - result[0][i], i: result[1][i], dif: [result[2][i], result[3][i], result[4][i], result[5][i]] });
        }
    }
    return resultArray;
}
export async function batchLoadDifWidthPost1(losedDataInfo: Array<Array<number>>,manager:any) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/batchLevelDataProgressiveWavelet1`, {
        table_name: manager.dataName.includes(".") ? manager.dataName.split(".")[1] : manager.dataName,//store.state.controlParams.currentTable,
        losedDataInfo: JSON.stringify({ data: losedDataInfo }),
    });
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l: manager.maxLevel - result[0][i], i: result[1][i], dif: [result[2][i], result[3][i], result[4][i], result[5][i]] });
        }
    }
    return resultArray;
}
export async function batchLoadDifWidthPost1MinMax(losedDataInfo: Array<Array<number>>,manager:any) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/batchLevelDataProgressiveWaveletMinMax`, {
        table_name: manager.dataName.includes(".") ? manager.dataName.split(".")[1] : manager.dataName,
        losedDataInfo: JSON.stringify({ data: losedDataInfo }),
    });
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l: manager.maxLevel - result[0][i], i: result[1][i], dif: [0,result[2][i], result[3][i],0]});
        }
    }
    return resultArray;
}
export async function loadDif(level: number, start: number, end: number) {

    //@ts-ignore
    const { data } = await axios.get(`postgres/line_chart/trendQueryProgressiveWavelet?table_name=${this.dataName}&current_level=${level}&start_time=${start}&end_time=${end}`);
    const resultData = [];
    for (let i = 0; i < data[0].length; i++) {
        resultData.push([data[0][i], data[1][i], data[2][i], data[3][i]]);
    }
    return resultData;
}

export async function batchLoadAllDifWidthPost(losedDataInfo: Array<Array<Array<number>>>,manager:any) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/batchAllLevelDataWaveletPostHandler`, {
        table_name: manager.dataName.includes(".") ? manager.dataName.split(".")[1] : manager.dataName,//store.state.controlParams.currentTable,
        losedDataInfo: JSON.stringify({ data: losedDataInfo }),
    });
    //console.log(data)
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l: manager.maxLevel - result[0][i], i: result[1][i], dif: [0, result[2][i], result[3][i], 0] });
        }
    }
    resultArray.sort((a, b) => {
        if (a.l !== b.l) {
            return a.l - b.l
        } else {
            return a.i - b.i
        }
    })
    return resultArray;
}

export function batchLoadWithWs(losedDataInfo:Array<Array<number>>, dataName: string, url: string, maxLevel: number,tagName?:string){
    if (losedDataInfo.length === 0) {
        return [];
    }
    const sendData = {
        url: url,
        tn: dataName.includes(".") ? dataName.split(".")[1] : dataName,//store.state.controlParams.currentTable,
        data: JSON.stringify({ data: losedDataInfo }),
        tagName:tagName,
    }
    const startT = new Date().getTime()
    return new Promise((resolve, reject) => {
        ws.send(JSON.stringify(sendData))
        ws.onmessage = (e) => {
            // const reveiveData = e.data.toString();
           //console.log("websocket load time:",new Date().getTime()-startT)
            //@ts-ignore
            const result = JSON.parse(e.data)["data"]
            const resultArray = [];
            if (result && result[0] && result[0].length > 0) {
                for (let i = 0; i < result[0].length; i++) {
                    resultArray.push({ l: maxLevel - result[0][i], i: result[1][i], dif: [result[2][i], result[3][i], result[4][i], result[5][i]] });
                }
            }
            resolve(resultArray)
        }
    })
}

export function batchLoadWithWsForRawMinMax(losedDataInfo:Array<Array<number>>, dataName: string, url: string, maxLevel: number,tagName?:string){
    if (losedDataInfo.length === 0) {
        return [];
    }
    const sendData = {
        url: url,
        tn: dataName.includes(".") ? dataName.split(".")[1] : dataName,//store.state.controlParams.currentTable,
        data: JSON.stringify({ data: losedDataInfo }),
        tagName:tagName,
    }
    const startT = new Date().getTime()
    return new Promise((resolve, reject) => {
        ws.send(JSON.stringify(sendData))
        ws.onmessage = (e) => {
            // const reveiveData = e.data.toString();
           //console.log("websocket load time:",new Date().getTime()-startT)
            //@ts-ignore
            const result = JSON.parse(e.data)["data"]
            const resultArray = [];
            if (result && result[0] && result[0].length > 0) {
                for (let i = 0; i < result[0].length; i++) {
                    resultArray.push({ l: maxLevel - result[0][i], i: result[1][i], dif: [result[2][i], result[3][i], result[4][i], result[5][i]] });
                }
            }
            resolve(resultArray)
        }
    })
}
export function batchLoadMinMaxMissWithWs(losedDataInfo:Array<Array<number>>, dataName: string, url: string, maxLevel: number,tagName?:string){
    if (losedDataInfo.length === 0) {
        return [];
    }
    const sendData = {
        url: url,
        tn: dataName.includes(".") ? dataName.split(".")[1] : dataName,//store.state.controlParams.currentTable,
        data: JSON.stringify({ data: losedDataInfo }),
        tagName:tagName,
        line_type:store.state.controlParams.currentLineType,
        mode:store.state.controlParams.currentMode,
        user_cookie:getUserCookie()
    }
    const startT = new Date().getTime()
    return new Promise((resolve, reject) => {
        ws.send(JSON.stringify(sendData))
        ws.onmessage = (e) => {
            // const reveiveData = e.data.toString();
           //console.log("websocket load time:",new Date().getTime()-startT)
            //@ts-ignore
            const result = JSON.parse(e.data)["data"]
            const resultArray = [];
            if (result && result[0] && result[0].length > 0) {
                for (let i = 0; i < result[0].length; i++) {
                    resultArray.push({ l: result[0][i], i: result[1][i], dif: [0, result[2][i], result[3][i], 0] });
                }
            }
            resolve(resultArray)
        }
    })
}

export async function batchLoadMinMaxMissWithPostForMultiLineType(losedDataInfo:Array<Array<number>>, dataName: string, url: string, maxLevel: number,tagName?:string) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/batchLoadMinMaxMissWithPostForMultiLineType`, {
        table_name: dataName.includes(".") ? dataName.split(".")[1] : dataName,
        losedDataInfo:  {data:losedDataInfo} ,
        tagName:tagName,
        line_type:store.state.controlParams.currentLineType,
        mode:store.state.controlParams.currentMode
    });
    const result = data.data;
    const resultArray = [];
    if (result && result[0] && result[0].length > 0) {
        for (let i = 0; i < result[0].length; i++) {
            resultArray.push({ l:  result[0][i], i: result[1][i], dif: [0,result[2][i], result[3][i],0] });
        }
    }
    return resultArray;
}