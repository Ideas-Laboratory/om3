import { initWaveletDecode,initHaarDecode } from './wavlet-decoder';
import * as d3 from 'd3';
import { NoUniformColObj } from '@/model/non-uniform-col-obj';


export function getIndexTime(l: number, index: number, maxLevel: number) {
    const gap = 2 ** maxLevel / (2 ** l)
    const startTime = index * gap;
    const endTime = startTime + gap - 1;
    return {
        startT: startTime,
        endT: endTime
    }
}

export function getX(width: number, tS: number, tE: number, t: number, maxLevel: number) {
    return width * (t - tS) / (tE - tS + 1);
}


export function formatSampleM4Data(data: Array<any>, startTime: number, endTime: number, width: number) {
    console.log(startTime, endTime);
    const columnArray: any = [];
    for (let i = 0; i < width; i++) {
        columnArray.push({ min: Infinity, max: -Infinity });
    }
    let result = data.map((v) => {
        v.x = width * (v.t - startTime) / (endTime - startTime + 1);
        v.y = v.v;
        if (v.y > columnArray[Math.floor(v.x)].max) {
            columnArray[Math.floor(v.x)].max = v.y;
        }
        if (v.y < columnArray[Math.floor(v.x)].min) {
            columnArray[Math.floor(v.x)].min = v.y;
        }
        return v;
    });
    // console.log(columnArray);
    return result;
}
export function formatSampleArgM4Data(data: Array<any>, startTime: number, endTime: number, width: number) {
    const resultArray: Array<any> = [];
    data.forEach(v => {
        const x = Math.floor(width * (v.mint - startTime) / (endTime - startTime + 1));
        const y1 = v.mint_v;
        let y2;
        let y3;
        if (v.minv_t < v.maxv_t) {
            y2 = v.minv;
            y3 = v.maxv;
        } else {
            y2 = v.maxv;
            y3 = v.minv;
        }
        const y4 = v.maxt_v;
        resultArray.push({ x, y: y1, v: y1 });
        resultArray.push({ x, y: y2, v: y2 });
        resultArray.push({ x, y: y3, v: y3 });
        resultArray.push({ x, y: y4, v: y4 });
    });
    return resultArray;
}

export function formatDataForWaveletBrush(data: {
    mint: Array<any>,
    minv: Array<any>,
    maxv: Array<any>,
    maxt: Array<any>
}, levelInfo: Array<number>) {
    const dif = levelInfo[0] - levelInfo[1];
    console.log(dif);
    const result = new Array<any>()

    if (dif === 0) {
        data.mint.forEach((v, i) => {
            result.push({ x: i, y: data.mint[i], v: data.mint[i] });
        });
    } else if (dif === 1) {
        data.mint.forEach((v, i) => {
            result.push({ x: i, y: data.mint[i], v: data.mint[i] });
            result.push({ x: i, y: data.maxt[i], v: data.maxt[i] });
        });
    } else {
        data.mint.forEach((v, i) => {
            result.push({ x: i, y: data.mint[i], v: data.mint[i] });
            result.push({ x: i, y: data.minv[i], v: data.minv[i] });
            result.push({ x: i, y: data.maxv[i], v: data.maxv[i] });
            result.push({ x: i, y: data.maxt[i], v: data.maxt[i] });
        });
    }
    const maxv = d3.max(result, d => d.v);
    const minv = d3.min(result, d => d.v);
    return { result, rowData: [data.mint, data.minv, data.maxv, data.maxt], maxv, minv };
}

export function formatToRenderDataForTrend(levelInfo: Array<number>, data: { data: Array<Array<number>>, start: number, end: number, l: number }, width: number) {
    const result = new Array<any>();

    const dif = levelInfo[0] - levelInfo[1]
    const startT = getIndexTime(levelInfo[1], data.start, levelInfo[0]).startT;
    const endT = getIndexTime(levelInfo[1], data.start + data.data.length - 1, levelInfo[0]).endT;
    data.data.forEach((v, i) => {
        const indexTime = getIndexTime(levelInfo[1], data.start + i, levelInfo[0]);
        if (dif === 0) {
            result.push({ x: getX(width, startT, endT, indexTime.startT, levelInfo[0]), y: v[0], v: v[0] });
        } else if (dif === 1) {
            result.push({ x: getX(width, startT, endT, indexTime.startT, levelInfo[0]), y: v[0], v: v[0] });
            result.push({ x: getX(width, startT, endT, indexTime.endT, levelInfo[0]), y: v[3], v: v[3] });
        } else {
            const startX = getX(width, startT, endT, indexTime.startT, levelInfo[0]);
            const endX = getX(width, startT, endT, indexTime.endT, levelInfo[0])
            const midX1 = getX(width, startT, endT, indexTime.startT + 1, levelInfo[0])
            const midX2 = getX(width, startT, endT, indexTime.endT - 1, levelInfo[0]);
            if (v[0] < v[3]) {
                result.push({ x: startX, y: v[0], v: v[0] });
                result.push({ x: midX1, y: v[1], v: v[1] });
                result.push({ x: midX2, y: v[2], v: v[2] });
                result.push({ x: endX, y: v[3], v: v[3] });
            } else {
                result.push({ x: startX, y: v[0], v: v[0] });
                result.push({ x: midX1, y: v[2], v: v[2] });
                result.push({ x: midX2, y: v[1], v: v[1] });
                result.push({ x: endX, y: v[3], v: v[3] });
            }

        }
    });
    const maxv = d3.max(result, d => d.v);
    const minv = d3.min(result, d => d.v);
    return { renderData: result, maxv, minv };
}

export function getGlobalMinMaxInfo(data: { data: Array<Array<number>>, start: number, end: number, l: number }){
    const maxv = d3.max(data.data, d => d[2]);
    const minv = d3.min(data.data, d => d[1]);
    return { maxv, minv };
}

export function formatToRenderDataForHaar(data:Array<number>,width:number){
    const result=new Array<{x:number,y:number,v:number}>();
    let maxv=-Infinity;
    let minv=Infinity;
    data.forEach((v,i)=>{
        maxv=Math.max(v,maxv);
        minv=Math.min(v,minv);
        result.push({
            x:i,
            v:v,
            y:v
        });
    });
    return {renderData:result,maxv,minv};
}

export function fomatRenderDataForLittleThanWidth(timeRange: Array<number>, width: number, data: { data: Array<[number, number, number, number]>, start: number, end: number, l: number }) {
    if (timeRange[1] - timeRange[0] + 1 !== data.data.length) {
        throw new Error("data not match time range");
    }
    const result = new Array<any>();
    const xScale = d3.scaleLinear().domain([0, data.data.length - 1]).range([0, width - 1]);
    data.data.forEach((v, i) => {
        result.push({ x: xScale(i), y: v[0], v: v[0] });
    });
    return result;
}

export function formatRenderDataForViewChange(levelInfo: Array<number>, data: { data: Array<Array<number>>, start: number, end: number, l: number }, width: number) {
    const result = new Array<any>();
    const noPowResult = new Array<any>();
    const dif = levelInfo[0] - levelInfo[1];
    const startT = getIndexTime(levelInfo[1], data.start, levelInfo[0]).startT;
    const endT = getIndexTime(levelInfo[1], data.start + data.data.length - 1, levelInfo[0]).endT;
    data.data.forEach((v, i) => {
        const indexTime = getIndexTime(levelInfo[1], data.start + i, levelInfo[0])
        if (dif === 0) {
            result.push({ x: getX(width, startT, endT, indexTime.startT, levelInfo[0]), y: v[0], v: v[0] });
            noPowResult.push({ x: i, y: v[0], v: v[0] });
        } else if (dif === 1) {
            result.push({ x: getX(width, startT, endT, indexTime.startT, levelInfo[0]), y: v[0], v: v[0] });
            result.push({ x: getX(width, startT, endT, indexTime.endT, levelInfo[0]), y: v[3], v: v[3] });
            noPowResult.push({ x: 2 * i, y: v[0], v: v[0] });
            noPowResult.push({ x: 2 * i + 1, y: v[3], v: v[3] });
        } else {
            const startX = getX(width, startT, endT, indexTime.startT, levelInfo[0]);
            const endX = getX(width, startT, endT, indexTime.endT, levelInfo[0])
            const midX1 = getX(width, startT, endT, indexTime.startT + 1, levelInfo[0])
            const midX2 = getX(width, startT, endT, indexTime.endT - 1, levelInfo[0]);
            if (v[0] < v[3]) {
                result.push({ x: startX, y: v[0], v: v[0] });
                result.push({ x: midX1, y: v[1], v: v[1] });
                result.push({ x: midX2, y: v[2], v: v[2] });
                result.push({ x: endX, y: v[3], v: v[3] });
            } else {
                result.push({ x: startX, y: v[0], v: v[0] });
                result.push({ x: midX1, y: v[2], v: v[2] });
                result.push({ x: midX2, y: v[1], v: v[1] });
                result.push({ x: endX, y: v[3], v: v[3] });
            }
            noPowResult.push({ x: 4 * i, y: v[0], v: v[0] });
            noPowResult.push({ x: 4 * i + 1, y: v[1], v: v[1] });
            noPowResult.push({ x: 4 * i + 2, y: v[2], v: v[2] });
            noPowResult.push({ x: 4 * i + 3, y: v[3], v: v[3] });
        }

    });
    const maxv = d3.max(result, d => d.v);
    const minv = d3.min(result, d => d.v);
    return { powRenderData: result, noPowResult, maxv, minv };
}

export function formatNonPowDataForViewChange(nonUniformColObjs: Array<NoUniformColObj>, width: number, globalDataLen: number, yScale: any) {
    const result = [];
    const logRes = [];
    const startT = nonUniformColObjs[0].tStart;
    const endT = nonUniformColObjs[nonUniformColObjs.length - 1].tEnd;
    for (let i = 0; i < nonUniformColObjs.length; i++) {
        const tempRes = {
            startV: null,
            endV: null,
            min: Infinity,
            max: -Infinity,
        }
        const maxLevel = Math.log2(globalDataLen);
        const startX = getX(width, startT, endT, nonUniformColObjs[i].tStart, maxLevel);

        const endX = getX(width, startT, endT, nonUniformColObjs[i].tEnd, maxLevel);
        const midX1 = startX+0.1//getX(width, startT, endT, nonUniformColObjs[i].tStart+1, maxLevel);
        const midX2 = endX-0.1//getX(width, startT, endT, nonUniformColObjs[i].tEnd -1, maxLevel);
        nonUniformColObjs[i].positionInfo.startX = startX;
        nonUniformColObjs[i].positionInfo.endX = endX;
        nonUniformColObjs[i].positionInfo.minX = midX1;
        nonUniformColObjs[i].positionInfo.maxX = midX2;
        if (nonUniformColObjs[i].startV) {
            result.push({ x: getX(width, startT, endT, nonUniformColObjs[i].tStart, Math.log2(globalDataLen)), y: nonUniformColObjs[i].startV, v: nonUniformColObjs[i].startV });
           
        }

        if (nonUniformColObjs[i].startV !== undefined && nonUniformColObjs[i].endV !== undefined) {
            if (nonUniformColObjs[i].startV! < nonUniformColObjs[i].endV!) {
                result.push({ x: midX1, y: nonUniformColObjs[i].vRange[0], v: nonUniformColObjs[i].vRange[0] });
             
                result.push({ x: midX2, y: nonUniformColObjs[i].vRange[1], v: nonUniformColObjs[i].vRange[1] });
              
            } else {
                result.push({ x: midX1, y: nonUniformColObjs[i].vRange[1], v: nonUniformColObjs[i].vRange[1] });
               
                result.push({ x: midX2, y: nonUniformColObjs[i].vRange[0], v: nonUniformColObjs[i].vRange[0] });
               
            }

        } else {
            result.push({ x: startX, y: nonUniformColObjs[i].vRange[0], v: nonUniformColObjs[i].vRange[0] });
            
            result.push({ x: endX, y: nonUniformColObjs[i].vRange[1], v: nonUniformColObjs[i].vRange[1] });
           
        }


        if (nonUniformColObjs[i].endV) {
            result.push({ x: endX, y: nonUniformColObjs[i].endV, v: nonUniformColObjs[i].endV });
           
        }
    }
   
    return result;
}

export function formatHaarInitData(data: Array<any>, levelInfo: Array<number>){
    const haarDif: Array<number> = [];
    const dif = levelInfo[0] - levelInfo[1];
    data.forEach(v => {
        haarDif.push(v.dif);
       
    });

    let tempLast = haarDif.pop();
    haarDif.unshift(tempLast!);
   
    const tempResult = initHaarDecode(haarDif);
    const result = new Array<any>();
    tempResult.forEach((v,i)=>{
        result.push({x:i,v:v,y:v})
    });
    return result;
}

export function formatSampleWaveletInitData(data: Array<any>, levelInfo: Array<number>) {
    const mintd: Array<number> = [];
    const minvd: Array<number> = [];
    const maxvd: Array<number> = [];
    const maxtd: Array<number> = [];
    const dif = levelInfo[0] - levelInfo[1];
    data.forEach(v => {
        mintd.push(v.minid);
        minvd.push(v.minvd);
        maxvd.push(v.maxvd);
        maxtd.push(v.maxid);
    });

    let tempLast = mintd.pop();
    mintd.unshift(tempLast!);
    tempLast = minvd.pop();
    minvd.unshift(tempLast!);
    tempLast = maxvd.pop();
    maxvd.unshift(tempLast!);
    tempLast = maxtd.pop();
    maxtd.unshift(tempLast!);
    const tempResult = initWaveletDecode({ mintd, minvd, maxvd, maxtd });
    const result = new Array<any>();
    if (dif === 0) {
        tempResult.maxt.forEach((v, i) => {
            result.push({ x: i, y: tempResult.mint[i], v: tempResult.mint[i] });
        });
    } else if (dif === 1) {
        tempResult.maxt.forEach((v, i) => {
            result.push({ x: i, y: tempResult.mint[i], v: tempResult.mint[i] });
            result.push({ x: i, y: tempResult.maxt[i], v: tempResult.maxt[i] });
        });
    } else {
        tempResult.maxt.forEach((v, i) => {
            result.push({ x: i, y: tempResult.mint[i], v: tempResult.mint[i] });
            result.push({ x: i, y: tempResult.minv[i], v: tempResult.minv[i] });
            result.push({ x: i, y: tempResult.maxv[i], v: tempResult.maxv[i] });
            result.push({ x: i, y: tempResult.maxt[i], v: tempResult.maxt[i] });
        });
    }

    return { result, rowData: [tempResult.mint, tempResult.minv, tempResult.maxv, tempResult.maxt] };
}

export function formatDataForBaseData(data: Array<any>, width: number, startTime: number, endTime: number) {

    data.forEach((v, i) => {
        v.x = (width) * (v.t - startTime) / (endTime - startTime + 1);
        v.y = v.v;
    });
    return data;
}

export function formatDataForMultiM4(data:Array<{t:number,v:number}>,width:number,startTime:number,endTime:number){
    const result=new Array<{t:number,v:number,x:number,y:number}>();
    let maxV=-Infinity;
    let minV=Infinity;
    data.forEach(v=>{
        maxV=Math.max(maxV,v.v);
        minV=Math.min(minV,v.v);
        result.push({t:v.t,v:v.v,x:(width) * (v.t - startTime) / (endTime - startTime + 1),y:0});
    });
    return {minv:minV,maxv:maxV,res:result};
}