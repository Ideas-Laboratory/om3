import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import TrendTree from "./tend-query-tree";
import { getIndexTime } from "./format-data"
import store from "@/store";
import HaarTree from "./haar-tree";
import LevelDataManager from "@/model/level-data-manager";
import LevelIndexObj from "@/model/level-index-obj";
import { max } from "d3";
import { ElLoading } from 'element-plus'


export function getLevelData(start: TrendTree) {
    const result: Array<Array<number>> = [];
    let temp: TrendTree | null = start;
    let { startT, endT } = getIndexTime(temp.level, temp.index, store.state.controlParams.tableMaxLevel);

    while (temp != null) {
        endT = getIndexTime(temp.level, temp.index, store.state.controlParams.tableMaxLevel).endT;
        result.push(temp.yArray);
        temp = temp.nextSibling;
    }
    return { data: result, start: startT, end: endT, l: start.level };
}

export function getLevelDataForHaar(start: HaarTree) {
    const result: Array<number> = [];
    let temp: HaarTree | null = start;

    while (temp != null) {
        result.push(temp.value);
        temp = temp.nextSibling
    }

    return result;
}
export function isIntersect(first: Array<number>, second: Array<number>) {
    const headMax = first[0] >= second[0] ? first[0] : second[0];
    const tailMin = first[1] <= second[1] ? first[1] : second[1];
    if (headMax <= tailMin) {
        return {
            isIntersect: true,
            pos: headMax === first[0] ? 'second' : 'first'
        }
    } else {
        return {
            isIntersect: false,
            pos: headMax === first[0] ? 'second' : 'first'
        }
    }
}


//first - second   [2,5]-[1,3]=[4,5] [0,5]-[0,3]=[4,5]  
function computeDifRange(first: Array<number>, second: Array<number>): Array<Array<number>> {
    if (first[0] >= second[0] && first[1] <= second[1]) {
        return [];
    }
    if (first[0] >= second[0] && first[1] > second[1]) {
        return [[second[1] + 1, first[1]]];
    }
    if (first[0] < second[0] && first[1] <= second[1]) {
        return [[first[0], second[0] - 1]];
    }
    if (first[0] < second[0] && first[1] > second[1]) {
        return [[first[0], second[0] - 1], [second[1] + 1, first[1]]];
    }
    return []
}

export function computeNeedLoadDataRange(needRange: Array<number>, loadedSet: Array<Array<number>>) {

    let currentNeedSet = [[needRange[0], needRange[1]]];
    let nextNeedSet: Array<Array<number>> = [];
    for (let i = 0; i < loadedSet.length; i++) {
        for (let j = 0; j < currentNeedSet.length; j++) {
            if (isIntersect(currentNeedSet[j], loadedSet[i]).isIntersect) {
                nextNeedSet = [...nextNeedSet, ...computeDifRange(currentNeedSet[j], loadedSet[i])]
            } else {
                nextNeedSet.push(currentNeedSet[j]);
            }
        }
        currentNeedSet = nextNeedSet;
        nextNeedSet = [];
    }
    return currentNeedSet;
}
export function checkSetType(timeBoxRange: Array<number>, chartRange: Array<number>) {
    if (timeBoxRange[0] > chartRange[1] || timeBoxRange[1] < chartRange[0]) {
        return 3;
    }
    if (chartRange[0] >= timeBoxRange[0] && chartRange[1] <= timeBoxRange[1]) {
        return 1;
    }
    return 2;
}

export function computeTimeRangeParent(maxLevel: number, width: number, timeRange: Array<number>) {
    const timeWidth = timeRange[1] - timeRange[0] + 1;

    const modNum = Math.floor(timeWidth / width);
    const ceilPow = Math.floor(Math.log2(modNum));
    if (modNum > 0) {
        return [maxLevel - ceilPow, Math.floor(timeRange[0] / (2 ** (ceilPow)))]
    } else {
        return [maxLevel, timeRange[0]];
    }
}

export function computeTimeRangeChild(currentLevel: number, maxLevel: number, range: Array<number>) {
    for (let i = currentLevel + 1; i <= maxLevel; i++) {
        range[0] = range[0] * 2;
        range[1] = range[1] * 2 + 1;
    }
    return range;
}

export function computeTimeSE(currentLevel: number, width: number, timeRange: Array<number>, globalDataLen: number, maxLevel: number, name?: string) {
    const res = new Array<NoUniformColObj>();
    for (let i = 0; i < width; i++) {
        res.push(new NoUniformColObj(i, timeRange[0], timeRange[1], currentLevel, width, globalDataLen, maxLevel, name));
    }
    const timeRangeLength = timeRange[1] - timeRange[0] + 1;
    const startTime = timeRange[0];
    const timeGap = Math.ceil(timeRangeLength / width);

    const minSegmentIndex = Math.floor(width * (timeRange[0] - startTime) / timeRangeLength);
    const maxSegmentIndex = Math.floor(width * (timeRange[1] - startTime) / timeRangeLength);

    let previousSegmentIndex = minSegmentIndex;
    for (let i = minSegmentIndex; i <= maxSegmentIndex; i++) {
        const relativeStartTime = i * timeRangeLength / width + startTime;
        const relativeEndTime = (i + 1) * timeRangeLength / width + startTime - 1;

        const segmentStart = Math.ceil(relativeStartTime);
        const segmentEnd = Math.floor(relativeEndTime);

        if (segmentStart <= segmentEnd) {
            res[i].setTStart(segmentStart);
            res[i].setTEnd(segmentEnd);
        }

        previousSegmentIndex = i;
    }
    return res;
}
export function computeTimeSE1(currentLevel: number, width: number, timeRange: Array<number>, globalDataLen: number, maxLevel: number, name?: string) {
    const originIndex = [];
    const res = new Array<NoUniformColObj>();
    for (let i = 0; i < width; i++) {
        res.push(new NoUniformColObj(i, timeRange[0], timeRange[1], currentLevel, width, globalDataLen, maxLevel, name));
    }
    const tR = timeRange[1] - timeRange[0] + 1;
    const tS = timeRange[0];
    for (let i = timeRange[0] + 1; i <= timeRange[1]; i++) {
        const pre = Math.floor(width * ((i - 1) - tS) / tR);
        const next = Math.floor(width * (i - tS) / tR);

        if (pre !== next) {
            originIndex.push(i)
            res[pre].setTEnd(i - 1);
            res[next].setTStart(i);
        }
    }

    return res;
}

export function computeColTimeRange(currentLevel: number, width: number, timeRange: Array<number>, globalDataLen: number, maxLevel: number) {
    const res = new Array(width);
    for (let i = 0; i < width; i++) {
        res[i] = [0, 0];
    }
    const tR = timeRange[1] - timeRange[0] + 1;
    const tS = timeRange[0];
    for (let i = timeRange[0] + 1; i <= timeRange[1]; i++) {
        const pre = Math.floor(width * ((i - 1) - tS) / tR);
        const next = Math.floor(width * (i - tS) / tR);
        if (pre !== next) {
            res[pre][1] = i - 1;
            res[next][0] = i;
        }
    }
    return res;
}

export function computeLosedDataRange(parents: Array<TrendTree>) {
    const losedDataRange: Array<Array<number>> = [];
    parents.forEach(v => {
        losedDataRange.push([v.level + 1, v.index * 2, v.index * 2 + 1]);
    });
    return losedDataRange;
}
export function computeLosedDataRangeV1(parents: Array<TrendTree>) {
    const losedDataRange: Array<Array<number>> = [];

    parents.forEach(v => {
        if (v.nodeType !== 'O') {
            console.log("compute losed node cut");
            console.log(v)
        }
        if (v._leftChild === null && v._rightChild === null && v.nodeType === 'O') {
            losedDataRange.push([v.level, v.index, v.index]);
        }
    });
    return losedDataRange;
}

export function computeLosedDataRangeV1ForRawMinMax(parents: Array<TrendTree>) {
    const losedDataRange: Array<Array<number>> = [];

    parents.forEach(v => {
        if (v._leftChild === null && v._rightChild === null && v.nodeType === 'O') {
            losedDataRange.push([v.level, v.index, v.index]);
        } else if (v._leftChild === null || v._rightChild === null) {
            throw new Error("child invaild")
        }
    });
    return losedDataRange;
}

export function canCut1(node: TrendTree, col1: NoUniformColObj, col2: NoUniformColObj, yScale: any) {
    return false;
}

export function canCut(node: TrendTree, col1: NoUniformColObj, col2: NoUniformColObj, yScale: any) {

    // if (col1.vRange[0] === Infinity || col1.vRange[1] === -Infinity || col2.vRange[0] === Infinity || col2.vRange[1] === -Infinity) {
    //     console.log("cant cut1")
    //     // return false;
    // }
    // if (node.nodeType === 'NULL') {

    //     return true;
    // }

    // if (col1.vRange[0] === Infinity || col1.vRange[1] === -Infinity || col2.vRange[0] === Infinity || col2.vRange[1] === -Infinity) {
    //     //console.log("cant cut")
    //     return false;
    // }
    // if ((node.nextSibling && node.nextSibling.nodeType === 'NULL') || (node.previousSibling && node.previousSibling.nodeType === "NULL")) {
    //    // console.log("next null")
    //     return true
    // }
    // if (node.parent && (node.parent.nodeType === 'LEFTNULL' || node.parent.nodeType === 'RIGHTNULL')) {
    //     //console.log(node)
    //     return false;
    // }
    // console.log("1111111")
    // return false
    if (node.nextSibling && node.nextSibling.nodeType === 'NULL') {
        return false;
    }
    if (node.previousSibling && node.previousSibling.nodeType === 'NULL') {
        return false;
    }
    const nodeRange0 = node.yArray[1];
    const nodeRange1 = node.yArray[2];
    if (col1.vRange[0] <= nodeRange0 && col1.vRange[1] >= nodeRange1 && col2.vRange[0] <= nodeRange0 && col2.vRange[1] >= nodeRange1) {
        return true
    } else {
        return false;
    }


    // if (Math.ceil(yScale(col1.vRange[0])) <= nodeRange0 && Math.ceil(yScale(col1.vRange[1])) >= nodeRange1 && Math.ceil(yScale(col2.vRange[0])) <= nodeRange0 && Math.ceil(yScale(col2.vRange[1])) >= nodeRange1) {
    //     return true
    // } else {
    //     return false;
    // }
    // if (col1.vRange[0] > col2.vRange[0]) {
    //     const temp = col1;
    //     col1 = col2;
    //     col2 = temp;
    // }
    // if (col2.vRange[0] > col1.vRange[1]) {
    //     return false;
    // }
    // const curRange = [col2.vRange[0], Math.min(col1.vRange[1], col2.vRange[1])];
    // if (curRange[1] < curRange[0]) {
    //     console.log(curRange);
    //     throw new Error("range error");
    // }
    // if (node.yArray[1]>= curRange[0] && node.yArray[2] <= curRange[1]) {
    //     return true
    // }
    // return false;
    // const curRange = [Math.floor(yScale(col2.vRange[0])), Math.ceil(yScale(Math.min(col1.vRange[1], col2.vRange[1])))];
    // if (curRange[1] < curRange[0]) {
    //     console.log(curRange)
    //     throw new Error("range error");
    // }
    // if (yScale(node.yArray[1]) >= curRange[0] && yScale(node.yArray[2]) <= curRange[1]) {
    //     return true
    // }
    // return false;
}

export function computeTimeFilterBaseLevelInfo(timeRange: Array<number>, viewWidth: number, maxLevel: number) {
    if (timeRange[1] - timeRange[0] + 1 <= viewWidth) {
        return [maxLevel, timeRange[0], timeRange[1]];
    }
    for (let l = Math.ceil(Math.log2(viewWidth)); l <= maxLevel; l++) {
        const nodeTimeRange = 2 ** maxLevel / (2 ** l);
        const timeStartIndex = Math.floor(timeRange[0] / nodeTimeRange);
        const timeEndIndex = Math.floor(timeRange[1] / nodeTimeRange);
        if (timeEndIndex >= 2 ** l) {
            continue;
        }
        if (timeEndIndex - timeStartIndex + 1 >= viewWidth) {
            return [l, timeStartIndex, timeEndIndex];
        }
    }
    return [];
}


function indexIsContain(levelObj: LevelIndexObj, first: number, last: number) {
    const loadedDataRange = levelObj.loadedDataRange;
    for (let i = 0; i < loadedDataRange.length; i++) {
        if (loadedDataRange[i][0] <= first && loadedDataRange[i][1] >= last) {
            return true
        }
    }
    return false
}
export function deleteSavedNodeIndex(mr: LevelDataManager, dataRanges: Array<Array<Array<number>>>) {
    const saveNodexIndex: Array<Array<Array<number>>> = []
    for (let i = 0; i < dataRanges.length; i++) {
        saveNodexIndex.push([])
        const curLevelNeedLoadRange = dataRanges[i]
        const curL = curLevelNeedLoadRange[0][0] + 1;
        if (!mr.levelIndexObjs[curL]) {
            for (let j = 0; j < curLevelNeedLoadRange.length; j++) {
                saveNodexIndex[i].push(curLevelNeedLoadRange[j]);
            }
        } else {
            if (mr.levelIndexObjs[curL].isFull) {
                continue
            } else {
                const curLevelObjInfoRange = mr.levelIndexObjs[curL];
                for (let j = 0; j < curLevelNeedLoadRange.length; j++) {
                    const firstIndex = curLevelNeedLoadRange[j][0] * 2;
                    const lastIndex = curLevelNeedLoadRange[j][0] * 2 + 1
                    if (indexIsContain(curLevelObjInfoRange, firstIndex, lastIndex)) {
                        continue
                    } else {
                        saveNodexIndex[i].push(curLevelNeedLoadRange[j])
                    }
                }
            }
        }
    }
    return saveNodexIndex;
}

export function modeTimeStamp(originTimeStamp: number, sementicType: 'ms' | 's' | 'm' | 'h' | 'd' | 'W' | 'M' | 'Y') {
    let modTimeStamp = originTimeStamp - originTimeStamp % getMSBySemanticType(sementicType)
    return modTimeStamp;
}

export function getMSBySemanticType(sementicType: 'ms' | 's' | 'm' | 'h' | 'd' | 'W' | 'M' | 'Y') {
    switch (sementicType) {
        case 'ms':
            return 1;
        case 's':
            return 1000;
        case 'm':
            return (60 * 1000);
        case 'h':
            return (60 * 60 * 1000);
        case 'd':
            return (24 * 60 * 60 * 1000);
        case 'W':
            return (7 * 24 * 60 * 60 * 1000);
        case 'M':
            return (30 * 24 * 60 * 60 * 1000);
        case 'Y':
            return (12 * 30 * 24 * 60 * 60 * 1000);
        default:
            throw new Error("sermantic type error");
    }
}
// semanticInterval 使用数值ms 做为单位, timeStart 为time series 真是的起始时间单位为ms, ser

/**
 * compute column real time range
 * @param currentLevel 使用数值ms 做为单位
 * @param width 
 * @param timeRange 结点的编号范围
 * @param timeStart 数据采集的真正的开始时间 timestamp ms
 * @param globalDataLen 
 * @param maxLevel 
 * @param semanticInterval 数据采集的真实的时间间隔 ms
 * @param semanticType 语义缩放的尺度
 * @param name 
 * @returns 
 */
export function computeSemanticColumn(currentLevel: number, width: number, timeRange: Array<number>, timeStart: number, globalDataLen: number, maxLevel: number, semanticInterval: number, semanticType: 'ms' | 's' | 'm' | 'h' | 'd' | 'W' | 'M' | 'Y', name?: string) {
    const semanticTypeRange = getMSBySemanticType(semanticType)
    timeStart = timeStart + timeRange[0] * semanticInterval;
    const res = [];
    const columnNum = Math.ceil((timeRange[1] - timeRange[0]) * semanticInterval / semanticTypeRange);
    const modeStart = modeTimeStamp(timeStart, semanticType);
    for (let i = 0; i < columnNum; i++) {
        res.push(new NoUniformColObj(i, Infinity, -Infinity, currentLevel, width, globalDataLen, maxLevel, name));
    }
    res[0].setRealStartAndInterval(timeStart, semanticInterval);
    res[0].setSemanticTStart(timeStart);
    res[0].setSemanticTEnd(modeStart + semanticTypeRange - 1);
    const newSenStart = modeStart + semanticTypeRange;
    for (let i = 1; i < columnNum; i++) {
        res[i].setRealStartAndInterval(timeStart, semanticInterval);
        res[i].setSemanticTStart(newSenStart + (i - 1) * semanticTypeRange);
        res[i].setSemanticTEnd(newSenStart + i * semanticTypeRange - 1);
    }
    return res;
}

export function convertWaveletToRawTableName(wName: string) {
    if (!wName.includes("wavelet")) {
        return wName;
    }
    const rowTableName = wName.split("_wavelet").join("");
    return rowTableName;
}

let userCookie = "";

export function getUserCookie() {
    if (userCookie === "") {
        userCookie = localStorage.getItem("om3_cookie")!;
    }
    return userCookie
}


// Function to convert ArrayBuffer to Base64 string
export function arrayBufferToBase64(arrayBuffer:ArrayBuffer) {
    let binary = '';
    const bytes = new Uint8Array(arrayBuffer);
    const len = bytes.byteLength;
    
    for (let i = 0; i < len; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    
    return btoa(binary);
  }
  
  // Function to convert Base64 string to ArrayBuffer
export function base64ToArrayBuffer(base64:string) {
    const binaryString = atob(base64);
    const len = binaryString.length;
    const bytes = new Uint8Array(len);
    
    for (let i = 0; i < len; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    
    return bytes.buffer;
  }
  
export function openLoading(msg:string){
    const loading = ElLoading.service({
        lock: true,
        text: msg,
        background: 'rgba(0, 0, 0, 0.7)',
      })
      return loading
}