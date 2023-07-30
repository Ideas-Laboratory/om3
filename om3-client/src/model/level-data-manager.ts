import LevelIndexObj from "./level-index-obj";
import axios from "axios";
import TrendTree from "@/helper/tend-query-tree";
import store, { pushTimeArray } from "@/store";
import * as d3 from 'd3';
import { canCut, checkSetType, computeLosedDataRange, computeLosedDataRangeV1, computeTimeSE, deleteSavedNodeIndex, computeSemanticColumn, convertWaveletToRawTableName, computeLosedDataRangeV1ForRawMinMax } from "@/helper/util";
import { NoUniformColObj } from "./non-uniform-col-obj";
import { UniformGapObj } from "./uniform-gap-obj";
import { loadDataForRangeLevel, batchLoadDataForRangeLevelRawMinMax, batchLoadDataForRangeLevel, batchLoadDataForRangeLevel1, batchLoadDataForRangeLevel2MinMaxMiss, batchLoadDataForRangeLevel1MinMaxMiss, batchLoadDataForRangeLevel1WS, batchLoadDataForRangeLevelForMinMaxMiss } from "../api/build_tree"


import Cache from "lru-cache"
import { getFlag } from "@/global_state/state";




let allTimes: any = []
let allSumTimes: any = []

export default class LevelDataManager {
    levelIndexObjs: Array<LevelIndexObj>
    maxLevel: number
    realDataRowNum: number
    dataName: string
    md5Num?: number
    isShow: boolean
    columnInfos: Array<NoUniformColObj> | null
    curNodeNum: number
    dataCache: Array<TrendTree>
    cacheMap: Map<number, TrendTree>
    cacheHead: TrendTree | null
    cacheTail: TrendTree | null
    maxCacheNodeNum: number
    lruCache: any
    deleteQueue: Array<TrendTree>
    isIntering: boolean
    isEvicting: boolean
    constructor(levelIndexObjs: Array<LevelIndexObj>, dataName: string, maxLevel?: number) {
        this.levelIndexObjs = levelIndexObjs;
        this.maxLevel = maxLevel ? maxLevel : store.state.controlParams.tableMaxLevel;
        this.realDataRowNum = 2 ** (maxLevel ? maxLevel : store.state.controlParams.tableMaxLevel);
        this.dataName = dataName;
        this.isShow = true;
        this.columnInfos = null;
        this.curNodeNum = 0;
        this.dataCache = new Array<TrendTree>();
        this.cacheMap = new Map<number, TrendTree>();
        this.cacheHead = null;
        this.cacheTail = null;
        this.maxCacheNodeNum = 100000
        this.lruCache = null;
        this.initCache();
        this.deleteQueue = [];
        this.isIntering = false;
        this.isEvicting = false
    }


    async getDataMockServer(info: { level: number, start: number, end: number, offset: number, width: number }) {
        const internalLevel = this.maxLevel - info.level;
        const outterLevel = info.level;
        const nextLevel = internalLevel;
        if (nextLevel < 1) {
            throw new Error("level out of range")
        }
        const width = 2 ** Math.ceil(Math.log2(info.width));
        let offset = info.offset

        let end = info.end;
        let start = info.start;
        let focusPoint = start + offset;
        let nextStart = focusPoint - width / 4;
        let nextEnd = focusPoint + width / 4;


        if (nextStart <= start) {
            nextStart = start;
            nextEnd = start + width / 2 - 1;
        }
        if (nextEnd >= end) {
            nextEnd = end;
            nextStart = end - width / 2 + 1;
        }
        const data = await this.getData(outterLevel + 1, nextStart * 2, nextStart * 2 + width - 1);
        return [[offset, 2 * nextStart], data];
    }
    async getData(level: number, start: number, end: number, noRet?: boolean) {
        if (level > this.maxLevel) {
            return []
        }
        let lastFullLevel = 2;
        let nextStart = start;
        let nextEnd = end;
        for (let i = level - 1; i > 1; i--) {
            nextStart = Math.floor(nextStart / 2);
            nextEnd = Math.floor(nextEnd / 2);
            if (this.levelIndexObjs[i] && this.levelIndexObjs[i].isFull) {
                lastFullLevel = i;
                break;
            }
        }
        for (let i = lastFullLevel + 1; i <= level; i++) {
            nextStart = nextStart * 2;
            nextEnd = nextEnd * 2 + 1;
            const losedDataInfo = this.checkLoadedDataInSingalLevel([[i, nextStart, nextEnd]]);
            //debugger
            await batchLoadDataForRangeLevel(losedDataInfo, this);
        }
        if (this.levelIndexObjs[level]) {
            //更新缓存
            // this.updateDataCacheForRange(level, start, end)
            if (noRet) {
                return { data: [], start: 0, end: 0, l: 0 }
            } else {
                if (this.dataName === 'sensor8') {
                    const extraDataLevel = Math.ceil(Math.log2(2 ** this.maxLevel - this.realDataRowNum));
                    if (level >= this.maxLevel - extraDataLevel) {
                        const curExtraLevel = level - (this.maxLevel - extraDataLevel);
                        const fakeRowNum = 2 ** curExtraLevel;
                        if (end >= 2 ** level - fakeRowNum) {
                            end = 2 ** level - fakeRowNum - 1;
                        }
                    }
                    let lastIndex = this.realDataRowNum - 1;
                    for (let i = this.maxLevel; i >= level; i--) {
                        lastIndex = Math.floor(lastIndex / 2);
                    }
                    if (end < start) {
                        return [];
                    }
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                } else {
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                }
            }

        } else {
            return { data: [], start: 0, end: 0, l: 0 }
            //throw new Error("cannot get data from data manager");
        }
    }
    async getDataMinMaxMiss(level: number, start: number, end: number, noRet?: boolean) {
        if (level > this.maxLevel) {
            return []
        }
        let lastFullLevel = 2;
        let nextStart = start;
        let nextEnd = end;
        for (let i = level - 1; i > 1; i--) {
            nextStart = Math.floor(nextStart / 2);
            nextEnd = Math.floor(nextEnd / 2);
            if (this.levelIndexObjs[i] && this.levelIndexObjs[i].isFull) {
                lastFullLevel = i;
                break;
            }
        }

        for (let i = lastFullLevel + 1; i <= level; i++) {
            nextStart = nextStart * 2;
            nextEnd = nextEnd * 2 + 1;
            const losedDataInfo = this.checkLoadedDataInSingalLevel([[i, nextStart, nextEnd]]);
            const processLosedDataInfo = [];
            for (let j = 0; j < losedDataInfo.length; j++) {
                const tempInfo = losedDataInfo[j]
                processLosedDataInfo.push([tempInfo[0] - 1, Math.floor(tempInfo[1] / 2), Math.floor(tempInfo[2] / 2)])
            }

            await batchLoadDataForRangeLevel2MinMaxMiss(processLosedDataInfo, this);
        }

        if (this.levelIndexObjs[level]) {
            //更新缓存
            // this.updateDataCacheForRange(level, start, end)
            if (noRet) {
                return { data: [], start: 0, end: 0, l: 0 }
            } else {
                if (this.dataName === 'sensor8') {
                    const extraDataLevel = Math.ceil(Math.log2(2 ** this.maxLevel - this.realDataRowNum));
                    if (level >= this.maxLevel - extraDataLevel) {
                        const curExtraLevel = level - (this.maxLevel - extraDataLevel);
                        const fakeRowNum = 2 ** curExtraLevel;
                        if (end >= 2 ** level - fakeRowNum) {
                            end = 2 ** level - fakeRowNum - 1;
                        }
                    }
                    let lastIndex = this.realDataRowNum - 1;
                    for (let i = this.maxLevel; i >= level; i--) {
                        lastIndex = Math.floor(lastIndex / 2);
                    }
                    if (end < start) {
                        return [];
                    }
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                } else {
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                }
            }

        } else {
            return { data: [], start: 0, end: 0, l: 0 }
            //throw new Error("cannot get data from data manager");
        }
    }

    async getDataRawMinMax(level: number, start: number, end: number, noRet?: boolean) {
        if (level > this.maxLevel) {
            return []
        }
        let lastFullLevel = 2;
        let nextStart = start;
        let nextEnd = end;
        //debugger
        for (let i = level - 1; i > 1; i--) {
            nextStart = Math.floor(nextStart / 2);
            nextEnd = Math.floor(nextEnd / 2);
            if (this.levelIndexObjs[i] && this.levelIndexObjs[i].isFull) {
                lastFullLevel = i;
                break;
            }
        }
        for (let i = lastFullLevel + 1; i <= level; i++) {
            nextStart = nextStart * 2;
            nextEnd = nextEnd * 2 + 1;
            const losedDataInfo = this.checkLoadedDataInSingalLevel([[i, nextStart, nextEnd]]);
            //debugger
            await batchLoadDataForRangeLevelRawMinMax(losedDataInfo, this);
        }
        if (this.levelIndexObjs[level]) {
            //更新缓存
            // this.updateDataCacheForRange(level, start, end)
            if (noRet) {
                return { data: [], start: 0, end: 0, l: 0 }
            } else {
                if (this.dataName === 'sensor8') {
                    const extraDataLevel = Math.ceil(Math.log2(2 ** this.maxLevel - this.realDataRowNum));
                    if (level >= this.maxLevel - extraDataLevel) {
                        const curExtraLevel = level - (this.maxLevel - extraDataLevel);
                        const fakeRowNum = 2 ** curExtraLevel;
                        if (end >= 2 ** level - fakeRowNum) {
                            end = 2 ** level - fakeRowNum - 1;
                        }
                    }
                    let lastIndex = this.realDataRowNum - 1;
                    for (let i = this.maxLevel; i >= level; i--) {
                        lastIndex = Math.floor(lastIndex / 2);
                    }
                    if (end < start) {
                        return [];
                    }
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                } else {
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                }
            }

        } else {
            return { data: [], start: 0, end: 0, l: 0 }
            //throw new Error("cannot get data from data manager");
        }
    }
    async getDataV1(level: number, start: number, end: number, noRet?: boolean) {
        if (level > this.maxLevel) {
            return [];
        }
        if (this.levelIndexObjs[level]) {

            const hasData = this.levelIndexObjs[level].hasDataForRange(start, end);
            if (hasData.has) {
                return this.levelIndexObjs[level].getDataByIndex(start, end);
            } else {
                const losedRange = hasData.range;

                for (let i = 0; i < losedRange.length; i++) {
                    let l = level - 1;
                    const losedDataOtherLevel = [];
                    for (; l >= 0; l--) {
                        if (this.levelIndexObjs[l].isFull) {
                            break;
                        }
                        const levelHasData = this.levelIndexObjs[l].hasDataForRange(Math.floor(losedRange[i][0] / (2 ** (level - l))), Math.floor(losedRange[i][1] / (2 ** (level - l))));
                        if (levelHasData.has) {
                            break;
                        }
                        losedDataOtherLevel.push([l, Math.floor(losedRange[i][0] / (2 ** (level - l))), Math.floor(losedRange[i][1] / (2 ** (level - l)))]);
                    }
                    await loadDataForRangeLevel(losedDataOtherLevel, this);
                }
                losedRange.forEach(range => {
                    range.unshift(level);
                });
                await loadDataForRangeLevel(losedRange, this);
            }
        } else {
            let l = level - 1;
            const losedDataOtherLevel = [];
            for (; l >= 0; l--) {
                if (this.levelIndexObjs[l] && this.levelIndexObjs[l].isFull) {
                    break;
                }
                if (this.levelIndexObjs[l]) {
                    const levelHasData = this.levelIndexObjs[l].hasDataForRange(Math.floor(start / (2 ** (level - l))), Math.floor(end / (2 ** (level - l))));
                    if (levelHasData.has) {
                        break;
                    }
                }
                losedDataOtherLevel.push([l, Math.floor(start / (2 ** (level - l))), Math.floor(end / (2 ** (level - l)))]);
            }
            losedDataOtherLevel.unshift([level, start, end]);
            await loadDataForRangeLevel(losedDataOtherLevel, this);
        }
        if (this.levelIndexObjs[level]) {
            if (noRet) {
                return []
            } else {
                if (this.dataName === 'sensor8') {
                    const extraDataLevel = Math.ceil(Math.log2(2 ** this.maxLevel - this.realDataRowNum));
                    if (level >= this.maxLevel - extraDataLevel) {
                        const curExtraLevel = level - (this.maxLevel - extraDataLevel);
                        const fakeRowNum = 2 ** curExtraLevel;
                        if (end >= 2 ** level - fakeRowNum) {
                            end = 2 ** level - fakeRowNum - 1;
                        }
                    }
                    let lastIndex = this.realDataRowNum - 1;
                    for (let i = this.maxLevel; i >= level; i--) {
                        lastIndex = Math.floor(lastIndex / 2);
                    }
                    if (end < start) {
                        return [];
                    }
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                } else {
                    return this.levelIndexObjs[level].getDataByIndex(start, end);
                }
            }
        } else {
            throw new Error("cannot get data from data manager");
        }
    }
    getLoadedData(level: number, start: number, end: number) {
        if (this.levelIndexObjs[level]) {
            if (this.dataName === 'sensor8') {
                const extraDataLevel = Math.ceil(Math.log2(2 ** this.maxLevel - this.realDataRowNum));
                if (level >= this.maxLevel - extraDataLevel) {
                    const curExtraLevel = level - (this.maxLevel - extraDataLevel);
                    const fakeRowNum = 2 ** curExtraLevel;
                    if (end >= 2 ** level - fakeRowNum) {
                        end = 2 ** level - fakeRowNum - 1;
                    }
                }
                if (end < start) {
                    return [];
                }
                return this.levelIndexObjs[level].getDataByIndex(start, end);
            } else {
                return this.levelIndexObjs[level].getDataByIndex(start, end);
            }

        } else {
            throw new Error("cannot get data from data manager");
        }
    }



    checkMonotonicity(nonUniformColObjs: Array<NoUniformColObj>, preIndexArray: Array<number>, needLoadDifNodes: Array<TrendTree>) {
        for (let i = 0; i < needLoadDifNodes.length; i++) {
            const first = needLoadDifNodes[i].yArray[0];
            const minV = needLoadDifNodes[i].yArray[1];
            const maxV = needLoadDifNodes[i].yArray[2];
            const last = needLoadDifNodes[i].yArray[3];
            const pL = needLoadDifNodes[i].level;
            const pTRange = (2 ** this.maxLevel) / (2 ** pL);
            const pTimeS = needLoadDifNodes[i].index * pTRange;
            const pTimeE = pTRange + pTimeS - 1;
            if ((first === minV && last === maxV) || (first === maxV && last === minV)) {
                const k = (last - first) / (pTimeE - pTimeS);
                const b = first - k * pTimeS;
                nonUniformColObjs[preIndexArray[i]].mergeLast(k, b);
                nonUniformColObjs[preIndexArray[i] + 1].mergeFirst(k, b);
                preIndexArray.splice(i, 1);
                needLoadDifNodes.splice(i, 1);
            }
        }
    }
    constructTreeForBatchLoad1(losedRange: Array<Array<number>>, difVals: Array<{ l: number, i: number, dif: Array<number> }>) {
        let count = 0;
        for (let i = 0; i < losedRange.length; i++) {
            const levelRange = losedRange[i];

            const startNode = this.levelIndexObjs[losedRange[i][0]].getTreeNodeStartIndex(losedRange[i][1]);
            let p = startNode;
            const newTreeNode = [];
            for (let j = losedRange[i][1]; j <= losedRange[i][2]; j++) {
                if (p?.index === j && j === difVals[count].i && p.level === difVals[count].l) {
                    //@ts-ignore
                    p.difference = difVals[count].dif;
                    const yArray1: [number, number, number, number] = [0, 0, 0, 0]
                    const yArray2: [number, number, number, number] = [0, 0, 0, 0]
                    yArray1[0] = p.yArray[0];
                    yArray2[0] = p.yArray[0] - p.difference![0];
                    yArray1[3] = p.yArray[3] + p.difference![3];
                    yArray2[3] = p.yArray[3];
                    if (p.difference![1] < 0) {
                        yArray1[1] = p.yArray[1];
                        yArray2[1] = p.yArray[1] - p.difference![1];
                    } else {
                        yArray1[1] = p.yArray[1] + p.difference![1];
                        yArray2[1] = p.yArray[1]
                    }
                    if (p.difference![2] < 0) {
                        yArray1[2] = p.yArray[2] + p.difference![2];
                        yArray2[2] = p.yArray[2];
                    } else {
                        yArray1[2] = p.yArray[2];
                        yArray2[2] = p.yArray[2] - p.difference![2];
                    }
                    const firstNode = new TrendTree(p, true, p.index, yArray1, null);
                    const secondNode = new TrendTree(p, false, p.index, yArray2, null);
                    newTreeNode.push(firstNode);
                    newTreeNode.push(secondNode);
                    p = p.nextSibling;
                    count++;
                    if (p === null || count >= difVals.length) {
                        break;
                    }

                } else {
                    console.log(losedRange[i][0], losedRange[i][1])
                    console.log("lose range:", losedRange, p, p?.index, j);
                    console.log(this.levelIndexObjs);
                    debugger
                    throw new Error("dif not match node");
                }
            }
            for (let j = 0; j < newTreeNode.length - 1; j++) {
                newTreeNode[j].nextSibling = newTreeNode[j + 1];
                newTreeNode[j + 1].previousSibling = newTreeNode[j];
                if (newTreeNode[j].index != newTreeNode[j + 1].index - 1) {
                    throw new Error("sibling index error");
                }
            }
            if (this.levelIndexObjs[losedRange[i][0] + 1]) {
                this.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            } else {
                this.levelIndexObjs[losedRange[i][0] + 1] = new LevelIndexObj(losedRange[i][0] + 1, false);
                this.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            }
        }
    }
    constructTreeForBatchLoad(losedRange: Array<Array<number>>, difVals: Array<{ l: number, i: number, dif: Array<number> }>) {
        let count = 0;
        let nodeNum = 0;
        for (let i = 0; i < losedRange.length; i++) {
            const levelRange = losedRange[i];

            const startNode = this.levelIndexObjs[losedRange[i][0] - 1].getTreeNodeStartIndex(Math.floor(losedRange[i][1] / 2));
            let p = startNode;
            const newTreeNode = [];
            for (let j = Math.floor(losedRange[i][1] / 2); j <= Math.floor(losedRange[i][2] / 2); j++) {
                if (p?.index === j && j === difVals[count].i && p.level === difVals[count].l) {
                    //@ts-ignore
                    p.difference = difVals[count].dif;
                    const yArray1: [number, number, number, number] = [0, 0, 0, 0]
                    const yArray2: [number, number, number, number] = [0, 0, 0, 0]
                    yArray1[0] = p.yArray[0];
                    yArray2[0] = p.yArray[0] - p.difference![0];
                    yArray1[3] = p.yArray[3] + p.difference![3];
                    yArray2[3] = p.yArray[3];
                    if (p.difference![1] < 0) {
                        yArray1[1] = p.yArray[1];
                        yArray2[1] = p.yArray[1] - p.difference![1];
                    } else {
                        yArray1[1] = p.yArray[1] + p.difference![1];
                        yArray2[1] = p.yArray[1]
                    }
                    if (p.difference![2] < 0) {
                        yArray1[2] = p.yArray[2] + p.difference![2];
                        yArray2[2] = p.yArray[2];
                    } else {
                        yArray1[2] = p.yArray[2];
                        yArray2[2] = p.yArray[2] - p.difference![2];
                    }
                    const firstNode = new TrendTree(p, true, p.index, yArray1, null);
                    const secondNode = new TrendTree(p, false, p.index, yArray2, null);
                    this.cacheMap.set(firstNode.index, firstNode);
                    this.cacheMap.set(secondNode.index, secondNode);
                    nodeNum += 2
                    newTreeNode.push(firstNode);
                    newTreeNode.push(secondNode);
                    p = p.nextSibling;
                    count++;
                    if (p === null || count >= difVals.length) {
                        break;
                    }

                } else {
                    console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                    console.log("lose range:", losedRange, p, p?.index, j);
                    console.log(this.levelIndexObjs);
                    debugger
                    throw new Error("dif not match node");
                }
            }
            for (let j = 0; j < newTreeNode.length - 1; j++) {
                newTreeNode[j].nextSibling = newTreeNode[j + 1];
                newTreeNode[j + 1].previousSibling = newTreeNode[j];
                if (newTreeNode[j].index != newTreeNode[j + 1].index - 1) {
                    throw new Error("sibling index error");
                }

            }
            if (this.levelIndexObjs[losedRange[i][0]]) {
                this.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            } else {
                this.levelIndexObjs[losedRange[i][0]] = new LevelIndexObj(losedRange[i][0], false);
                this.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            }
        }
    }

    async viewChangeInteraction(currentLevel: number, width: number, timeRange: Array<number>, yScale: any) {
        // console.time("v_c")
        const nonUniformColObjs = computeTimeSE(currentLevel, width, timeRange, this.realDataRowNum, this.maxLevel, this.dataName);
        //drawViewChangeLineChart({ dataManager:this,data: {maxv:0,minv:0,powRenderData:[],noPowRenderData:[]}, startTime: 0, endTime: timeRange[1], algorithm: "trendtree", width:width, height: 600 })
        //context!.commit("addViewChangeQueryNoPowLineChartObj", { dataManager:this,data: nonUniformColObjs, startTime: 0, endTime: timeRange[1], algorithm: "trendtree", width:width, height: 600 });

        let needLoadDifNode: Array<TrendTree> = [];
        let colIndex = 0;
        for (let i = 0; i < this.levelIndexObjs[currentLevel].firstNodes.length; i++) {
            const firtIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][0], this.maxLevel);
            const lastIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][1], this.maxLevel);
            let p = this.levelIndexObjs[currentLevel].firstNodes[i];
            if (firtIndexTimeRange.startT <= timeRange[0] && lastIndexTimeRange.endT >= timeRange[1]) {
                while (p != null) {
                    if (colIndex >= nonUniformColObjs.length) {
                        break;
                        //throw new Error("col index out range");
                    }
                    const type = nonUniformColObjs[colIndex].isContain(p);
                    if (type === 1) {
                        p = p.nextSibling!;
                    } else if (type === 2) {
                        needLoadDifNode.push(p);
                        p = p.nextSibling!;
                    } else if (type === 3) {
                        colIndex++;
                    } else if (type === 5) {
                        p = p.nextSibling!
                    } else if (type === 6) {
                        //throw new Error("error in viewchange")
                        break;
                    } else {
                        p = p.nextSibling!;
                        //throw new Error("node time is little than col");
                    }
                }
            }

        }
        //const draw=drawViewChangeLineChart({width:600,height:600,minv:0,maxv:1000,x:10,y:10})
        //draw(nonUniformColObjs)
        if (needLoadDifNode.length === 0) {
            //debugger
            return nonUniformColObjs;
        }
        let losedDataInfo = computeLosedDataRange(needLoadDifNode);
        if (losedDataInfo.length === 0) {
            return nonUniformColObjs;
        }
        const currentLevelLosedRange = [];
        for (let j = 0; j < losedDataInfo.length; j++) {
            const level = losedDataInfo[j][0];
            const start = losedDataInfo[j][1];
            const end = losedDataInfo[j][2];
            if (this.levelIndexObjs[level]) {

                const hasData = this.levelIndexObjs[level].hasDataForRange(start, end);
                if (!hasData.has) {
                    const losedRange = hasData.range;
                    losedRange.forEach(range => {
                        range.unshift(level);
                        currentLevelLosedRange.push(range);
                    });
                    //debugger
                }
            } else {
                currentLevelLosedRange.push([level, start, end]);
            }
        }
        if (currentLevelLosedRange.length > 0) {
            await batchLoadDataForRangeLevel(currentLevelLosedRange, this);
        }

        let countNum = 1
        while (needLoadDifNode.length > 0) {
            colIndex = 0;
            const tempNeedLoadDifNodes = [];
            const tempQue: Array<TrendTree> = [];

            needLoadDifNode.forEach(v => {
                if (v._leftChild === null || v._rightChild === null) {
                    debugger
                    throw new Error("cannot find next level node");
                }
                tempQue.push(v._leftChild!);
                tempQue.push(v._rightChild!);
            });
            const preColIndex = [];
            for (let i = 0; i < tempQue.length; i++) {
                if (colIndex >= nonUniformColObjs.length) {
                    //break;
                    throw new Error("col index out range");
                }
                const type = nonUniformColObjs[colIndex].isContain(tempQue[i]);
                if (type === 1) {
                    continue;
                } else if (type === 2) {
                    tempNeedLoadDifNodes.push(tempQue[i]);
                    preColIndex.push(colIndex);
                } else if (type === 3) {
                    colIndex++;
                    i--;
                } else if (type === 6) {
                    break;
                } else {
                    continue;
                    // throw new Error("node time is little than col");
                }
            }
            if (preColIndex.length != tempNeedLoadDifNodes.length) {
                throw new Error("cannot memory index");
            }

            for (let i = 0; i < tempNeedLoadDifNodes.length; i++) {
                const con1 = canCut(tempNeedLoadDifNodes[i], nonUniformColObjs[preColIndex[i]], nonUniformColObjs[preColIndex[i] + 1], yScale);
                if (con1) {
                    tempNeedLoadDifNodes.splice(i, 1)
                    preColIndex.splice(i, 1);
                }
            }
            //this.checkMonotonicity(nonUniformColObjs,preColIndex,tempNeedLoadDifNodes);
            needLoadDifNode = tempNeedLoadDifNodes;
            if (needLoadDifNode.length > 0 && needLoadDifNode[0].level === this.maxLevel - 1) {
                //console.log("last level:",needLoadDifNode.length);
                for (let i = 0; i < needLoadDifNode.length; i++) {

                    nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[0]);
                    nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[0]);
                    nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[3]);
                    nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[3]);
                }
                break;
            }
            if (needLoadDifNode.length === 0) {
                break;
            }
            let losedDataInfo = computeLosedDataRange(needLoadDifNode);
            const currentLevelLosedRange = [];

            for (let j = 0; j < losedDataInfo.length; j++) {
                const level = losedDataInfo[j][0]
                const start = losedDataInfo[j][1]
                const end = losedDataInfo[j][2];
                if (this.levelIndexObjs[level]) {

                    const hasData = this.levelIndexObjs[level].hasDataForRange(start, end);
                    if (!hasData.has) {
                        const losedRange = hasData.range;
                        losedRange.forEach(range => {
                            range.unshift(level);
                            currentLevelLosedRange.push(range);
                        });
                    }
                } else {
                    currentLevelLosedRange.push([level, start, end]);
                }
            }

            await batchLoadDataForRangeLevel(currentLevelLosedRange, this);
        }
        //console.timeEnd("v_c")
        //draw(nonUniformColObjs)
        return nonUniformColObjs;
    }



    async viewChangeInteraction1(currentLevel: number, width: number, timeRange: Array<number>, yScale: any) {
        allTimes = []
        // console.time("v_c")
        const nonUniformColObjs = computeTimeSE(currentLevel, width, timeRange, this.realDataRowNum, this.maxLevel);
        let needLoadDifNode: Array<TrendTree> = [];
        let colIndex = 0;
        for (let i = 0; i < this.levelIndexObjs[currentLevel].firstNodes.length; i++) {
            const firtIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][0], this.maxLevel);
            const lastIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][1], this.maxLevel);
            let p = this.levelIndexObjs[currentLevel].firstNodes[i];
            if (firtIndexTimeRange.startT <= timeRange[0] && lastIndexTimeRange.endT >= timeRange[1]) {
                while (p != null) {
                    if (colIndex >= nonUniformColObjs.length) {
                        break;
                        //throw new Error("col index out range");
                    }
                    const type = nonUniformColObjs[colIndex].isContain(p);
                    if (type === 1) {
                        p = p.nextSibling!;
                    } else if (type === 2) {
                        needLoadDifNode.push(p);
                        p = p.nextSibling!;
                    } else if (type === 3) {
                        colIndex++;
                    } else if (type === 5) {
                        p = p.nextSibling!
                    } else if (type === 6) {
                        // throw new Error("error in viewchange")
                        break;
                    } else {
                        p = p.nextSibling!;
                        //throw new Error("node time is little than col");
                    }
                }
            }

        }
        if (needLoadDifNode.length === 0) {
            const sumTime = allTimes.reduce((pre: any, cur: any) => pre + cur, 0)
            allSumTimes.push(sumTime)
            allTimes = [];
            if (allSumTimes.length >= 50) {
                console.log(allSumTimes)
            }
            //debugger
            return nonUniformColObjs;
        }
        let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);
        //console.log(losedDataInfo)
        // if (losedDataInfo.length === 0) {
        //     return nonUniformColObjs;
        // }

        if (losedDataInfo.length > 0) {
            const startTime = new Date().getTime()
            await batchLoadDataForRangeLevel1(losedDataInfo, this);
            allTimes.push(new Date().getTime() - startTime)
        }


        while (needLoadDifNode.length > 0) {
            colIndex = 0;
            const tempNeedLoadDifNodes = [];
            const tempQue: Array<TrendTree> = [];

            needLoadDifNode.forEach(v => {
                if (v._leftChild === null || v._rightChild === null) {
                    console.log(v)
                    console.log(this)
                    debugger
                    throw new Error("cannot find next level node");
                }
                tempQue.push(v._leftChild!);
                tempQue.push(v._rightChild!);
            });
            const preColIndex = [];
            for (let i = 0; i < tempQue.length; i++) {
                if (colIndex >= nonUniformColObjs.length) {
                    break;
                    //throw new Error("col index out range");
                }
                const type = nonUniformColObjs[colIndex].isContain(tempQue[i]);
                if (type === 1) {
                    continue;
                } else if (type === 2) {
                    tempNeedLoadDifNodes.push(tempQue[i]);
                    preColIndex.push(colIndex);
                } else if (type === 3) {
                    colIndex++;
                    i--;
                } else if (type === 6) {
                    break;
                } else {
                    continue;
                    // throw new Error("node time is little than col");
                }
            }
            if (preColIndex.length != tempNeedLoadDifNodes.length) {
                throw new Error("cannot memory index");
            }

            for (let i = 0; i < tempNeedLoadDifNodes.length; i++) {
                if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                    const con1 = canCut(tempNeedLoadDifNodes[i], nonUniformColObjs[preColIndex[i]], nonUniformColObjs[preColIndex[i] + 1], yScale);
                    if (con1) {
                        tempNeedLoadDifNodes.splice(i, 1)
                        preColIndex.splice(i, 1);
                    }
                }

            }
            //this.checkMonotonicity(nonUniformColObjs,preColIndex,tempNeedLoadDifNodes);
            needLoadDifNode = tempNeedLoadDifNodes;
            if (needLoadDifNode.length > 0 && needLoadDifNode[0].level === this.maxLevel - 1) {
                //console.log("last level:",needLoadDifNode.length);
                for (let i = 0; i < needLoadDifNode.length; i++) {

                    nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[0]);
                    nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[0]);

                    if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                        nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[3]);
                        nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[3]);
                    }

                }
                break;
            }
            if (needLoadDifNode.length === 0) {
                break;
            }
            let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);
            if (losedDataInfo.length > 0) {
                const startTime = new Date().getTime()
                await batchLoadDataForRangeLevel1(losedDataInfo, this);
                allTimes.push(new Date().getTime() - startTime)
            }

        }
        const sumTime = allTimes.reduce((pre: any, cur: any) => pre + cur, 0)
        // console.log(sumTime)
        //allSumTimes.push(sumTime)
        //allTimes = [];
        //if(allSumTimes.length>=50){
        //    console.log(allSumTimes)
        // }
        //console.timeEnd("v_c")
        return nonUniformColObjs;
    }








    checkLoadedDataInSingalLevel(losedDataInfo: Array<Array<number>>) {
        const currentLevelLosedRange = [];
        for (let j = 0; j < losedDataInfo.length; j++) {
            const level = losedDataInfo[j][0]
            const start = losedDataInfo[j][1]
            const end = losedDataInfo[j][2];
            if (this.levelIndexObjs[level]) {

                const hasData = this.levelIndexObjs[level].hasDataForRange(start, end);
                if (!hasData.has) {
                    const losedRange = hasData.range;
                    losedRange.forEach(range => {
                        range.unshift(level);
                        currentLevelLosedRange.push(range);
                    });
                }
            } else {
                currentLevelLosedRange.push([level, start, end]);
            }
        }
        return currentLevelLosedRange
    }
    checkLoadedDataInSingalLevelV1(losedDataInfo: Array<Array<number>>) {
        const currentLevelLosedRange = [];
        for (let j = 0; j < losedDataInfo.length; j++) {
            const level = losedDataInfo[j][0]
            const start = losedDataInfo[j][1]
            const end = losedDataInfo[j][2];
            if (this.levelIndexObjs[level]) {

                const hasData = this.levelIndexObjs[level].hasDataForRange(start, end);
                if (!hasData.has) {
                    const losedRange = hasData.range;
                    losedRange.forEach(range => {
                        range.unshift(level);
                        currentLevelLosedRange.push(range);
                    });
                }
            } else {
                currentLevelLosedRange.push([level, start, end]);
            }
        }
        return currentLevelLosedRange
    }



    getIndexTime(l: number, index: number, maxLevel: number) {
        const gap = 2 ** maxLevel / (2 ** l)
        const startTime = index * gap;
        const endTime = startTime + gap - 1;
        return {
            startT: startTime,
            endT: endTime
        }
    }

    initCache() {
        const options = {
            max: 1000000,
            maxSize: 1000000,
            sizeCalculation: (value: any, key: string) => {
                return 1
            },
            dispose: (value: any, key: string, reason: string) => {
                if (reason === "evict") {
                    console.log(key)
                    this.deleteQueue.push(value);
                }

            },
            disposeAfter: (value: any, key: string, reason: string) => {
                //console.log("dispose after----------------------",key,reason)
            },
            ttl: 1000 * 60 * 60,
            allowStale: false,
            updateAgeOnGet: true,
            updateAgeOnHas: true,

        }
        this.lruCache = new Cache(options);
        this.evictTreeNode()
        return
    }



    evictTreeNode() {
        setInterval(() => {
            if (this.isIntering) {
                return
            }
            this.isEvicting = true
            this.deleteQueue.forEach((curNode) => {
                const l = curNode.level;
                const i = curNode.index;
                this.lruCache.delete(l + "_" + i);
                if (this.levelIndexObjs[l].hasDataForRange(i, i).has) {
                    this.deleteNodeWhithChild(l, i);
                    console.log("evict l:" + l + " i:" + i + " finish!")
                }
            })
            this.isEvicting = false
        }, 1000)

    }
    deleteNodeWhithChild(level: number, index: number) {

        let curLS = index;
        let curRS = index;
        for (let j = level; j < this.maxLevel - 1; j++) {
            if (j == level) {
                curLS = index;
                curRS = index;
            } else {
                curLS = 2 ** (j - level) * curLS;
                curRS = 2 ** (j - level) * curRS + 1;
            }
            const curLLoadedRange = this.levelIndexObjs[j].loadedDataRange;
            const curFirstNodes = this.levelIndexObjs[j].firstNodes;

            if (!curLLoadedRange || !curFirstNodes) {
                break;
            }

            const newFirstNodes: Array<TrendTree> = [];
            const newLoadDataRanges = [];
            for (let i = 0; i < curLLoadedRange.length; i++) {
                const curRange0 = curLLoadedRange[i];
                if (curRange0[1] < curRS) {
                    newFirstNodes.push(curFirstNodes[i]);
                    newLoadDataRanges.push([curRange0[0], curRange0[1]]);
                } else if (curRange0[0] < curLS && curRange0[1] === curLS) {
                    if (curFirstNodes[i].nextSibling) {
                        newLoadDataRanges.push([curRange0[0], curRange0[1] - 1]);
                        newFirstNodes.push(curFirstNodes[i])
                    }
                    let curP = curFirstNodes[i];
                    while (curP) {
                        if (curP.index === curLS) {
                            if (curP.previousSibling) {
                                curP.previousSibling.nextSibling = null
                                curP.previousSibling = null
                            }
                            break;
                        }
                        //@ts-ignore
                        curP = curP.nextSibling
                    }
                    if (curP === null) {
                        throw new Error("cannot find range node");
                    }
                } else if (curRange0[0] < curLS && curRange0[1] > curLS && curRange0[1] < curRS) {
                    newFirstNodes.push(curFirstNodes[i]);
                    newLoadDataRanges.push([curRange0[0], curLS - 1]);
                    let curP = curFirstNodes[i];
                    while (curP) {
                        if (curP.index === curLS) {
                            if (curP.previousSibling) {
                                curP.previousSibling.nextSibling = null
                                curP.previousSibling = null
                            }
                            break;
                        }
                        //@ts-ignore
                        curP = curP.nextSibling
                    }
                    if (curP === null) {
                        throw new Error("cannot find range node");
                    }
                } else if (curRange0[0] < curLS && curRange0[1] === curRS) {
                    newFirstNodes.push(curFirstNodes[i]);
                    newLoadDataRanges.push([curRange0[0], curLS - 1]);
                    let curP = curFirstNodes[i];
                    while (curP) {
                        if (curP.index === curLS) {
                            if (curP.previousSibling) {
                                curP.previousSibling.nextSibling = null;
                                curP.previousSibling = null;
                            }
                            break;
                        }
                        //@ts-ignore
                        curP = curP.nextSibling
                    }
                    if (curP === null) {
                        throw new Error("cannot find range node");
                    }

                } else if (curRange0[0] < curLS && curRange0[1] > curRS) {
                    //debugger
                    newFirstNodes.push(curFirstNodes[i]);
                    newLoadDataRanges.push([curRange0[0], curLS - 1])

                    let curP = curFirstNodes[i];
                    while (curP) {
                        if (curP.index === curLS) {
                            if (curP.previousSibling) {
                                curP.previousSibling.nextSibling = null
                                curP.previousSibling = null;
                            }
                        }
                        if (curP.index === curRS) {
                            if (curP.nextSibling) {
                                newFirstNodes.push(curP.nextSibling);
                                newLoadDataRanges.push([curRS + 1, curRange0[1]]);
                                curP.nextSibling.previousSibling = null;
                                curP.nextSibling = null
                            }
                            break;
                        }
                        //@ts-ignore
                        curP = curP.nextSibling
                    }
                    if (curP === null) {
                        throw new Error("cannot find range node");
                    }

                } else if (curRange0[0] === curLS && curRange0[1] > curRS) {
                    let curP = curFirstNodes[i];
                    while (curP) {
                        if (curP.index === curRS) {
                            if (curP.nextSibling) {
                                newFirstNodes.push(curP.nextSibling);
                                newLoadDataRanges.push([curRS + 1, curRange0[1]]);
                                curP.nextSibling.previousSibling = null;
                                curP.nextSibling = null
                            }
                            break;
                        }
                        //@ts-ignore
                        curP = curP.nextSibling
                    }
                    if (curP === null) {
                        throw new Error("cannot find range node");
                    }

                } else if (curRange0[0] === curLS && curRange0[1] === curRS) {
                    continue;
                } else if (curRange0[0] === curLS && curRange0[1] < curRS) {
                    continue
                } else if (curRange0[0] > curLS && curRange0[1] > curRS && curRange0[0] < curRS) {
                    let curP = curFirstNodes[i];
                    while (curP) {
                        if (curP.index === curRS) {
                            if (curP.nextSibling) {
                                newFirstNodes.push(curP.nextSibling);
                                newLoadDataRanges.push([curRS + 1, curRange0[1]]);
                                curP.nextSibling.previousSibling = null;
                                curP.nextSibling = null;
                            }
                            break;
                        }
                        //@ts-ignore
                        curP = curP.nextSibling
                    }
                    if (curP === null) {
                        throw new Error("cannot find range node");
                    }
                } else if (curRange0[0] > curLS && curRange0[0] === curRS) {
                    if (curFirstNodes[i].nextSibling) {
                        newFirstNodes.push(curFirstNodes[i].nextSibling!)
                        newLoadDataRanges.push([curRange0[0] + 1, curRange0[1]])
                        curFirstNodes[i]!.nextSibling!.previousSibling = null
                        curFirstNodes[i].nextSibling = null

                    }

                } else if (curRange0[0] > curRS) {
                    newFirstNodes.push(curFirstNodes[i])
                    newLoadDataRanges.push([curRange0[0], curRange0[1]])
                } else if (curRange0[0] > curLS && curRange0[1] === curRS) {
                    continue
                }
                else {
                    console.log(curRange0, curLS, curRS)
                    throw new Error("range error");

                }
            }

            this.levelIndexObjs[j].loadedDataRange = newLoadDataRanges;
            this.levelIndexObjs[j].firstNodes = newFirstNodes;
        }
    }
    updateMaxCacheSize(size: number) {
        this.maxCacheNodeNum = size;
    }
    lruCacheDelete() {
        console.log(this.cacheMap.size)
        if (this.cacheMap.size <= this.maxCacheNodeNum) {

            return
        }
        let needDeleteNum = this.cacheMap.size - this.maxCacheNodeNum
        if (needDeleteNum % 2 != 0) {
            needDeleteNum++
        }
        console.log("delete nums:", needDeleteNum)
        for (let i = this.levelIndexObjs.length - 2; i > 0; i--) {
            if (needDeleteNum <= 0) {
                break
            }
            const levelObj = this.levelIndexObjs[i];
            for (let j = 0; j < levelObj.firstNodes.length; j++) {
                let firstNode = levelObj.firstNodes[j];
                this.levelIndexObjs[firstNode.level + 1].loadedDataRange = [];
                this.levelIndexObjs[firstNode.level + 1].firstNodes = [];
                this.levelIndexObjs[firstNode.level + 1].isFull = false;
                while (firstNode) {
                    if (firstNode._leftChild) {
                        this.cacheMap.delete(firstNode._leftChild.index)
                    }
                    if (firstNode._rightChild) {
                        this.cacheMap.delete(firstNode._rightChild.index)
                    }
                    //@ts-ignore
                    firstNode = firstNode.nextSibling
                    needDeleteNum -= 2;
                }
            }
        }
    }







    async computePowCon(currentLevel: number, width: number, gapNeedNodes: Array<TrendTree>, nonUniformColObjs: Array<NoUniformColObj>) {
        //debugger
        console.log("pwo:", width)
        const times = [];
        let needLoadDifNode: Array<TrendTree> = [];
        if (gapNeedNodes.length % width !== 0) {
            throw new Error("gap node not match widht")
        }
        const timeNum = gapNeedNodes.length / width;
        const groups = new Array<Array<TrendTree>>(width)
        for (let i = 0; i < groups.length; i++) {
            groups[i] = []
        }
        if (timeNum > 1) {
            for (let i = 0; i < gapNeedNodes.length; i++) {
                const groupIdx = Math.floor(i / timeNum)
                groups[groupIdx].push(gapNeedNodes[i])
            }
            for (let i = 0; i < groups.length - 1; i++) {
                const group = groups[i];
                const secGroup = groups[i + 1]
                needLoadDifNode.push(group[group.length - 1])
                needLoadDifNode.push(secGroup[0])
            }
        } else {
            needLoadDifNode = gapNeedNodes;
        }


        let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);


        if (losedDataInfo.length > 0) {
            const levelTime = new Date().getTime()
            await batchLoadDataForRangeLevel1(losedDataInfo, this);
            times.push(new Date().getTime() - levelTime);
        } else {
            times.push(1)
        }
        //debugger
        let columnGapArray: Array<UniformGapObj> = [];
        if (timeNum == 1) {
            for (let i = 0; i < needLoadDifNode.length - 1; i++) {
                // debugger
                if (needLoadDifNode[i]._rightChild && needLoadDifNode[i + 1]._leftChild) {
                    const columnGap = new UniformGapObj(needLoadDifNode[i], needLoadDifNode[i + 1])
                    columnGapArray.push(columnGap);
                } else {
                    throw new Error("children cannot empty")
                }
            }
        } else {
            for (let i = 0; i < needLoadDifNode.length; i += 2) {
                if (needLoadDifNode[i]._rightChild && needLoadDifNode[i + 1]._leftChild) {
                    const columnGap = new UniformGapObj(needLoadDifNode[i], needLoadDifNode[i + 1])
                    columnGapArray.push(columnGap);
                } else {
                    throw new Error("children cannot empty")
                }
            }
            for (let i = 0; i < groups.length - 1; i++) {
                const group1 = groups[i];
                const group2 = groups[i + 1];
                for (let j = 0; j < group1.length - 1; j++) {
                    columnGapArray[i].updateLeftMinMax(group1[j])
                }
                for (let j = 1; j < group2.length; j++) {
                    columnGapArray[i].updateRighetMinMax(group2[j])
                }
            }
        }
        let con = true
        while (con) {
            //debugger
            let isFinish = false
            needLoadDifNode = []
            for (let i = 0; i < columnGapArray.length; i++) {
                const isCut = columnGapArray[i].canCut()
                isFinish = isCut && isFinish
                if (!isCut) {
                    needLoadDifNode.push(columnGapArray[i].firstNode);
                    needLoadDifNode.push(columnGapArray[i].secondNode);
                }
            }
            if (isFinish) {
                break;
            }
            if (needLoadDifNode.length == 0) {
                break
            }
            if (needLoadDifNode.length > 0 && needLoadDifNode[0].level === this.maxLevel - 1) {
                for (let i = 0; i < columnGapArray.length; i++) {
                    const colGap = columnGapArray[i];
                    if (!colGap.isOk) {
                        if (colGap.firstNode && colGap.secondNode) {
                            colGap.lastLevelUpdateMinMax()
                        }
                    }
                }
                break;
            }
            losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);
            if (losedDataInfo.length > 0) {
                const levelTime = new Date().getTime()
                await batchLoadDataForRangeLevel1(losedDataInfo, this);
                times.push(new Date().getTime() - levelTime);
            }
            for (let i = 0; i < columnGapArray.length; i++) {
                const colGap = columnGapArray[i];
                if (!colGap.isOk) {
                    if (colGap.firstNode._rightChild && colGap.secondNode._leftChild) {
                        colGap.updateTwoNode(colGap.firstNode, colGap.secondNode);
                    }
                }
            }
        }
        if (nonUniformColObjs.length !== columnGapArray.length + 1) {
            throw new Error("column not match gap")
        }
        for (let i = 0; i < columnGapArray.length; i++) {
            const colGap = columnGapArray[i];
            if (colGap.canUseT) {
                nonUniformColObjs[i].endV = colGap.tOne;
                nonUniformColObjs[i].startV = colGap.tOne;
            }
        }

        if (times.length >= 3) {
            allTimes.push(times[0])
            //allTimes.push(times[1])
        }
        console.log(allTimes)
        console.log(times)
        return nonUniformColObjs

    }











    bfsSearchTree(currentLevel: number, width: number, timeRange: Array<number>, nonUniformColObjs: Array<NoUniformColObj>) {
        let needLoadDifNode: Array<TrendTree> = [];
        let colIndex = 0;
        for (let i = 0; i < this.levelIndexObjs[currentLevel].firstNodes.length; i++) {
            const firtIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][0], this.maxLevel);
            const lastIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][1], this.maxLevel);
            let p = this.levelIndexObjs[currentLevel].firstNodes[i];
            if (firtIndexTimeRange.startT <= timeRange[0] && lastIndexTimeRange.endT >= timeRange[1]) {
                while (p != null) {
                    if (colIndex >= nonUniformColObjs.length) {
                        break;
                        //throw new Error("col index out range");
                    }
                    const type = nonUniformColObjs[colIndex].isContain(p);
                    if (type === 1) {
                        p = p.nextSibling!;
                    } else if (type === 2) {
                        needLoadDifNode.push(p);
                        p = p.nextSibling!;
                    } else if (type === 3) {
                        colIndex++;
                    } else if (type === 5) {
                        p = p.nextSibling!
                    } else if (type === 6) {
                        throw new Error("error in viewchange")
                        break;
                    } else {
                        p = p.nextSibling!;
                        //throw new Error("node time is little than col");
                    }
                }
            }

        }
        if (needLoadDifNode.length === 0) {
            //debugger
            return nonUniformColObjs;
        }

        while (needLoadDifNode.length > 0) {
            colIndex = 0;
            const tempNeedLoadDifNodes = [];
            const tempQue: Array<TrendTree> = [];

            needLoadDifNode.forEach(v => {
                if (v._leftChild === null || v._rightChild === null) {
                    console.log(v)
                    console.log(this)
                    debugger
                    throw new Error("cannot find next level node");
                }
                tempQue.push(v._leftChild!);
                tempQue.push(v._rightChild!);
            });
            const preColIndex = [];
            for (let i = 0; i < tempQue.length; i++) {
                if (colIndex >= nonUniformColObjs.length) {
                    break;
                    //throw new Error("col index out range");
                }
                const type = nonUniformColObjs[colIndex].isContain(tempQue[i]);
                if (type === 1) {
                    continue;
                } else if (type === 2) {
                    tempNeedLoadDifNodes.push(tempQue[i]);
                    preColIndex.push(colIndex);
                } else if (type === 3) {
                    colIndex++;
                    i--;
                } else if (type === 6) {
                    break;
                } else {
                    continue;
                    // throw new Error("node time is little than col");
                }
            }
            if (preColIndex.length != tempNeedLoadDifNodes.length) {
                throw new Error("cannot memory index");
            }

            for (let i = 0; i < tempNeedLoadDifNodes.length; i++) {
                if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                    const con1 = canCut(tempNeedLoadDifNodes[i], nonUniformColObjs[preColIndex[i]], nonUniformColObjs[preColIndex[i] + 1], '');
                    if (con1) {
                        tempNeedLoadDifNodes.splice(i, 1)
                        preColIndex.splice(i, 1);
                    }
                }

            }
            needLoadDifNode = tempNeedLoadDifNodes;
            if (needLoadDifNode.length > 0 && needLoadDifNode[0].level === this.maxLevel - 1) {
                //console.log("last level:",needLoadDifNode.length);
                for (let i = 0; i < needLoadDifNode.length; i++) {

                    nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[0]);
                    nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[0]);

                    if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                        nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[3]);
                        nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[3]);
                    }
                }
                break;
            }
            if (needLoadDifNode.length === 0) {
                break;
            }
        }
        return nonUniformColObjs;
    }

    async viewChangeInteraction1WS(currentLevel: number, width: number, timeRange: Array<number>, yScale: any, props?: any) {
        // while(this.isEvicting){console.log()};
        this.isIntering = true
        const tagName = "" + Math.random() + "" + Math.random()
        allTimes = []
        // console.time("v_c")
        const nonUniformColObjs = computeTimeSE(currentLevel, width, timeRange, this.realDataRowNum, this.maxLevel);
        let needLoadDifNode: Array<TrendTree> = [];
        let colIndex = 0;
        for (let i = 0; i < this.levelIndexObjs[currentLevel].firstNodes.length; i++) {
            const firtIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][0], this.maxLevel);
            const lastIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][1], this.maxLevel);
            let p = this.levelIndexObjs[currentLevel].firstNodes[i];
            if (firtIndexTimeRange.startT <= timeRange[0] && lastIndexTimeRange.endT >= timeRange[1]) {
                while (p != null) {
                    if (colIndex >= nonUniformColObjs.length) {
                        break;
                        //throw new Error("col index out range");
                    }
                    const type = nonUniformColObjs[colIndex].isContain(p);
                    if (type === 1) {
                        p = p.nextSibling!;
                    } else if (type === 2) {
                        needLoadDifNode.push(p);
                        p = p.nextSibling!;
                    } else if (type === 3) {
                        colIndex++;
                    } else if (type === 5) {
                        p = p.nextSibling!
                    } else if (type === 6) {
                        throw new Error("error in viewchange")
                        break;
                    } else {
                        p = p.nextSibling!;
                        //throw new Error("node time is little than col");
                    }
                }
            }

        }
        if (needLoadDifNode.length === 0) {
            const sumTime = allTimes.reduce((pre: any, cur: any) => pre + cur, 0)
            allSumTimes.push(sumTime)
            await batchLoadDataForRangeLevel1WS([], this, "empty");
            allTimes = [];
            if (allSumTimes.length >= 50) {
                console.log(allSumTimes)
            }
            //debugger
            return nonUniformColObjs;
        }
        let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);


        if (losedDataInfo.length > 0) {
            const startTime = new Date().getTime()
            await batchLoadDataForRangeLevel1WS(losedDataInfo, this, tagName);
            allTimes.push(new Date().getTime() - startTime)
        }


        while (needLoadDifNode.length > 0) {
            colIndex = 0;
            const tempNeedLoadDifNodes = [];
            const tempQue: Array<TrendTree> = [];

            needLoadDifNode.forEach(v => {
                if (v._leftChild === null || v._rightChild === null) {
                    debugger
                    throw new Error("cannot find next level node");
                }
                tempQue.push(v._leftChild!);
                tempQue.push(v._rightChild!);
                const leftHas = this.lruCache.has(v._leftChild.level + "_" + v._leftChild.index);
                const rightHas = this.lruCache.has(v._rightChild.level + "_" + v._rightChild.index);
                // if(!leftHas){
                //     console.log("fddddd",v._leftChild.level)
                // }

            });
            const preColIndex = [];
            for (let i = 0; i < tempQue.length; i++) {
                if (colIndex >= nonUniformColObjs.length) {
                    break;
                    //throw new Error("col index out range");
                }
                const type = nonUniformColObjs[colIndex].isContain(tempQue[i]);
                if (type === 1) {
                    continue;
                } else if (type === 2) {
                    tempNeedLoadDifNodes.push(tempQue[i]);
                    preColIndex.push(colIndex);
                } else if (type === 3) {
                    colIndex++;
                    i--;
                } else if (type === 6) {
                    break;
                } else {
                    continue;
                    // throw new Error("node time is little than col");
                }
            }
            if (preColIndex.length != tempNeedLoadDifNodes.length) {
                throw new Error("cannot memory index");
            }

            for (let i = 0; i < tempNeedLoadDifNodes.length; i++) {
                if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                    const con1 = canCut(tempNeedLoadDifNodes[i], nonUniformColObjs[preColIndex[i]], nonUniformColObjs[preColIndex[i] + 1], yScale);
                    if (con1) {
                        tempNeedLoadDifNodes.splice(i, 1)
                        preColIndex.splice(i, 1);
                    }
                }

            }
            //this.checkMonotonicity(nonUniformColObjs,preColIndex,tempNeedLoadDifNodes);
            needLoadDifNode = tempNeedLoadDifNodes;
            if (needLoadDifNode.length > 0 && needLoadDifNode[0].level === this.maxLevel - 2) {
                //console.log("last level:",needLoadDifNode.length);
                for (let i = 0; i < needLoadDifNode.length; i++) {

                    nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[0]);
                    nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[0]);

                    if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                        nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[3]);
                        nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[3]);
                    }

                }
                break;
            }
            if (needLoadDifNode.length === 0) {
                break;
            }
            let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);
            if (losedDataInfo.length > 0) {
                const startTime = new Date().getTime()
                await batchLoadDataForRangeLevel1WS(losedDataInfo, this, tagName);
                allTimes.push(new Date().getTime() - startTime)
            }

        }
        if (props && props.rootT) {
            console.log("roott:", props.rootT)
            allTimes.push(props.rootT);
        }
        //console.log(allTimes)
        const sumTime = allTimes.reduce((pre: any, cur: any) => pre + cur, 0)
        allSumTimes.push(sumTime);
        if (allSumTimes.length === 6) {
            pushTimeArray(allSumTimes);
            allSumTimes = []
        }

        console.log("AllLevelTime:", allSumTimes);
        await batchLoadDataForRangeLevel1WS(losedDataInfo, this, "empty");
        this.isIntering = false;
        return nonUniformColObjs;
    }







    async viewChangeInteractionFinal(currentLevel: number, width: number, timeRange: Array<number>, yScale: any) {
        console.log(currentLevel, width, timeRange)
        const dataName = this.dataName.includes(".") ? this.dataName.split(".")[1] : this.dataName;
        const currentFlagInfo = store.state.allFlags[this.dataName];
        if (currentFlagInfo === undefined) {
            throw new Error(this.dataName + "get flagx faild")
        } else {
            console.log("flag length:", currentFlagInfo.length)
        }

        allTimes = []
        // console.time("v_c")
        const nonUniformColObjs = computeTimeSE(currentLevel, width, timeRange, this.realDataRowNum, this.maxLevel);
        let needLoadDifNode: Array<TrendTree> = [];
        let colIndex = 0;

        for (let i = 0; i < this.levelIndexObjs[currentLevel].firstNodes.length; i++) {
            const firtIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][0], this.maxLevel);
            const lastIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][1], this.maxLevel);
            let p = this.levelIndexObjs[currentLevel].firstNodes[i];

            if (firtIndexTimeRange.startT <= timeRange[0] && lastIndexTimeRange.endT >= timeRange[1]) {
                while (p != null) {
                    if (colIndex >= nonUniformColObjs.length) {
                        break;
                        //throw new Error("col index out range");
                    }
                    const type = nonUniformColObjs[colIndex].isMissContain(p);
                    nonUniformColObjs[colIndex].containColumnRange(p, type);
                    if (type === 1) {
                        p = p.nextSibling!;
                    } else if (type === 2) {
                        needLoadDifNode.push(p);
                        p = p.nextSibling!;
                    } else if (type === 3) {
                        colIndex++;
                    } else if (type === 5) {
                        p = p.nextSibling!
                    } else if (type === 6) {
                        throw new Error("error in viewchange")
                        break;
                    } else {
                        p = p.nextSibling!;
                        //throw new Error("node time is little than col");
                    }
                }
            }

        }

        if (needLoadDifNode.length === 0) {
            return nonUniformColObjs;
        }

        let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);


        if (losedDataInfo.length > 0) {

            await batchLoadDataForRangeLevel1MinMaxMiss(losedDataInfo, this);
        }


        while (needLoadDifNode.length > 0) {
            colIndex = 0;
            const tempNeedLoadDifNodes = [];
            const tempQue: Array<TrendTree> = [];

            needLoadDifNode.forEach(v => {
                if (v._leftChild === null || v._rightChild === null) {
                    console.log(v)
                    console.log(this)
                    debugger
                    throw new Error("cannot find next level node");
                }
                this.lruCache.has(v._leftChild.level + "_" + v._leftChild.index);
                this.lruCache.has(v._rightChild.level + "_" + v._rightChild.index);
                if (v._leftChild.nodeType !== 'NULL') {
                    tempQue.push(v._leftChild!);
                }
                if (v._rightChild.nodeType !== 'NULL') {
                    tempQue.push(v._rightChild!);
                }
            });

            const preColIndex = [];
            for (let i = 0; i < tempQue.length; i++) {
                if (colIndex >= nonUniformColObjs.length) {
                    break;
                    //throw new Error("col index out range");
                }
                const type = nonUniformColObjs[colIndex].isMissContain(tempQue[i]);
                nonUniformColObjs[colIndex].containColumnRange(tempQue[i], type);
                if (type === 1) {
                    continue;
                } else if (type === 2) {
                    tempNeedLoadDifNodes.push(tempQue[i]);
                    preColIndex.push(colIndex);
                } else if (type === 3) {
                    colIndex++;
                    i--;
                } else if (type === 6) {
                    break;
                } else {
                    continue;
                    // throw new Error("node time is little than col");
                }
            }
            if (preColIndex.length != tempNeedLoadDifNodes.length) {
                throw new Error("cannot memory index");
            }

            for (let i = 0; i < tempNeedLoadDifNodes.length; i++) {
                if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                    const con1 = canCut(tempNeedLoadDifNodes[i], nonUniformColObjs[preColIndex[i]], nonUniformColObjs[preColIndex[i] + 1], yScale);
                    if (con1) {
                        tempNeedLoadDifNodes.splice(i, 1)
                        preColIndex.splice(i, 1);
                    }
                }

            }
            //this.checkMonotonicity(nonUniformColObjs,preColIndex,tempNeedLoadDifNodes);
            needLoadDifNode = tempNeedLoadDifNodes;

            if (needLoadDifNode.length > 0 && needLoadDifNode[0].level === this.maxLevel - 1) {

                console.log("last level:", needLoadDifNode.length);

                for (let i = 0; i < needLoadDifNode.length; i++) {
                    const nodeFlag1 = currentFlagInfo[2 * needLoadDifNode[i].index];
                    if (nodeFlag1 === 1) {
                        throw new Error("flag error")
                    }
                    const nodeFlag2 = currentFlagInfo[2 * needLoadDifNode[i].index + 1]
                    if (nodeFlag2 === 0) {
                        nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[1]);
                        nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[1]);
                        if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                            nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[2]);
                            nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[2]);
                        }
                    } else {
                        nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[2]);
                        nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[2]);
                        if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                            nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[1]);
                            nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[1]);
                        }
                    }

                }
                break;
            }
            if (needLoadDifNode.length === 0) {
                break;
            }
            let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);
            if (losedDataInfo.length > 0) {
                await batchLoadDataForRangeLevel1MinMaxMiss(losedDataInfo, this);
            }

        }
        for (let i = 0; i < nonUniformColObjs.length; i++) {
            nonUniformColObjs[i].checkIsMis();
        }
        return nonUniformColObjs;
    }



    async viewChangeInteractionFinal1(currentLevel: number, width: number, timeRange: Array<number>, yScale: any, drawer: any) {
        
        const currentFlagInfo = getFlag(this.dataName);
        if (currentFlagInfo === undefined) {
            throw new Error(this.dataName + " get flag faild")
        } else {
            //console.log("flag length:", currentFlagInfo.length)
        }

        allTimes = []
        // console.time("v_c")
        const nonUniformColObjs = computeTimeSE(currentLevel, width, timeRange, this.realDataRowNum, this.maxLevel);
        let needLoadDifNode: Array<TrendTree> = [];
        let colIndex = 0;
        for (let i = 0; i < this.levelIndexObjs[currentLevel].firstNodes.length; i++) {
            const firtIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][0], this.maxLevel);
            const lastIndexTimeRange = this.getIndexTime(currentLevel, this.levelIndexObjs[currentLevel].loadedDataRange[i][1], this.maxLevel);
            let p = this.levelIndexObjs[currentLevel].firstNodes[i];

            if (firtIndexTimeRange.startT <= timeRange[0] && lastIndexTimeRange.endT >= timeRange[1]) {
                while (p != null) {
                    if (colIndex >= nonUniformColObjs.length) {
                        break;
                        //throw new Error("col index out range");
                    }
                    const type = nonUniformColObjs[colIndex].isMissContain(p);
                    nonUniformColObjs[colIndex].containColumnRange(p, type);
                    if (type === 1) {
                        p = p.nextSibling!;
                    } else if (type === 2 || type === 7 || type === 8 || type === 9 || type === 10) {

                        if (type === 2) {
                            needLoadDifNode.push(p);
                        }
                        if (type === 7) {
                            needLoadDifNode.push(p);
                            colIndex++;
                        }
                        if (type === 8) {
                            if (colIndex != 0) {
                                needLoadDifNode.push(p);
                            }
                        }
                        if (type === 9) {
                            colIndex++;
                            if (colIndex !== nonUniformColObjs.length - 1) {
                                needLoadDifNode.push(p);
                            }
                        }
                        if (type === 10) {
                            needLoadDifNode.push(p);
                            //preColIndex.push(colIndex);
                        }
                        p = p.nextSibling!;
                    } else if (type === 3) {
                        colIndex++;
                    } else if (type === 5) {
                      
                        p = p.nextSibling!
                    } else if (type === 6) {
                        
                        break;
                    } else {
                      
                        p = p.nextSibling!;
                        //throw new Error("node time is little than col");
                    }
                }
            }else{
                debugger
            }

        }
        // debugger
        if (needLoadDifNode.length === 0) {
            return nonUniformColObjs;
        }

        let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);

        // debugger
        if (losedDataInfo.length > 0) {

            await batchLoadDataForRangeLevel1MinMaxMiss(losedDataInfo, this);
        }


        while (needLoadDifNode.length > 0) {
            colIndex = 0;
            const tempNeedLoadDifNodes = [];
            const tempQue: Array<TrendTree> = [];

            needLoadDifNode.forEach(v => {
                if ((v._leftChild === null || v._rightChild === null) && v.nodeType === 'O') {
                    debugger
                    throw new Error("cannot find next level node");
                }
                if (v.nodeType === 'NULL') {
                    //
                } else {
                    this.lruCache.has(v._leftChild!.level + "_" + v._leftChild!.index);
                    this.lruCache.has(v._rightChild!.level + "_" + v._rightChild!.index);
                    if (v._leftChild!.nodeType !== 'NULL') {
                        tempQue.push(v._leftChild!);
                    }
                    if (v._rightChild!.nodeType !== 'NULL') {
                        tempQue.push(v._rightChild!);
                    }
                }

            });

            const preColIndex = [];
            for (let i = 0; i < tempQue.length; i++) {
                if (colIndex >= nonUniformColObjs.length) {
                    break;
                    //throw new Error("col index out range");
                }
                const type = nonUniformColObjs[colIndex].isMissContain(tempQue[i]);
                nonUniformColObjs[colIndex].containColumnRange(tempQue[i], type);
                if (type === 1) {
                    continue;
                } else if (type === 2 || type === 7 || type === 8 || type === 9 || type === 10) {

                    if (type === 2) {
                        tempNeedLoadDifNodes.push(tempQue[i]);
                        preColIndex.push(colIndex);
                    }
                    if (type === 7) {
                        tempNeedLoadDifNodes.push(tempQue[i]);
                        preColIndex.push(colIndex)
                    }
                    if (type === 8) {
                        if (colIndex != 0) {
                            tempNeedLoadDifNodes.push(tempQue[i]);
                            preColIndex.push(colIndex - 1)
                        }
                    }
                    if (type === 9) {
                        if (colIndex !== nonUniformColObjs.length - 1) {
                            tempNeedLoadDifNodes.push(tempQue[i]);
                            preColIndex.push(colIndex)
                        }
                    }
                    if (type === 10) {
                        tempNeedLoadDifNodes.push(tempQue[i]);
                        preColIndex.push(colIndex);
                    }

                } else if (type === 3) {
                    colIndex++;
                    i--;
                } else if (type === 6) {
                    break;
                } else {
                    continue;
                    // throw new Error("node time is little than col");
                }
            }
            if (preColIndex.length != tempNeedLoadDifNodes.length) {
                throw new Error("cannot memory index");
            }
            for (let i = 0; i < tempNeedLoadDifNodes.length; i++) {
                if (tempNeedLoadDifNodes[i].gapFlag !== 'NO') {
                    continue;
                }
                if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                    const con1 = canCut(tempNeedLoadDifNodes[i], nonUniformColObjs[preColIndex[i]], nonUniformColObjs[preColIndex[i] + 1], yScale);
                    if (con1) {
                        tempNeedLoadDifNodes.splice(i, 1)
                        preColIndex.splice(i, 1);
                    }
                }

            }
            needLoadDifNode = tempNeedLoadDifNodes;

            if (needLoadDifNode.length > 0 && needLoadDifNode[0].level === this.maxLevel - 2) {

                for (let i = 0; i < needLoadDifNode.length; i++) {

                    const nodeFlag2 = currentFlagInfo[2 * needLoadDifNode[i].index + 1]
                    if (needLoadDifNode[i].gapFlag === 'NO') {
                        if (nodeFlag2 === 0) {
                            nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[1]);
                            nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[1]);
                            if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                                nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[2]);
                                nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[2]);
                            }
                        } else {
                            nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[2]);
                            nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[2]);
                            if (preColIndex[i] + 1 < nonUniformColObjs.length) {
                                nonUniformColObjs[preColIndex[i] + 1].addFirstVal(needLoadDifNode[i].yArray[1]);
                                nonUniformColObjs[preColIndex[i] + 1].forceMerge(needLoadDifNode[i].yArray[1]);
                            }
                        }
                    } else {
                        if (nodeFlag2 === 0) {
                            nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[1]);
                            nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[1]);

                            nonUniformColObjs[preColIndex[i]].addFirstVal(needLoadDifNode[i].yArray[2]);
                            nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[2]);

                        } else {
                            nonUniformColObjs[preColIndex[i]].addLastVal(needLoadDifNode[i].yArray[2]);
                            nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[2]);

                            nonUniformColObjs[preColIndex[i]].addFirstVal(needLoadDifNode[i].yArray[1]);
                            nonUniformColObjs[preColIndex[i]].forceMerge(needLoadDifNode[i].yArray[1]);

                        }
                    }

                }
                break;
            }
            if (store.state.controlParams.progressive && drawer) {
                drawer(nonUniformColObjs, "progressive")
            }

            if (needLoadDifNode.length === 0) {
                break;
            }
            let losedDataInfo = computeLosedDataRangeV1(needLoadDifNode);
            if (losedDataInfo.length > 0) {
                await batchLoadDataForRangeLevel1MinMaxMiss(losedDataInfo, this);
            }

        }
        for (let i = 0; i < nonUniformColObjs.length; i++) {
            nonUniformColObjs[i].checkIsMis();
        }

        return nonUniformColObjs;
    }

}



