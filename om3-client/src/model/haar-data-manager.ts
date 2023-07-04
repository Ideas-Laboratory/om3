import HaarIndexObj from "./haar-index-obj";
import axios from "axios";
import TrendTree from "@/helper/tend-query-tree";
import store from "@/store";
import * as d3 from 'd3';
import { canCut, checkSetType, computeLosedDataRange, computeTimeSE } from "@/helper/util";
import { ElNotification } from 'element-plus'
import { NoUniformColObj } from "./non-uniform-col-obj";
import HaarTree from "@/helper/haar-tree";

export default class HaarDataManager {
    haarIndexObjs: Array<HaarIndexObj>
    maxLevel: number
    realDataRowNum: number
    dataName: string
    
    constructor(haarIndexObjs: Array<HaarIndexObj>, dataName: string,maxLevel?:number) {
        this.haarIndexObjs = haarIndexObjs;
        this.maxLevel = maxLevel?maxLevel:store.state.controlParams.tableMaxLevel;
        this.realDataRowNum = 7193200;
        this.dataName = dataName;
        
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
        for (let i = level - 1; i > 2; i--) {
            nextStart = Math.floor(nextStart / 2);
            nextEnd = Math.floor(nextEnd / 2);
            if (this.haarIndexObjs[i] && this.haarIndexObjs[i].isFull) {
                lastFullLevel = i;
                break;
            }
        }
        for (let i = lastFullLevel + 1; i <= level; i++) {
            nextStart = nextStart * 2;
            nextEnd = nextEnd * 2 + 1;
            const losedDataInfo = this.checkLoadedDataInSingalLevel([[i, nextStart, nextEnd]]);
            await this.batchLoadDataForRangeLevel(losedDataInfo);
        }
        if (this.haarIndexObjs[level]) {
            if (noRet) {
                return {data:[],start:0,end:0,l:0}
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
                    return this.haarIndexObjs[level].getDataByIndex(start, end);
                } else {
                    return this.haarIndexObjs[level].getDataByIndex(start, end);
                }
            }
        } else {
            return {data:[],start:0,end:0,l:0}
            //throw new Error("cannot get data from data manager");
        }

    }
    async getDataV1(level: number, start: number, end: number, noRet?: boolean) {
        if (level > this.maxLevel) {
            return [];
        }
        if (this.haarIndexObjs[level]) {

            const hasData = this.haarIndexObjs[level].hasDataForRange(start, end);
            if (hasData.has) {
                return this.haarIndexObjs[level].getDataByIndex(start, end);
            } else {
                const losedRange = hasData.range;

                for (let i = 0; i < losedRange.length; i++) {
                    let l = level - 1;
                    const losedDataOtherLevel = [];
                    for (; l >= 0; l--) {
                        if (this.haarIndexObjs[l].isFull) {
                            break;
                        }
                        const levelHasData = this.haarIndexObjs[l].hasDataForRange(Math.floor(losedRange[i][0] / (2 ** (level - l))), Math.floor(losedRange[i][1] / (2 ** (level - l))));
                        if (levelHasData.has) {
                            break;
                        }
                        losedDataOtherLevel.push([l, Math.floor(losedRange[i][0] / (2 ** (level - l))), Math.floor(losedRange[i][1] / (2 ** (level - l)))]);
                    }
                    await this.loadDataForRangeLevel(losedDataOtherLevel);
                }
                losedRange.forEach(range => {
                    range.unshift(level);
                });
                await this.loadDataForRangeLevel(losedRange);
            }
        } else {
            let l = level - 1;
            const losedDataOtherLevel = [];
            for (; l >= 0; l--) {
                if (this.haarIndexObjs[l] && this.haarIndexObjs[l].isFull) {
                    break;
                }
                if (this.haarIndexObjs[l]) {
                    const levelHasData = this.haarIndexObjs[l].hasDataForRange(Math.floor(start / (2 ** (level - l))), Math.floor(end / (2 ** (level - l))));
                    if (levelHasData.has) {
                        break;
                    }
                }
                losedDataOtherLevel.push([l, Math.floor(start / (2 ** (level - l))), Math.floor(end / (2 ** (level - l)))]);
            }
            losedDataOtherLevel.unshift([level, start, end]);
            await this.loadDataForRangeLevel(losedDataOtherLevel);
        }
        if (this.haarIndexObjs[level]) {
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
                    return this.haarIndexObjs[level].getDataByIndex(start, end);
                } else {
                    return this.haarIndexObjs[level].getDataByIndex(start, end);
                }
            }
        } else {
            throw new Error("cannot get data from data manager");
        }
    }
    getLoadedData(level: number, start: number, end: number) {
        if (this.haarIndexObjs[level]) {
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
                return this.haarIndexObjs[level].getDataByIndex(start, end);
            } else {
                return this.haarIndexObjs[level].getDataByIndex(start, end);
            }

        } else {
            throw new Error("cannot get data from data manager");
        }
    }

    async batchLoadDataForRangeWithCheck(losedRange: Array<Array<number>>) {
        for (let i = 0; i < losedRange.length; i++) {
            const checkRes = this.checkLoadedDataInSingalLevel([[losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2), Math.floor(losedRange[i][2] / 2)]]);
            if (checkRes.length !== 0) {
                await this.getData(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2), Math.floor(losedRange[i][2] / 2));
            }
        }
        await this.batchLoadDataForRangeLevel(losedRange);
        return
    }

    async batchLoadDataForRangeLevel(losedRange: Array<Array<number>>) {
        const difVals = await this.batchLoadDifWidthPost(losedRange);
        let count = 0;
        for (let i = 0; i < losedRange.length; i++) {
            const levelRange = losedRange[i];

            const startNode = this.haarIndexObjs[losedRange[i][0] - 1].getTreeNodeStartIndex(Math.floor(losedRange[i][1] / 2));
            let p = startNode;
            const newTreeNode = [];
            for (let j = Math.floor(losedRange[i][1] / 2); j <= Math.floor(losedRange[i][2] / 2); j++) {
                if (p?.index === j && j === difVals[count].i && p.level === difVals[count].l) {
                   
                    p.difference = difVals[count].dif;
                    const a=(2*p.value+difVals[count].dif)/2;
                    const b=(2*p.value-difVals[count].dif)/2;
                    const firstNode = new HaarTree(p, true, p.index, a);
                    const secondNode = new HaarTree(p, false, p.index, b);
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
                    console.log(this.haarIndexObjs);
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
            if (this.haarIndexObjs[losedRange[i][0]]) {
                this.haarIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            } else {
                this.haarIndexObjs[losedRange[i][0]] = new HaarIndexObj(losedRange[i][0], false);
                this.haarIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            }
        }
    }



    async loadDataForRangeLevel(losedRange: Array<Array<number>>) {
        if (losedRange.length <= 0) {
            return
        }
        //await this.batchLoadDataForRangeLevel(losedRange)
        for (let i = losedRange.length - 1; i >= 0; i--) {
            const levelRange = losedRange[i];
            const difVal = await this.loadDif(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2), Math.floor(losedRange[i][2] / 2));

            const startNode = this.haarIndexObjs[losedRange[i][0] - 1].getTreeNodeStartIndex(Math.floor(losedRange[i][1] / 2));
            let p = startNode;
            const newTreeNode = [];
            let count = 0;
            for (let j = Math.floor(losedRange[i][1] / 2); j <= Math.floor(losedRange[i][2] / 2); j++) {
                if (p?.index === j) {
                    //@ts-ignore


                    p.difference = difVal[count].dif;
                    const a=(2*p.value+difVal[count].dif)/2;
                    const b=(2*p.value-difVal[count].dif)/2;
                    const firstNode = new HaarTree(p, true, p.index, a);
                    const secondNode = new HaarTree(p, false, p.index, b);
                    newTreeNode.push(firstNode);
                    newTreeNode.push(secondNode);
                    p = p.nextSibling;
                    if (p === null) {
                        break;
                    }
                    count++;
                } else {
                    console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                    console.log("lose range:", losedRange, p, p?.index, j);
                    console.log(this.haarIndexObjs);
                    debugger
                    throw new Error("dif not match node");
                }
            }
            for (let j = 0; j < newTreeNode.length - 1; j++) {
                newTreeNode[j].nextSibling = newTreeNode[j + 1];
                if (newTreeNode[j].index != newTreeNode[j + 1].index - 1) {
                    throw new Error("sibling index error");
                }
                newTreeNode[j + 1].previousSibling = newTreeNode[j];            }
            if (this.haarIndexObjs[losedRange[i][0]]) {
                this.haarIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            } else {
                this.haarIndexObjs[losedRange[i][0]] = new HaarIndexObj(losedRange[i][0], false);
                this.haarIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
            }
        }
    }

    async batchLoadDif(losedDataInfo: Array<Array<number>>) {
        if (losedDataInfo.length === 0) {
            return [];
        }
        const { data } = await axios.get(`postgres/line_chart/batchLevelDataProgressiveWavelet?table_name=${store.state.controlParams.currentTable}&losedDataInfo[]=${losedDataInfo}`);
        const result = data.data;
        const resultArray = [];
        if (result && result[0] && result[0].length > 0) {
            for (let i = 0; i < result[0].length; i++) {
                resultArray.push({ l: this.maxLevel - result[0][i], i: result[1][i], dif: [result[2][i], result[3][i], result[4][i], result[5][i]] });
            }
        }

        return resultArray;
    }
    async batchLoadDifWidthPost(losedDataInfo: Array<Array<number>>) {
        if (losedDataInfo.length === 0) {
            return [];
        }
        const { data } = await axios.post(`postgres/line_chart/batchLevelDataProgressiveWavelet`, {
            table_name: store.state.controlParams.currentTable,
            losedDataInfo: JSON.stringify({ data: losedDataInfo }),
        });
        const result = data.data;
        const resultArray = [];
        if (result && result[0] && result[0].length > 0) {
            for (let i = 0; i < result[0].length; i++) {
                resultArray.push({ l: this.maxLevel - result[0][i], i: result[1][i], dif: result[2][i] });
            }
        }

        return resultArray;
    }
    async loadDif(level: number, start: number, end: number) {

        //@ts-ignore
        const { data } = await axios.get(`postgres/line_chart/trendQueryProgressiveWavelet?table_name=${store.state.controlParams.currentTable}&current_level=${level}&start_time=${start}&end_time=${end}`);
        return data;
    }

   

 

    // timeBoxQuery(start: number, end: number, min: number, max: number, current_level: number, visRange: Array<number>): boolean {
    //     const checkRange = [visRange[0] + start, visRange[0] + end];
    //     let startLevel = current_level;
    //     for (let i = current_level - 1; i >= 0; i--) {
    //         if (Math.floor(checkRange[0] / (2 ** (current_level - i))) === Math.floor(checkRange[1] / (2 ** (current_level - i)))) {
    //             startLevel = i;
    //         }
    //     }
    //     for (let i = startLevel; i < current_level; i++) {
    //         const result = this.haarIndexObjs[i].timeBoxQuery([Math.floor(checkRange[0] / (2 ** (current_level - i))), Math.floor(checkRange[1] / (2 ** (current_level - i)))], [min, max]);
    //         if (result === 3) {
    //             return false;
    //         } else if (result === 1) {
    //             return true;
    //         }
    //     }
    //     return false
    // }
    
   

   
   

  

   


    checkLoadedDataInSingalLevel(losedDataInfo: Array<Array<number>>) {
        const currentLevelLosedRange = [];
        for (let j = 0; j < losedDataInfo.length; j++) {
            const level = losedDataInfo[j][0]
            const start = losedDataInfo[j][1]
            const end = losedDataInfo[j][2];
            if (this.haarIndexObjs[level]) {

                const hasData = this.haarIndexObjs[level].hasDataForRange(start, end);
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
    countNodeNum(){
        let sum=0;
        for(let i=0;i<this.haarIndexObjs.length;i++){
            for(let j=0;j<this.haarIndexObjs[i].loadedDataRange.length;j++){
                sum+=this.haarIndexObjs[i].loadedDataRange[j][1]-this.haarIndexObjs[i].loadedDataRange[j][0]+1
            }
        }
        console.log("allNodeNum",sum);
    }

}