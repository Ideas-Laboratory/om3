import TrendTree from "@/helper/tend-query-tree";
import { getIndexTime } from "@/helper/format-data";

export class NoUniformColObj {
    col: number;
    tStart: number;
    tEnd: number;
    semanticTStart: number;
    semanticTEnd: number;
    level: number;
    startIndex: number;
    endIndex: number;
    index: number;
    startV: number | undefined;
    endV: number | undefined;
    vRange: Array<number>;
    width: number;
    globalDataLen: number;
    positionInfo: {
        startX: number,
        endX: number,
        minX: number,
        maxX: number;
    }
    minVTimeRange: Array<number>
    maxVTimeRange: Array<number>
    maxLevel: number;
    dataName?: string;
    isMis: boolean;
    shapeMinMax: Array<number>;
    shapeMinT: Array<Array<number>>;
    shapeMaxT: Array<Array<number>>;
    minNodes: Array<TrendTree>;
    maxNodes: Array<TrendTree>;
    realStart: number;
    sementicInterval: number;
    constructor(col: number, tStart: number, tEnd: number, level: number, width: number, globalDataLen: number, maxLevel: number, dataName?: string) {
        this.isMis = false;
        this.startV = undefined;
        this.endV = undefined;
        this.minVTimeRange = [0, 0];
        this.maxVTimeRange = [0, 0];
        this.vRange = [Infinity, -Infinity];
        this.width = width;
        this.col = col;
        this.tStart = tStart;
        this.tEnd = tEnd;
        this.semanticTStart = tStart;
        this.semanticTEnd = tEnd;
        this.level = level;
        this.startIndex = 0;
        this.endIndex = 0;
        this.index = 0;
       
       
        this.globalDataLen = globalDataLen;
        this.maxLevel = maxLevel
        this.positionInfo = {
            startX: 0,
            endX: 0,
            minX: 0,
            maxX: 0
        }
       
        this.dataName = dataName;
        this.shapeMinMax = [Infinity, -Infinity];
        this.shapeMinT = [];
        this.shapeMaxT = [];
        this.minNodes = [];
        this.maxNodes = [];
        this.realStart = 0;
        this.sementicInterval = 0;
    }
    rebuild(col: number, tStart: number, tEnd: number, level: number, width: number, globalDataLen: number, maxLevel: number, dataName?: string) {
        this.width = width;
        this.col = col;
        this.tStart = tStart;
        this.tEnd = tEnd;
        this.level = level;
        this.startIndex = 0;
        this.endIndex = 0;
        this.index = 0;
        this.startV = undefined;
        this.endV = undefined;
        this.vRange[0] = Infinity;
        this.vRange[1] = -Infinity;
        this.globalDataLen = globalDataLen;
        this.maxLevel = maxLevel
        this.positionInfo.startX = 0;
        this.positionInfo.endX = 0;
        this.positionInfo.minX = 0;
        this.positionInfo.maxX = 0;
        this.minVTimeRange[0] = 0;
        this.minVTimeRange[1] = 0;
        this.maxVTimeRange[0] = 0;
        this.maxVTimeRange[1] = 0;
        this.dataName = dataName;

    }
    setRealStartAndInterval(start: number, interval: number) {
        this.realStart = start;
        this.sementicInterval = interval;
    }

    setTStart(t: number) {
        this.tStart = t;
    }
    setTEnd(t: number) {
        this.tEnd = t;
    }

    setSemanticTStart(t: number) {
        this.semanticTStart = t;
        this.tStart = Math.floor((this.semanticTStart - this.realStart) / this.sementicInterval);
    }
    setSemanticTEnd(t: number) {
        this.semanticTEnd = t;
        this.tEnd = Math.floor((this.semanticTEnd - this.realStart) / this.sementicInterval) - 1;
    }
    getEndRange() {
        const index = Math.floor(this.tEnd / 2);
        if (index % 2 === 0) {
            return [index, index + 1];
        } else {
            return [index - 1, index];
        }
    }
    getStartRange() {
        const index = Math.floor(this.tStart / 2);
        if (index % 2 === 0) {
            return [index, index + 1];
        } else {
            return [index - 1, index];
        }
    }
    // 1 完全属于该列  2 与该列相交   3 大于该列 4 完全小于该列 5 不完全小于该列
    isContain(p: TrendTree) {
        const pL = p.level;
        const pTRange = (2 ** this.maxLevel) / (2 ** pL);
        const pTimeS = p.index * pTRange;
        const pTimeE = pTRange + pTimeS - 1;
        if (this.tStart === pTimeS) {
            this.startV = p.yArray[0];
        }
        if (this.tEnd === pTimeE) {
            this.endV = p.yArray[3];
        }
        if (pTimeE >= this.globalDataLen) {
            if (pTimeS <= this.tEnd) {
                if (p.yArray[1] < this.vRange[0]) {
                    this.vRange[0] = p.yArray[1];
                    const tRange = getIndexTime(p.level, p.index, this.maxLevel);
                    this.minVTimeRange = [tRange.startT, tRange.endT];
                }
                if (p.yArray[2] > this.vRange[1]) {
                    this.vRange[1] = p.yArray[2];
                    const tRange = getIndexTime(p.level, p.index, this.maxLevel);
                    this.maxVTimeRange = [tRange.startT, tRange.endT];
                }
            }
            return 6;
        }
        if (pTimeS >= this.tStart && pTimeE <= this.tEnd) {
            if (p.yArray[1] < this.vRange[0]) {
                this.vRange[0] = p.yArray[1];
                const tRange = getIndexTime(p.level, p.index, this.maxLevel);
                this.minVTimeRange = [tRange.startT, tRange.endT];
            }
            if (p.yArray[2] > this.vRange[1]) {
                this.vRange[1] = p.yArray[2];
                const tRange = getIndexTime(p.level, p.index, this.maxLevel);
                this.maxVTimeRange = [tRange.startT, tRange.endT];
            }

            return 1;
        } else if (pTimeS <= this.tEnd && pTimeE > this.tEnd) {
            return 2;
        } else if (pTimeS > this.tEnd) {
            return 3
        } else if (pTimeE < this.tStart) {
            // debugger
            return 4;
        } else if (pTimeE >= this.tStart) {
            //debugger
            return 5
        } else {
            console.log(p, this)

            throw new Error("time out of range")
        }
    }
    isMissContain(p: TrendTree) {
        const pL = p.level;
        const pTRange = (2 ** this.maxLevel) / (2 ** pL);
        const pTimeS = p.index * pTRange;
        const pTimeE = pTRange + pTimeS - 1;

       
        if (pTimeE >= this.globalDataLen) {
            return 6;
        }
        if (pTimeS >= this.tStart && pTimeE <= this.tEnd) {
            if(pTimeS==this.tStart&&pTimeE==this.tEnd){
                return 7;
            }else if(pTimeS===this.tStart){
                return 8;
            }else if(pTimeE===this.tEnd){
                return 9;
            }else if(p.gapFlag!=='NO'){
                return 10
            }
            //console.log(pTimeS,pTimeE,this.tStart,this.tEnd)
            return 1;
        } else if (pTimeS <= this.tEnd && pTimeE > this.tEnd) {
            return 2;
        } else if (pTimeS > this.tEnd) {
            return 3
        } else if (pTimeE < this.tStart) {
            return 4;
        } else if (pTimeE >= this.tStart) {
            return 5
        } else {
            console.log(p, this)
            throw new Error("time out of range")
        }
    }
    containColumnRange(p: TrendTree, type: number) {

        if (p.nodeType === "NULL") {
            return
        }
        if ( p.yArray[1] === undefined || p.yArray[2] === undefined) {
            debugger
            throw new Error("error val")
        }
        const pL = p.level;
        const pTRange = (2 ** this.maxLevel) / (2 ** pL);
        const pTimeS = p.index * pTRange;
        const pTimeE = pTRange + pTimeS - 1;
        if (this.tStart === pTimeS) {
            
            // throw new Error("cannot use this val")
            // this.startV = p.yArray[0];
        }
        if (this.tEnd === pTimeE) {
            
            // throw new Error("cannot use this val")
            // this.endV = 0;//p.yArray[3];
        }
        if (type === 6) {
            // console.log(pTimeE)
            // console.log(this.globalDataLen)
            // console.error("type 6");
            //throw new Error("type 8")
        }
        if (type === 1||type===7||type===8||type===9) {
            if (p.yArray[1] < this.vRange[0]) {
                this.vRange[0] = p.yArray[1];
                const tRange = getIndexTime(p.level, p.index, this.maxLevel);
                this.minVTimeRange = [tRange.startT, tRange.endT];
            }
            if (p.yArray[2] > this.vRange[1]) {
                this.vRange[1] = p.yArray[2];
                const tRange = getIndexTime(p.level, p.index, this.maxLevel);
                this.maxVTimeRange = [tRange.startT, tRange.endT];
            }
            // if (p.parent && p.parent.nodeType === 'LEFTNULL') {
            //     this.startV = p.yArray[0];
            // }
            // if (p.parent && p.parent.nodeType === 'RIGHTNULL') {
            //     if (this.endV! > p.yArray[3]) {
            //         throw new Error("dddddddddddd")
            //     }
            //     this.endV = p.yArray[3];
            // }
        }
        return;
    }
   


    
   



    addLastVal(v: number) {

        if (v === undefined) {
            return
        }
        this.endV = v
    }
    addFirstVal(v: number) {
        if (v === undefined) {
            return
        }
        this.startV = v;
    }
    forceMerge(val: number) {
        if (val === undefined) {
            throw new Error("undefine")
            return
        }
        if (val < this.vRange[0]) {
            this.vRange[0] = val;
        }
        if (val > this.vRange[1]) {
            this.vRange[1] = val;
        }
    }
    mergeLast(k: number, b: number) {

        const val = k * this.tEnd + b;
        if (val === undefined) {
            throw new Error("undefine")
            return
        }
        if (val > this.vRange[1]) {
            this.vRange[1] = val;
        }
        if (val < this.vRange[0]) {
            this.vRange[0] = val;
        }
        this.endV = val;
    }
    mergeFirst(k: number, b: number) {
        const val = k * this.tStart + b;
        if (val === undefined) {
            throw new Error("undefine")
            return
        }
        if (val > this.vRange[1]) {
            this.vRange[1] = val;
        }
        if (val < this.vRange[0]) {
            this.vRange[0] = val;
        }
        this.startV = val;
    }
    checkIsMis() {
        if (this.vRange[0] === Infinity || this.vRange[1] === -Infinity) {
            this.isMis = true
        }
        return this.isMis
    }

}
