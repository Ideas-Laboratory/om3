import TrendTree from "@/helper/tend-query-tree";
import { getIndexTime } from "@/helper/format-data";

export class NoUniformColObj {
    col: number;
    tStart: number;
    tEnd: number;
    level: number;
    startIndex: number;
    endIndex: number;
    index: number;
    startV: number | undefined;
    endV: number | undefined;
    vRange: Array<number>;
    width: number;
    globalDataLen: number;
    positionInfo:{
        startX:number,
        endX:number,
        minX:number,
        maxX:number;
    }
    minVTimeRange:Array<number>
    maxVTimeRange:Array<number>
    maxLevel:number;
    dataName?:string;
    isMis:boolean;
    constructor(col: number, tStart: number, tEnd: number, level: number, width: number, globalDataLen: number,maxLevel:number,dataName?:string) {
        this.isMis=false;
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
        this.vRange = [Infinity, -Infinity];
        this.globalDataLen = globalDataLen;
        this.maxLevel=maxLevel
        this.positionInfo={
            startX:0,
            endX:0,
            minX:0,
            maxX:0
        }
        this.minVTimeRange=[0,0];
        this.maxVTimeRange=[0,0];
        this.dataName=dataName
    }
    rebuild(col: number, tStart: number, tEnd: number, level: number, width: number, globalDataLen: number,maxLevel:number,dataName?:string){
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
        this.vRange[0]=Infinity;
        this.vRange[1]=-Infinity;
        this.globalDataLen = globalDataLen;
        this.maxLevel=maxLevel
        this.positionInfo.startX=0;
        this.positionInfo.endX=0;
        this.positionInfo.minX=0;
        this.positionInfo.maxX=0;
        this.minVTimeRange[0]=0;
        this.minVTimeRange[1]=0;
        this.maxVTimeRange[0]=0;
        this.maxVTimeRange[1]=0;
        this.dataName=dataName
    }

    setTStart(t: number) {
        this.tStart = t;
    }
    setTEnd(t: number) {
        this.tEnd = t;
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
    isMissContain(p:TrendTree){
        const pL = p.level;
        const pTRange = (2**this.maxLevel) / (2 ** pL);
        const pTimeS = p.index * pTRange;
        const pTimeE = pTRange + pTimeS - 1;

        if(pTimeE>=this.globalDataLen){
            return 6;
        }
        if (pTimeS >= this.tStart && pTimeE <= this.tEnd) {
            return 1;
        }else if (pTimeS <= this.tEnd && pTimeE > this.tEnd) {
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
    containColumnRange(p:TrendTree,type:number){
        
        if(p.nodeType==="NULL"){
            return
        }
        if(p.yArray[0]===undefined||p.yArray[1]===undefined||p.yArray[2]===undefined||p.yArray[3]===undefined){
            throw new Error("error val")
        }
        const pL = p.level;
        const pTRange = (2**this.maxLevel) / (2 ** pL);
        const pTimeS = p.index * pTRange;
        const pTimeE = pTRange + pTimeS - 1;
        if(this.tStart===pTimeS){
            this.startV=p.yArray[0];
        }
        if(this.tEnd===pTimeE){
            this.endV=0;//p.yArray[3];
        }
        if(type===6){
            throw new Error("type 8")
        }
        if(type===1){
            if (p.yArray[1] < this.vRange[0]) {
                this.vRange[0] = p.yArray[1];
                const tRange=getIndexTime(p.level,p.index,this.maxLevel);
                this.minVTimeRange=[tRange.startT,tRange.endT];
            }
            if (p.yArray[2] > this.vRange[1]) {
                this.vRange[1] = p.yArray[2];
                const tRange=getIndexTime(p.level,p.index,this.maxLevel);
                this.maxVTimeRange=[tRange.startT,tRange.endT];
            }
            if(p.parent&&p.parent.nodeType==='LEFTNULL'){
               console.log("hhhhhhhhhhhhhhh1")
                this.startV=p.yArray[0];
            }
            if(p.parent&&p.parent.nodeType==='RIGHTNULL'){
                console.log("hhhhhhhhhhhhhhh2")
                if(this.endV!>p.yArray[3]){
                    throw new Error("dddddddddddd")
                }
                this.endV=p.yArray[3];
            }
        }
        return;
    }
    
    addLastVal(v: number) {

        if(v===undefined){
            return
        }
        this.endV = v
    }
    addFirstVal(v: number) {
        if(v===undefined){
            return
        }
        this.startV = v;
    }
    forceMerge(val: number) {
        if(val===undefined){
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
    mergeLast(k:number,b:number){
        
        const val=k*this.tEnd+b;
        if(val===undefined){
            throw new Error("undefine")
            return
        }
        if(val>this.vRange[1]){
            this.vRange[1]=val;
        }
        if(val<this.vRange[0]){
            this.vRange[0]=val;
        }
        this.endV=val;
    }
    mergeFirst(k:number,b:number){
        const val=k*this.tStart+b;
        if(val===undefined){
            throw new Error("undefine")
            return
        }
        if(val>this.vRange[1]){
            this.vRange[1]=val;
        }
        if(val<this.vRange[0]){
            this.vRange[0]=val;
        }
        this.startV=val;
    }

}

