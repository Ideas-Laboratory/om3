import TrendTree from "@/helper/tend-query-tree";
import { checkSetType, computeNeedLoadDataRange, isIntersect } from "../helper/util"
export default class LevelIndexObj {
    level: number;
    isFull: boolean;
    loadedDataRange: Array<Array<number>>;
    firstNodes: Array<TrendTree>;
    isSelfCheck: boolean;
    constructor(level: number, isFull = false) {
        this.level = level;
        this.isFull = isFull;
        this.loadedDataRange = [];
        
        this.firstNodes = [];
        this.isSelfCheck = false;
    }

    addLoadedDataRange(firstNode: TrendTree, range: Array<number>) {
        if (this.loadedDataRange.length === 0) {
            this.loadedDataRange.push(range);
            this.firstNodes.push(firstNode);
            return
        }
        const loadedDataRangeLen = this.loadedDataRange.length;
        let isMerge = false;
        const needCheckIndex = [];
        for (let i = 0; i < loadedDataRangeLen; i++) {
            const intersectRes = isIntersect(this.loadedDataRange[i], range);
            if (intersectRes.isIntersect) {
                isMerge = true;
                if (intersectRes.pos === 'second') {
                    const lastNode = this.firstNodes[i];
                    this.firstNodes[i] = firstNode;
                    let p = firstNode;

                    while (p.nextSibling) {
                        if (p.nextSibling.index === lastNode.index) {
                            p.nextSibling = lastNode;
                            break;
                        }
                        p = p.nextSibling;
                    }
                } else {
                    let p = this.firstNodes[i];
                    while (p.nextSibling) {
                        if (p.nextSibling.index === firstNode.index) {
                            p.nextSibling = firstNode;
                            break;
                        }
                        p = p.nextSibling;
                    }
                }
                needCheckIndex.push(i);
                this.loadedDataRange[i][0] = Math.min(this.loadedDataRange[i][0], range[0]);
                this.loadedDataRange[i][1] = Math.max(this.loadedDataRange[i][1], range[1]);
                //break
            } else {
                const minHead = Math.min(this.loadedDataRange[i][0], range[0]);
                if (minHead === this.loadedDataRange[i][0] && (this.loadedDataRange[i][1] + 1 === range[0])) {
                    isMerge = true;
                    let p = this.firstNodes[i];
                    while (p.nextSibling) {
                        p = p.nextSibling;
                    }
                    if (p.index !== firstNode.index - 1) {
                        console.log(p, firstNode);
                        debugger
                        throw new Error("index error")
                    }
                    p.nextSibling = firstNode;

                    this.loadedDataRange[i][1] = range[1];
                    needCheckIndex.push(i);
                    //break
                } else if (minHead === range[0] && (range[1] + 1 === this.loadedDataRange[i][0])) {
                    isMerge = true;
                    let p = firstNode;
                    while (p.nextSibling) {
                        p = p.nextSibling;
                    }
                    p.nextSibling = this.firstNodes[i];
                    this.firstNodes[i] = firstNode;
                    this.loadedDataRange[i][0] = range[0];
                    needCheckIndex.push(i);
                    //break
                }
            }
        }
        if (this.isSelfCheck) {
            this.selfCheck()
        }
        let a=false;
        let tempIndexs=[]
        if(needCheckIndex.length>2){
          tempIndexs=  needCheckIndex.map(v=>{
                return v
            })
            a=true
            //debugger
        }
        if (needCheckIndex.length > 0) {
            this.reCheckIntersect(needCheckIndex);
        }
        if(a){
            debugger
        }
        if (this.isSelfCheck) {
            this.selfCheck()
        }
        if (!isMerge) {
            this.loadedDataRange.push(range);
            this.firstNodes.push(firstNode);
        }
        if (this.firstNodes.length === 1 && (this.loadedDataRange[0][1] - this.loadedDataRange[0][0] + 1) === 2 ** this.level) {
            this.isFull = true;
        }
    }


    getDataByIndex(start: number, end: number) {
        const data:Array<Array<number>> = [];
        for (let i = 0; i < this.firstNodes.length; i++) {
            if (this.loadedDataRange[i][0] <= start && this.loadedDataRange[i][1] >= end) {
                let p = this.firstNodes[i];
                while (p) {
                    if (p.index >= start && p.index <= end) {
                        data.push(p.yArray);
                    }
                    if (p.index > end) {
                        break;
                    }
                    //@ts-ignore
                    p = p.nextSibling;
                }
            }
        }
        return {data,start,end,l:this.level};
    }
    hasDataForRange(start: number, end: number) {
        if (this.isFull) {
            return {
                has: true,
                range: [],
            }
        }
        const needDataRange = computeNeedLoadDataRange([start, end], this.loadedDataRange);
        if (needDataRange.length === 0) {
            return {
                has: true,
                range: [],
            }
        } else {
            return {
                has: false,
                range: needDataRange
            }
        }
    }
    // fillLevel(){
    //     const needDataRes=this.hasDataForRange(0,2**this.level);
    //     if(needDataRes.has){
    //         return
    //     }

    // }
    getTreeNodeStartIndex(start: number) {
        for (let i = 0; i < this.firstNodes.length; i++) {
            if (this.loadedDataRange[i][0] <= start && this.loadedDataRange[i][1] >= start) {
                let root = this.firstNodes[i];
                while (root) {

                    if (root.index === start) {
                        return root
                    }
                    //@ts-ignore
                    root = root.nextSibling;
                }
            }
        }
        return null;
    }

    reCheckIntersect(needCheckedNodeIndex: Array<number>) {
        if (needCheckedNodeIndex.length > 2) {
           // throw new Error("check intersect error");
           console.log("..................................");
        }
        let needDeleteNodeIndex = [];
        while (needCheckedNodeIndex.length > 0) {
            let nextCheckNodeIndex = [];
            for (let i = 0; i < needCheckedNodeIndex.length; i++) {
                for (let j = 0; j < this.loadedDataRange.length; j++) {
                    if (needCheckedNodeIndex[i] === j) {
                        continue;
                    }
                    const intersectRes = isIntersect(this.loadedDataRange[j], this.loadedDataRange[needCheckedNodeIndex[i]]);
                    if (intersectRes.isIntersect) {
                        if (j === needCheckedNodeIndex[needCheckedNodeIndex.length - 1]) {
                            needCheckedNodeIndex.splice(needCheckedNodeIndex.length - 1, 1);
                        }
                        if (intersectRes.pos === 'second') {

                            needDeleteNodeIndex.push(j);

                            const lastNode = this.firstNodes[j];
                            const firstNode = this.firstNodes[needCheckedNodeIndex[i]];
                            let p = firstNode;

                            while (p.nextSibling) {
                                if (p.nextSibling.index === lastNode.index) {
                                    p.nextSibling = lastNode;
                                    break;
                                }
                                p = p.nextSibling;
                            }

                            this.loadedDataRange[needCheckedNodeIndex[i]][0] = Math.min(this.loadedDataRange[j][0], this.loadedDataRange[needCheckedNodeIndex[i]][0]);
                            this.loadedDataRange[needCheckedNodeIndex[i]][1] = Math.max(this.loadedDataRange[j][1], this.loadedDataRange[needCheckedNodeIndex[i]][1]);
                            nextCheckNodeIndex.push(needCheckedNodeIndex[i]);
                        } else {

                            needDeleteNodeIndex.push(needCheckedNodeIndex[i]);

                            let p = this.firstNodes[j];
                            while (p.nextSibling) {
                                if (p.nextSibling.index === this.firstNodes[needCheckedNodeIndex[i]].index) {
                                    p.nextSibling = this.firstNodes[needCheckedNodeIndex[i]];
                                    break;
                                }
                                p = p.nextSibling;
                            }
                            this.loadedDataRange[j][0] = Math.min(this.loadedDataRange[j][0], this.loadedDataRange[needCheckedNodeIndex[i]][0]);
                            this.loadedDataRange[j][1] = Math.max(this.loadedDataRange[j][1], this.loadedDataRange[needCheckedNodeIndex[i]][1]);
                            nextCheckNodeIndex.push(j);
                        }
                    } else {

                        const minHead = Math.min(this.loadedDataRange[j][0], this.loadedDataRange[needCheckedNodeIndex[i]][0]);
                        if (minHead === this.loadedDataRange[j][0] && (this.loadedDataRange[j][1] + 1 === this.loadedDataRange[needCheckedNodeIndex[i]][0])) {
                            needDeleteNodeIndex.push(needCheckedNodeIndex[i])
                            let p = this.firstNodes[j];
                            while (p.nextSibling) {
                                p = p.nextSibling;
                            }
                            p.nextSibling = this.firstNodes[needCheckedNodeIndex[i]];

                            this.loadedDataRange[j][1] = this.loadedDataRange[needCheckedNodeIndex[i]][1];
                            nextCheckNodeIndex.push(j)
                        } else if (minHead === this.loadedDataRange[needCheckedNodeIndex[i]][0] && (this.loadedDataRange[needCheckedNodeIndex[i]][1] + 1 === this.loadedDataRange[j][0])) {
                            needDeleteNodeIndex.push(j);
                            let p = this.firstNodes[needCheckedNodeIndex[i]];
                            while (p.nextSibling) {
                                p = p.nextSibling;
                            }
                            p.nextSibling = this.firstNodes[j];

                            this.loadedDataRange[needCheckedNodeIndex[i]][1] = this.loadedDataRange[j][1];
                            nextCheckNodeIndex.push(needCheckedNodeIndex[i]);
                        }

                    }


                }
            }
            for (let i = 0; i < needDeleteNodeIndex.length; i++) {
                const idx = needDeleteNodeIndex[i];
                for (let j = 0; j < nextCheckNodeIndex.length; j++) {
                    if (nextCheckNodeIndex[j] > idx) {
                        nextCheckNodeIndex[j]--;
                    }
                }
                this.loadedDataRange.splice(idx, 1);
                this.firstNodes.splice(idx, 1);
            }
            needDeleteNodeIndex = [];
            needCheckedNodeIndex = nextCheckNodeIndex;
            nextCheckNodeIndex = [];
        }
    }

    selfCheck() {
        if (this.loadedDataRange.length != this.firstNodes.length) {
            console.log(this);
            throw new Error("loadedDataRange len not match firtnodes")
        }
        for (let i = 0; i < this.loadedDataRange.length; i++) {
            if (this.loadedDataRange[i][0] !== this.firstNodes[i].index) {
                console.log(this);
                throw new Error("range not match node index");
            }
        }
        for (let i = 0; i < this.loadedDataRange.length; i++) {
            let p = this.firstNodes[i];
            let index = this.loadedDataRange[i][0];
            while (p && index <= this.loadedDataRange[i][1]) {
                if (p.index === index) {
                    p = p.nextSibling!;
                    index++;
                } else {
                    console.log(this);
                    throw new Error("range not match node index");
                }
            }
        }
    }

    timeBoxQuery(checkRange: Array<number>, minMax: Array<number>) {
        for (let i = 0; i < this.loadedDataRange.length; i++) {
            if (this.loadedDataRange[i][0] <= checkRange[0] && this.loadedDataRange[i][1] >= checkRange[1]) {
                let p = this.firstNodes[i];
                let hasType3 = false;
                while (p && p.index <= checkRange[1]) {
                    if (p.index >= checkRange[0]) {
                        const type = checkSetType(minMax, [p.yArray[1], p.yArray[2]])
                        if (type === 3) {
                            hasType3 = true;
                        }
                        if (type === 2) {
                            return 2
                        }
                    }
                    p = p.nextSibling as TrendTree;
                }
                if (hasType3) {
                    return 3;
                } else {
                    return 1;
                }
            }
        }
        throw new Error("error");
    }
}