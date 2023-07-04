import store from "@/store";
import axios from "axios";

const TOTAL_LEVELS = 31
export default class TrendTree {
    parent: TrendTree | null;
    _leftChild: TrendTree | null;
    _rightChild: TrendTree | null;
    preP: TrendTree | null;
    nextP: TrendTree | null;
    yArray: [number, number, number, number];
    difference: [number, number, number, number] | null;
    level: number;
    index: number;
    previousSibling: TrendTree | null;
    nextSibling: TrendTree | null;
    trendRange: Array<number>
    timeRange: Array<number>
    nodeType:'O'|'NULL'|'LEFTNULL'|'RIGHTNULL';
    gapFlag:'NO'|'L'|'R';//L miss data in left; R  miss data in right
    //fetcher: TreeNodeFetchers
    constructor(parent: TrendTree | null, leftChild = true, index = 0, yArray: [number, number, number, number], dif: [number, number, number, number] | null,nodeType?:'O'|'NULL'|'LEFTNULL'|'RIGHTNULL') {
        this.nodeType='O'
        if(nodeType){
            this.nodeType=nodeType
        }
        this.parent = parent;
        
        this._leftChild = null;
        this._rightChild = null;
        this.yArray = [0, 0, 0, 0];//Time-min,Val-min,Val-max,T-max
        this.difference = null;

        this.level = 0;
        this.previousSibling = null;
        this.nextSibling = null
        this.nextP = null;
        this.preP = null
        this.trendRange = [0, 0];
        if (yArray) {
            this.yArray = yArray;
        }
        if (dif) {
            this.difference = dif;
        }
        if (parent) {
            this.level = parent.level + 1;
            //this.fetcher = parent.fetcher;
            if (leftChild) {
                this.index = 2 * index;
                //@ts-ignore
                this.parent._leftChild = this;
            } else {
                this.index = 2 * index + 1;
                //@ts-ignore
                this.parent._rightChild = this;
            }
        } else {
            this.index = 0;
            //this.fetcher = new TreeNodeFetcher()
        }
        if (this.level > TOTAL_LEVELS) {
            throw new Error("This level is protected")
        }
        this.timeRange = [];
        if(parent===null){
            this.gapFlag='NO';
        }else{
            if(parent.gapFlag==='NO'){
                this.gapFlag='NO'
            }else if(parent.gapFlag==='L'){
                if(parent.nodeType==='LEFTNULL'){
                    if(leftChild){
                        this.gapFlag='NO'
                    }else{
                        this.gapFlag='L'
                    }
                }else{
                    if(leftChild){
                        this.gapFlag='L'
                    }else{
                        this.gapFlag='NO'
                    }
                }
                
            }else{
                if(parent.nodeType==='RIGHTNULL'){
                    if(leftChild){
                        this.gapFlag='R';
                    }else{
                        this.gapFlag='NO';
                    }
                }else{
                    if(leftChild){
                        this.gapFlag='NO';
                    }else{
                        this.gapFlag='R';
                    }
                }
                
            }
        }
    }
    get isLeafNode() {
        return this.level >= TOTAL_LEVELS - 2;
    }
    get levelNums() {
        return Math.pow(2, this.level);
    }

    getTimeRange(globalDataLen: number) {
        if (this.timeRange.length > 0) {
            return [this.timeRange[0], this.timeRange[1]];
        }
        const nodeDataRange = globalDataLen / (2 ** this.level);
        this.timeRange[0] = this.index * nodeDataRange;
        this.timeRange[1] = this.timeRange[0] + nodeDataRange - 1;
        return [this.timeRange[0], this.timeRange[1]];
    }
    data(startIndex: number) {
        if (this.level <= TOTAL_LEVELS) {
            return this.yArray.map((y, i) => {
                return {
                    x: this.index - startIndex,
                    y
                }
            })
        } else {
            return [{ x: this.index - startIndex, y: this.yArray[0] }, { x: this.index - startIndex, y: this.yArray[3] }]
        }
    }
   
}
    // get leftChild(): TrendTree | null {
    //     if (this._leftChild) return this._leftChild
    //     try {
    //         const child = new TrendTree(this, true, this.index);
    //         if (!this.difference) {
    //             this.fetcher.fetch(child);
    //         }
    //         return child;
    //     } catch (e) {
    //         return null;
    //     }
    // }

    // get rightChild(): TrendTree | null {
    //     if (this._rightChild) {
    //         return this._rightChild;
    //     }
    //     try {
    //         const child = new TrendTree(this, false, this.index);
    //         if (!this.difference) {
    //             this.fetcher.fetch(child);
    //         }
    //         return child;
    //     } catch (e) {
    //         return null;
    //     }
    // }

//     get previousSibling(): TrendTree | null {
//         return (
//             this.parent && (this.parent._leftChild === this ? this.parent.previousSibling && this.parent.previousSibling.rightChild : this.parent.leftChild)
//         );
//     }
//     get nextSibling(): TrendTree | null {
//         return (
//             this.parent && (this.parent._rightChild === this ? this.parent.nextSibling && this.parent.nextSibling.leftChild : this.parent.rightChild)
//         );
//     }
// }


// export class TreeNodeFetcher {
//     constructor() {

//     }
//     fetch(child: TrendTree) {

//     }
// }
