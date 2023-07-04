export default class HaarTree {
    parent: HaarTree | null;
    _leftChild: HaarTree | null;
    _rightChild: HaarTree | null;
    value: number;
    difference: number | null;
    level: number;
    index: number;
    previousSibling: HaarTree | null;
    nextSibling: HaarTree | null;
    trendRange: Array<number>
    timeRange:Array<number>
    //fetcher: TreeNodeFetcher
    constructor(parent: HaarTree | null, leftChild = true, index = 0, value:number, dif?:number) {
       
        this.parent = parent;
        this._leftChild = null;
        this._rightChild = null;
        this.value = value;//Time-min,Val-min,Val-max,T-max
        this.difference = null;

        this.level = 0;
        this.previousSibling = null;
        this.nextSibling = null
        this.trendRange = [0, 0];
        
        if (dif!==undefined) {
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
        this.timeRange=[];
    }
    get levelNums() {
        return Math.pow(2, this.level);
    }

    getTimeRange(globalDataLen:number){
        if(this.timeRange.length>0){
            return [this.timeRange[0],this.timeRange[1]];
        }
        const nodeDataRange=globalDataLen/(2**this.level);
        this.timeRange[0]=this.index*nodeDataRange;
        this.timeRange[1]=this.timeRange[0]+nodeDataRange-1;
        return [this.timeRange[0],this.timeRange[1]];
    }
}