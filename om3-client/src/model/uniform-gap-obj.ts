import TrendTree from "@/helper/tend-query-tree"

export class UniformGapObj{
    leftMin:number
    leftMax:number
    rightMin:number
    rightMax:number
    isOk:boolean
    tOne:number
    tTwo:number
    canUseT:boolean
    firstNode:TrendTree
    secondNode:TrendTree
    constructor(node1:TrendTree,node2:TrendTree){
        this.canUseT=false
        this.isOk=false
        this.tOne=0
        this.tTwo=0
        this.leftMax=-Infinity
        this.leftMin=Infinity
        this.rightMax=-Infinity;
        this.rightMin=Infinity;
        this.firstNode=node1._rightChild!;
        this.secondNode=node2._leftChild!;

        if(node1._leftChild!.yArray[1]<this.leftMin){
            this.leftMin=node1._leftChild!.yArray[1]
        }
        if(node1._leftChild!.yArray[2]>this.leftMax){
            this.leftMax=node1._leftChild!.yArray[2]
        }
        if(node2._rightChild!.yArray[1]<this.rightMin){
            this.rightMin=node2._rightChild!.yArray[1];
        }
        if(node2._rightChild!.yArray[2]>this.rightMax){
            this.rightMax=node2._rightChild!.yArray[2]
        }
    }

    canCut(){
        const midMin=this.firstNode.yArray[1]<this.secondNode.yArray[1]?this.firstNode.yArray[1]:this.secondNode.yArray[1];
        const midMax=this.firstNode.yArray[2]>this.secondNode.yArray[2]?this.firstNode.yArray[2]:this.secondNode.yArray[2];
        if(midMin>=this.leftMin&&midMin>=this.rightMin&&midMax<=this.leftMax&&midMax<=this.rightMax){
            this.isOk=true;
            return true
        }else{
            return false;
        }
    }
    updateTwoNode(node1:TrendTree,node2:TrendTree){
        this.firstNode=node1._rightChild!;
        this.secondNode=node2._leftChild!;

        if(node1._leftChild!.yArray[1]<this.leftMin){
            this.leftMin=node1._leftChild!.yArray[1]
        }
        if(node1._leftChild!.yArray[2]>this.leftMax){
            this.leftMax=node1._leftChild!.yArray[2]
        }
        if(node2._rightChild!.yArray[1]<this.rightMin){
            this.rightMin=node2._rightChild!.yArray[1];
        }
        if(node2._rightChild!.yArray[2]>this.rightMax){
            this.rightMax=node2._rightChild!.yArray[2]
        }
    }
    lastLevelUpdateMinMax(){
        this.tTwo=this.secondNode.yArray[0];
        this.tOne=this.firstNode.yArray[3];
        this.canUseT=true
    }
    updateLeftMinMax(p:TrendTree){
        if(p.yArray[1]<this.leftMin){
            this.leftMin=p.yArray[1]
        }
        if(p.yArray[2]>this.leftMax){
            this.leftMax=p.yArray[2]
        }
    }
    updateRighetMinMax(p:TrendTree){
        if(p.yArray[1]<this.rightMin){
            this.rightMin=p.yArray[1]
        }
        if(p.yArray[2]>this.rightMax){
            this.rightMax=p.yArray[2]
        }
    }
}