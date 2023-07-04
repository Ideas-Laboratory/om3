import {batchLoadDifWidthPostRawMinMax, loadDif, batchLoadDifWidthPost, batchLoadDifWidthPost1, batchLoadDif,batchLoadMinMaxMissWithWs, batchLoadAllDifWidthPost, batchLoadDifWidthPost1MinMax, batchLoadWithWs, batchLoadDifWidthPostForMinMaxMiss, batchLoadMinMaxMissWithPostForMultiLineType } from "./load_data"
import LevelIndexObj from "../model/level-index-obj";
import TrendTree from "@/helper/tend-query-tree";
import store, { pushTimeArray } from "@/store";


let nodeNum: Array<number> = [];

export async function batchLoadDataForRangeLevel1(losedRange: Array<Array<number>>, manager: any) {
    const difVals = await batchLoadDifWidthPost1(losedRange, manager);

    let count = 0;
    for (let i = 0; i < losedRange.length; i++) {
        const levelRange = losedRange[i];

        const startNode = manager.levelIndexObjs[losedRange[i][0]].getTreeNodeStartIndex(losedRange[i][1]);
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
                manager.cacheMap.set(firstNode.index, firstNode);
                manager.cacheMap.set(secondNode.index, secondNode);

                p = p.nextSibling;
                count++;
                if (p === null || count >= difVals.length) {
                    break;
                }

            } else {
                console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                console.log("lose range:", losedRange, p, p?.index, j);
                console.log(manager.levelIndexObjs);
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
        if (manager.levelIndexObjs[losedRange[i][0] + 1]) {
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0] + 1] = new LevelIndexObj(losedRange[i][0] + 1, false);
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
    }
}


export async function batchLoadDataForRangeLevel(losedRange: Array<Array<number>>, manager: any) {
    const difVals = await batchLoadDifWidthPost(losedRange, manager);
    
    let count = 0;
    let nodeNum = 0;
    for (let i = 0; i < losedRange.length; i++) {
        const levelRange = losedRange[i];

        const startNode = manager.levelIndexObjs[losedRange[i][0] - 1].getTreeNodeStartIndex(Math.floor(losedRange[i][1] / 2));
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
                manager.cacheMap.set(firstNode.index, firstNode);
                manager.cacheMap.set(secondNode.index, secondNode);
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
                console.log(manager.levelIndexObjs);
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
            const k1 = newTreeNode[j + 1].yArray[2] - newTreeNode[j].yArray[1];
            const k2 = newTreeNode[j + 1].yArray[1] - newTreeNode[j].yArray[2];
            newTreeNode[j].trendRange = k1 < k2 ? [newTreeNode[j + 1].yArray[2], newTreeNode[j].yArray[1], newTreeNode[j + 1].yArray[1], newTreeNode[j].yArray[2]] : [newTreeNode[j + 1].yArray[1], newTreeNode[j].yArray[2], newTreeNode[j + 1].yArray[2], newTreeNode[j].yArray[1]];
        }
        if (manager.levelIndexObjs[losedRange[i][0]]) {
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0]] = new LevelIndexObj(losedRange[i][0], false);
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
        // manager.updateDataCacheForRange(newTreeNode[0].level, newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index)
    }
    manager.addNodeNum(nodeNum);
}

export async function batchLoadDataForRangeLevelRawMinMax(losedRange: Array<Array<number>>, manager: any) {
    const difVals = await batchLoadDifWidthPostRawMinMax(losedRange, manager);
   // debugger
    let count = 0;
    let nodeNum = 0;
    for (let i = 0; i < losedRange.length; i++) {
        //debugger
        const levelRange = losedRange[i];

        const startNode = manager.levelIndexObjs[losedRange[i][0] - 1].getTreeNodeStartIndex(Math.floor(losedRange[i][1] / 2));
        let p = startNode;
        const newTreeNode = [];
        for (let j = Math.floor(losedRange[i][1] / 2); j <= Math.floor(losedRange[i][2] / 2); j++) {
            if (p?.index === j && 2*j === difVals[count].i && p.level+1 === difVals[count].l) {
                //@ts-ignore
                p.difference = difVals[count].dif;
                //@ts-ignore
                const yArray1: [number, number, number, number] = difVals[count].dif
                //@ts-ignore
                const yArray2: [number, number, number, number] = difVals[count+1].dif
               
                const firstNode = new TrendTree(p, true, p.index, yArray1, null);
                const secondNode = new TrendTree(p, false, p.index, yArray2, null);
               
                newTreeNode.push(firstNode);
                newTreeNode.push(secondNode);
                p = p.nextSibling;
                count+=2;
                if (p === null || count >= difVals.length) {
                    break;
                }

            } else {
                console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                console.log("lose range:", losedRange, p, p?.index, j);
                console.log(manager.levelIndexObjs);
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
        if (manager.levelIndexObjs[losedRange[i][0]]) {
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0]] = new LevelIndexObj(losedRange[i][0], false);
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
        // manager.updateDataCacheForRange(newTreeNode[0].level, newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index)
    }
    manager.addNodeNum(nodeNum);
}


export async function loadDataForRangeLevel(losedRange: Array<Array<number>>, manager: any) {
    if (losedRange.length <= 0) {
        return
    }
    let nodeNum = 0
    //await manager.batchLoadDataForRangeLevel(losedRange)
    for (let i = losedRange.length - 1; i >= 0; i--) {
        const levelRange = losedRange[i];
        const difVal = await loadDif(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2), Math.floor(losedRange[i][2] / 2));

        const startNode = manager.levelIndexObjs[losedRange[i][0] - 1].getTreeNodeStartIndex(Math.floor(losedRange[i][1] / 2));
        let p = startNode;
        const newTreeNode = [];
        let count = 0;
        for (let j = Math.floor(losedRange[i][1] / 2); j <= Math.floor(losedRange[i][2] / 2); j++) {
            if (p?.index === j) {
                //@ts-ignore
                p.difference = difVal[count];
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
                manager.cacheMap.set(firstNode.index, firstNode);
                manager.cacheMap.set(secondNode.index, secondNode);
                nodeNum += 2
                p = p.nextSibling;
                if (p === null) {
                    break;
                }
                count++;
            } else {
                console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                console.log("lose range:", losedRange, p, p?.index, j);
                console.log(manager.levelIndexObjs);
                debugger
                throw new Error("dif not match node");
            }
        }
        for (let j = 0; j < newTreeNode.length - 1; j++) {
            newTreeNode[j].nextSibling = newTreeNode[j + 1];
            if (newTreeNode[j].index != newTreeNode[j + 1].index - 1) {
                throw new Error("sibling index error");
            }
            newTreeNode[j + 1].previousSibling = newTreeNode[j];
            const k1 = newTreeNode[j + 1].yArray[2] - newTreeNode[j].yArray[1];
            const k2 = newTreeNode[j + 1].yArray[1] - newTreeNode[j].yArray[2];
            if (k1 < k2) {
                debugger
            }
            newTreeNode[j].trendRange = k1 < k2 ? [newTreeNode[j + 1].yArray[2], newTreeNode[j].yArray[1], newTreeNode[j + 1].yArray[1], newTreeNode[j].yArray[2]] : [newTreeNode[j + 1].yArray[1], newTreeNode[j].yArray[2], newTreeNode[j + 1].yArray[2], newTreeNode[j].yArray[1]];
        }
        if (manager.levelIndexObjs[losedRange[i][0]]) {
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0]] = new LevelIndexObj(losedRange[i][0], false);
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
    }
    manager.addNodeNum(nodeNum)
}













let currentNum = 0;
let currentTag = ""
export async function batchLoadDataForRangeLevel1WS(losedRange: Array<Array<number>>, manager: any, tagName?: string) {
    if (tagName === "empty") {
        nodeNum.push(currentNum);
        if(nodeNum.length==6){
            
            //pushTimeArray(nodeNum)
            nodeNum=[]
        }
        currentNum = 0;
        console.log("load node nums:", nodeNum);
        return
    }
    const difVals: Array<{ l: number, i: number }> = await batchLoadWithWs(losedRange, manager.dataName, "level_load_data", manager.maxLevel, tagName) as Array<{ l: number, i: number }>;
    if (currentTag === tagName) {
        currentNum += difVals.length;
    } else {
        currentTag = tagName!;
        currentNum += difVals.length;
    }

    let count = 0;
    for (let i = 0; i < losedRange.length; i++) {
        const levelRange = losedRange[i];

        const startNode = manager.levelIndexObjs[losedRange[i][0]].getTreeNodeStartIndex(losedRange[i][1]);
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
                manager.lruCache.set(firstNode.level+"_"+firstNode.index,firstNode);
                manager.lruCache.set(secondNode.level+"_"+secondNode.index,secondNode);

                p = p.nextSibling;
                count++;
                if (p === null || count >= difVals.length) {
                    break;
                }

            } else {
                console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                console.log("lose range:", losedRange, p, p?.index, j);
                console.log(manager.levelIndexObjs);
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
        if (manager.levelIndexObjs[losedRange[i][0] + 1]) {
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0] + 1] = new LevelIndexObj(losedRange[i][0] + 1, false);
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
    }
}


export async function batchLoadDataForRangeLevel1MinMaxMiss(losedRange: Array<Array<number>>, manager: any, tagName?: string){
    let difVals: Array<{ l: number, i: number, dif?: Array<number> }>
    if(store.state.controlParams.currentLineType==='Single'){
        difVals= await batchLoadMinMaxMissWithWs(losedRange, manager.dataName, "level_load_data_min_max_miss", manager.maxLevel, tagName) as Array<{ l: number, i: number }>;
    }else{
        difVals= await batchLoadMinMaxMissWithPostForMultiLineType(losedRange, manager.dataName, "level_load_data_min_max_miss", manager.maxLevel, tagName)
    }

    
    let count = 0;
    for (let i = 0; i < losedRange.length; i++) {
        const levelRange = losedRange[i];

        const startNode = manager.levelIndexObjs[losedRange[i][0]].getTreeNodeStartIndex(losedRange[i][1]);
        let p: TrendTree = startNode;
        const newTreeNode = [];
        for (let j = losedRange[i][1]; j <= losedRange[i][2];j++) {
        
           
            if (p?.index === j && j === difVals[count].i && p.level === difVals[count].l) {
                let dif = difVals[count].dif!;
                let curNodeType: "O" | "NULL" | "LEFTNULL" | "RIGHTNULL" = 'O';
                if (dif[1] === null && dif[2] === null) {
                    curNodeType = "NULL";
                } else if (dif[1] === null) {
                    curNodeType = "LEFTNULL"
                    p.gapFlag="L"
                } else if (dif[2] === null) {
                    curNodeType = "RIGHTNULL";
                    p.gapFlag="R"
                }
                if(curNodeType!=="O"){
                    p.nodeType = curNodeType
                }
                //@ts-ignore
                p.difference = difVals[count].dif;
                const yArray1: [any, any, any, any] = [undefined, undefined, undefined, undefined]
                const yArray2: [any, any, any, any] = [undefined, undefined, undefined, undefined]
                if (curNodeType === 'O') {
                    
                   
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
                } else if (curNodeType == "LEFTNULL") {
                   
                    yArray2[1] = p.yArray[1];
                    yArray2[2] = p.yArray[2];
                  
                } else if (curNodeType == "RIGHTNULL") {
                 
                    yArray1[1] = p.yArray[1];
                    yArray1[2] = p.yArray[2];
                  
                } 

                const firstNode = new TrendTree(p, true, p.index, yArray1, null);
                if (p.nodeType === 'LEFTNULL' || p.nodeType === 'NULL') {
                    firstNode.nodeType = 'NULL';
                }
                const secondNode = new TrendTree(p, false, p.index, yArray2, null);
                if (p.nodeType === 'RIGHTNULL' || p.nodeType == 'NULL') {
                    secondNode.nodeType = 'NULL';
                }
                // if(firstNode.nodeType==='NULL'){
                //     secondNode.gapFlag='L'
                // }
                // if(secondNode.nodeType==='NULL'){
                //     firstNode.gapFlag='R';
                // }
                newTreeNode.push(firstNode);
                newTreeNode.push(secondNode);
                manager.lruCache.set(firstNode.level+"_"+firstNode.index,firstNode);
                manager.lruCache.set(secondNode.level+"_"+secondNode.index,secondNode);
                p = p.nextSibling!;
                count++;
                if (p === null || count >= difVals.length) {
                    break;
                }

            } else {
                console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                console.log("lose range:", losedRange, p, p?.index, j);
                console.log(manager.levelIndexObjs);
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
        if (manager.levelIndexObjs[losedRange[i][0] + 1]) {
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0] + 1] = new LevelIndexObj(losedRange[i][0] + 1, false);
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
    }
}
export async function batchLoadDataForRangeLevel2MinMaxMiss(losedRange: Array<Array<number>>, manager: any, tagName?: string){
    let difVals: Array<{ l: number, i: number, dif?: Array<number> }>
    if(store.state.controlParams.currentLineType==='Single'){
        difVals= await batchLoadMinMaxMissWithWs(losedRange, manager.dataName, "level_load_data_min_max_miss", manager.maxLevel, tagName) as Array<{ l: number, i: number }>;
    }else{
        difVals= await batchLoadMinMaxMissWithPostForMultiLineType(losedRange, manager.dataName, "level_load_data_min_max_miss", manager.maxLevel, tagName)
    }

    
    let count = 0;
    for (let i = 0; i < losedRange.length; i++) {
        const levelRange = losedRange[i];

        const startNode = manager.levelIndexObjs[losedRange[i][0]].getTreeNodeStartIndex(losedRange[i][1]);
        let p: TrendTree = startNode;
        const newTreeNode = [];
        for (let j = losedRange[i][1]; j <= losedRange[i][2];j++) {
            if(p.nodeType==="NULL"){
                p=p.nextSibling!;
                //debugger
                continue
            }
            if(p?.index !== j || j !== difVals[count].i){
                continue
            }
           
            if (p?.index === j && j === difVals[count].i && p.level === difVals[count].l) {
                let dif = difVals[count].dif!;
                let curNodeType: "O" | "NULL" | "LEFTNULL" | "RIGHTNULL" = 'O';
                if (dif[1] === null && dif[2] === null) {
                    curNodeType = "NULL";
                } else if (dif[1] === null) {
                    curNodeType = "LEFTNULL"
                    p.gapFlag='L'
                } else if (dif[2] === null) {
                    curNodeType = "RIGHTNULL";
                    p.gapFlag='R'
                }
                if(curNodeType!=="O"){
                    p.nodeType = curNodeType
                }
                //@ts-ignore
                p.difference = difVals[count].dif;
                const yArray1: [any, any, any, any] = [undefined, undefined, undefined, undefined]
                const yArray2: [any, any, any, any] = [undefined, undefined, undefined, undefined]
                if (curNodeType === 'O') {
                    
                   
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
                } else if (curNodeType == "LEFTNULL") {
                   
                    yArray2[1] = p.yArray[1];
                    yArray2[2] = p.yArray[2];
                  
                } else if (curNodeType == "RIGHTNULL") {
                 
                    yArray1[1] = p.yArray[1];
                    yArray1[2] = p.yArray[2];
                  
                } 

                const firstNode = new TrendTree(p, true, p.index, yArray1, null);
                if (p.nodeType === 'LEFTNULL' || p.nodeType === 'NULL') {
                    firstNode.nodeType = 'NULL';
                }
                const secondNode = new TrendTree(p, false, p.index, yArray2, null);
                if (p.nodeType === 'RIGHTNULL' || p.nodeType == 'NULL') {
                    secondNode.nodeType = 'NULL';
                }
                newTreeNode.push(firstNode);
                newTreeNode.push(secondNode);
                manager.lruCache.set(firstNode.level+"_"+firstNode.index,firstNode);
                manager.lruCache.set(secondNode.level+"_"+secondNode.index,secondNode);
                p = p.nextSibling!;
                count++;
                if (p === null || count >= difVals.length) {
                    break;
                }

            } else {
                console.log(losedRange[i][0] - 1, Math.floor(losedRange[i][1] / 2))
                console.log("lose range:", losedRange, p, p?.index, j);
                console.log(manager.levelIndexObjs);
                debugger
                throw new Error("dif not match node");
            }
        }
        for (let j = 0; j < newTreeNode.length - 1; j++) {
            newTreeNode[j].nextSibling = newTreeNode[j + 1];
            newTreeNode[j + 1].previousSibling = newTreeNode[j];
            if (newTreeNode[j].index != newTreeNode[j + 1].index - 1) {
                console.error("sibling index error")
                //throw new Error("sibling index error");
            }

        }
        if (manager.levelIndexObjs[losedRange[i][0] + 1]) {
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0] + 1] = new LevelIndexObj(losedRange[i][0] + 1, false);
            manager.levelIndexObjs[losedRange[i][0] + 1].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
    }
}
export async function batchLoadDataForRangeLevelForMinMaxMiss(losedRange: Array<Array<number>>, manager: any) {
    const difVals = await batchLoadDifWidthPostForMinMaxMiss(losedRange, manager);
    let count = 0;
    let nodeNum = 0;
    debugger
    for (let i = 0; i < losedRange.length; i++) {
        const levelRange = losedRange[i];

        const startNode = manager.levelIndexObjs[losedRange[i][0] - 1].getTreeNodeStartIndex(Math.floor(losedRange[i][1] / 2));
        let p = startNode;
        const newTreeNode = [];
        for (let j = Math.floor(losedRange[i][1] / 2); j <= Math.floor(losedRange[i][2] / 2); j++) {
            if (p?.index === j && j === difVals[count].i && p.level === difVals[count].l) {
                let dif = difVals[count].dif!;
                let curNodeType: "O" | "NULL" | "LEFTNULL" | "RIGHTNULL" = 'O';
                if (dif[1] === null && dif[2] === null) {
                    throw new Error("data error")
                   // curNodeType = "NULL";
                } else if (dif[1] === null) {
                    curNodeType = "LEFTNULL"
                } else if (dif[2] === null) {
                    curNodeType = "RIGHTNULL";
                }
                if(curNodeType!=="O"){
                    p.nodeType = curNodeType
                }
               
                //@ts-ignore
                p.difference = difVals[count].dif;
                const yArray1: [number, number, number, number] = [0, 0, 0, 0]
                const yArray2: [number, number, number, number] = [0, 0, 0, 0]
                if(curNodeType==='O'){
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
                }else if(curNodeType==="LEFTNULL"){
                    yArray2[1] = p.yArray[1];
                    yArray2[2] = p.yArray[2];
                }else if(curNodeType==="RIGHTNULL"){
                    yArray1[1] = p.yArray[1];
                    yArray1[2] = p.yArray[2];
                }
               
                const firstNode = new TrendTree(p, true, p.index, yArray1, null);
                if (p.nodeType === 'LEFTNULL' || p.nodeType === 'NULL') {
                    firstNode.nodeType = 'NULL';
                }
                const secondNode = new TrendTree(p, false, p.index, yArray2, null);
                if (p.nodeType === 'RIGHTNULL' || p.nodeType == 'NULL') {
                    secondNode.nodeType = 'NULL';
                }
                manager.cacheMap.set(firstNode.index, firstNode);
                manager.cacheMap.set(secondNode.index, secondNode);
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
                console.log(manager.levelIndexObjs);
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
        debugger
        if (manager.levelIndexObjs[losedRange[i][0]]) {
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        } else {
            manager.levelIndexObjs[losedRange[i][0]] = new LevelIndexObj(losedRange[i][0], false);
            manager.levelIndexObjs[losedRange[i][0]].addLoadedDataRange(newTreeNode[0], [newTreeNode[0].index, newTreeNode[newTreeNode.length - 1].index]);
        }
    }
}

