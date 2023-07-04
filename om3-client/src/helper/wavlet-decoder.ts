import HaarDataManager from "@/model/haar-data-manager";
import HaarIndexObj from "@/model/haar-index-obj";
import LevelDataManager from "@/model/level-data-manager";
import LevelIndexObj from "@/model/level-index-obj";
import store from "@/store";
import HaarTree from "./haar-tree";
import TrendTree from "./tend-query-tree";

function initWaveletDecode(diffData: { mintd: Array<any>, minvd: Array<any>, maxvd: Array<any>, maxtd: Array<any> }) {
    let minT = diffData.mintd;
    let minV = diffData.minvd;
    let maxV = diffData.maxvd;
    let maxT = diffData.maxtd;

    for (let i = 0; i < Math.log2(maxV.length); i++) {
        const tempMaxV = [];
        const tempMaxT = [];
        const tempMinV = [];
        const tempMinT = [];
        for (let j = 0; j < 2 ** i; j++) {
            if (maxV[j + 2 ** i] >= 0) {
                tempMaxV.push(maxV[j]);
                tempMaxV.push(maxV[j] - maxV[j + 2 ** i]);
            } else {
                tempMaxV.push(maxV[j] + maxV[j + 2 ** i]);
                tempMaxV.push(maxV[j]);
            }
            if (minV[j + 2 ** i] >= 0) {
                tempMinV.push(minV[j] + minV[j + 2 ** i]);
                tempMinV.push(minV[j]);
            } else {
                tempMinV.push(minV[j]);
                tempMinV.push(minV[j] - minV[j + 2 ** i]);
            }
            tempMinT.push(minT[j]);
            tempMinT.push(minT[j] - minT[j + 2 ** i]);
            tempMaxT.push(maxT[j] + maxT[j + 2 ** i]);
            tempMaxT.push(maxT[j]);
        }
        maxV = [...tempMaxV, ...maxV.slice(2 ** (i + 1))];
        maxT = [...tempMaxT, ...maxT.slice(2 ** (i + 1))];
        minV = [...tempMinV, ...minV.slice(2 ** (i + 1))];
        minT = [...tempMinT, ...minT.slice(2 ** (i + 1))];
    }
    const m4Data = {
        mint: minT,
        minv: minV,
        maxv: maxV,
        maxt: maxT
    }
    return m4Data;
}

export function initHaarDecode(dif: Array<number>) {
    let minT = dif
    for (let i = 0; i < Math.log2(minT.length); i++) {
        const tempMinT = [];
        for (let j = 0; j < 2 ** i; j++) {
            const a = (2 * minT[j] + minT[j + 2 ** i]) / 2;
            tempMinT.push(a);
            const b = (2 * minT[j] - minT[j + 2 ** i]) / 2;
            tempMinT.push(b);
        }
        minT = [...tempMinT, ...minT.slice(2 ** (i + 1))];
    }
    return minT;
}

function waveletDecode(histroryData: Array<any>, diffData: { mintd: Array<any>, minvd: Array<any>, maxvd: Array<any>, maxtd: Array<any> }, range: [number, number]) {
    const historyMinT = histroryData[1].slice(range[0], range[1]);
    const historyMinV = histroryData[2].slice(range[0], range[1]);
    const historyMaxV = histroryData[3].slice(range[0], range[1]);
    const historyMaxT = histroryData[4].slice(range[0], range[1]);
    const difMinT = diffData.mintd;
    const difMinV = diffData.minvd;
    const difMaxV = diffData.maxvd;
    const difMaxT = diffData.maxtd;
    const nowMinT = [];
    const nowMinV = [];
    const nowMaxV = [];
    const nowMaxT = [];
    for (let i = 0; i < historyMinT.length; i++) {
        nowMinT.push(historyMinT[i], historyMinT[i] - difMinT[i]);
        if (difMinV[i] < 0) {
            nowMinV.push(historyMinV[i], historyMinV[i] - difMinV[i])
        } else {
            nowMinV.push(historyMinV[i] + difMinV[i], historyMinV[i]);
        }
        if (difMaxV[i] < 0) {
            nowMaxV.push(historyMaxV[i] + difMaxV[i], historyMaxV[i]);
        } else {
            nowMaxV.push(historyMaxV[i], historyMaxV[i] - difMaxV[i]);
        }
        nowMaxT.push(historyMaxT[i] + difMaxT[i], historyMaxT[i]);
    }
    return {
        mint: nowMinT,
        minv: nowMinV,
        maxv: nowMaxV,
        maxt: nowMaxT,
    }

}

function constructHaarTree(data: Array<any>, tableName?: string) {
    const levelIndex = new Array<HaarIndexObj>(Math.log2(data.length));
    //const dataManager=new LevelDataManager()
    const mintd: Array<number> = [];
    data.forEach(v => {
        mintd.push(v.dif);
    });

    let tempLast = mintd.pop();
    mintd.unshift(tempLast!);

    let lastLevelNodes = new Array<HaarTree>();
    let currentLevelNodes = new Array<HaarTree>();
    const root = new HaarTree(null, true, 0, mintd[0], mintd[1]);
    lastLevelNodes.push(root);
    levelIndex[0] = new HaarIndexObj(0, true);
    levelIndex[0].addLoadedDataRange(root, [0, 0]);
    for (let i = 1; i <= Math.log2(data.length); i++) {
        for (let j = 0; j < 2 ** (i - 1); j++) {

            const lastNode = lastLevelNodes[j];
            const a = (2 * lastNode.value + lastNode.difference!) / 2
            const b = (2 * lastNode.value - lastNode.difference!) / 2;
            const firstNode = new HaarTree(lastNode, true, lastNode.index, a, i === Math.log2(data.length) ? undefined : mintd[j * 2 + 2 ** i]);
            const secondNode = new HaarTree(lastNode, false, lastNode.index, b, i === Math.log2(data.length) ? undefined : mintd[j * 2 + 1 + 2 ** i]);
            currentLevelNodes.push(firstNode);
            currentLevelNodes.push(secondNode);
        }
        levelIndex[i] = new HaarIndexObj(currentLevelNodes[0].level, true);
        levelIndex[i].addLoadedDataRange(currentLevelNodes[0], [0, currentLevelNodes.length - 1]);
        for (let i = 0; i < currentLevelNodes.length - 1; i++) {
            currentLevelNodes[i].nextSibling = currentLevelNodes[i + 1];
            currentLevelNodes[i + 1].previousSibling = currentLevelNodes[i];
        }
        lastLevelNodes = currentLevelNodes;
        currentLevelNodes = [];
    }
    return {
        dataManager: new HaarDataManager(levelIndex, tableName ? tableName : store.state.controlParams.currentTable),
        trendTree: root
    }
}

function constructTrendTree(data: Array<any>, tableName?: string) {
    const levelIndex = new Array<LevelIndexObj>(Math.log2(data.length));
    //const dataManager=new LevelDataManager()
    const mintd: Array<number> = [];
    const minvd: Array<number> = [];
    const maxvd: Array<number> = [];
    const maxtd: Array<number> = [];
    data.forEach(v => {
        mintd.push(v.minid);
        minvd.push(v.minvd);
        maxvd.push(v.maxvd);
        maxtd.push(v.maxid);
    });

    let tempLast = mintd.pop();
    mintd.unshift(tempLast!);
    tempLast = minvd.pop();
    minvd.unshift(tempLast!);
    tempLast = maxvd.pop();
    maxvd.unshift(tempLast!);
    tempLast = maxtd.pop();
    maxtd.unshift(tempLast!);
    let lastLevelNodes = new Array<TrendTree>();
    let currentLevelNodes = new Array<TrendTree>();
    let nodeNum = 1
    const root = new TrendTree(null, true, 0, [mintd[0], minvd[0], maxvd[0], maxtd[0]], [mintd[1], minvd[1], maxvd[1], maxtd[1]]);
    lastLevelNodes.push(root);
    levelIndex[0] = new LevelIndexObj(0, true);
    levelIndex[0].addLoadedDataRange(root, [0, 0]);
    for (let i = 1; i <= Math.log2(data.length); i++) {
        for (let j = 0; j < 2 ** (i - 1); j++) {

            const lastNode = lastLevelNodes[j];
            //@ts-ignore
            const yArray1: [number, number, number, number] = new Array(4);
            //@ts-ignore
            const yArray2: [number, number, number, number] = new Array(4);
            yArray1[0] = lastNode.yArray[0];
            yArray2[0] = lastNode.yArray[0] - lastNode.difference![0];
            if (lastNode.difference![1] >= 0) {
                yArray1[1] = lastNode.yArray[1] + lastNode.difference![1];
                yArray2[1] = lastNode.yArray[1];
            } else {
                yArray1[1] = lastNode.yArray[1];
                yArray2[1] = lastNode.yArray[1] - lastNode.difference![1];
            }
            if (lastNode.difference![2] >= 0) {
                yArray1[2] = lastNode.yArray[2];
                yArray2[2] = lastNode.yArray[2] - lastNode.difference![2];
            } else {
                yArray1[2] = lastNode.yArray[2] + lastNode.difference![2];
                yArray2[2] = lastNode.yArray[2];
            }
            yArray1[3] = lastNode.yArray[3] + lastNode.difference![3];
            yArray2[3] = lastNode.yArray[3];
            //@ts-ignore
            const firstNode = new TrendTree(lastNode, true, lastNode.index, yArray1, i === Math.log2(data.length) ? null : [mintd[j * 2 + 2 ** i], minvd[j * 2 + 2 ** i], maxvd[j * 2 + 2 ** i], maxtd[j * 2 + 2 ** i]]);
            //@ts-ignore
            const secondNode = new TrendTree(lastNode, false, lastNode.index, yArray2, i === Math.log2(data.length) ? null : [mintd[j * 2 + 1 + 2 ** i], minvd[j * 2 + 1 + 2 ** i], maxvd[j * 2 + 1 + 2 ** i], maxtd[j * 2 + 1 + 2 ** i]]);
            // if (j === 0) {
            //     levelIndex[i]=new LevelIndexObj(firstNode.level,true);
            //     levelIndex[i].addLoadedDataRange(firstNode,[0,])
            // }
            currentLevelNodes.push(firstNode);
            currentLevelNodes.push(secondNode);
            nodeNum += 2
        }
        levelIndex[i] = new LevelIndexObj(currentLevelNodes[0].level, true);
        levelIndex[i].addLoadedDataRange(currentLevelNodes[0], [0, currentLevelNodes.length - 1]);
        for (let i = 0; i < currentLevelNodes.length - 1; i++) {
            currentLevelNodes[i].nextSibling = currentLevelNodes[i + 1];
            currentLevelNodes[i + 1].previousSibling = currentLevelNodes[i];
            // const k1 = currentLevelNodes[i + 1].yArray[2] - currentLevelNodes[i].yArray[1];
            // const k2 = currentLevelNodes[i + 1].yArray[1] - currentLevelNodes[i].yArray[2];
            // currentLevelNodes[i].trendRange = k1 < k2 ? [currentLevelNodes[i + 1].yArray[2], currentLevelNodes[i].yArray[1], currentLevelNodes[i + 1].yArray[1], currentLevelNodes[i].yArray[2]] : [currentLevelNodes[i + 1].yArray[1], currentLevelNodes[i].yArray[2], currentLevelNodes[i + 1].yArray[2], currentLevelNodes[i].yArray[1]];
        }
        lastLevelNodes = currentLevelNodes;
        currentLevelNodes = [];
    }
    const levelDataManager = new LevelDataManager(levelIndex, tableName ? tableName : store.state.controlParams.currentTable);
    // levelDataManager.addNodeNum(nodeNum)
    // console.log(levelDataManager.curNodeNum,levelDataManager.cacheMap.size)
    return {
        dataManager: levelDataManager,
        trendTree: root
    }
}

function constructRawMinMaxTrendTree(data: Array<any>, tableName?: string) {
    const levelIndex = new Array<LevelIndexObj>(Math.ceil(Math.log2(data.length)));
    //const dataManager=new LevelDataManager()
    const mintd: Array<number> = [];
    const minvd: Array<number> = [];
    const maxvd: Array<number> = [];
    const maxtd: Array<number> = [];
    data.forEach(v => {
        mintd.push(v.mint);
        minvd.push(v.minv);
        maxvd.push(v.maxv);
        maxtd.push(v.maxt);
    });

    let tempLast = mintd.pop();
    mintd.unshift(tempLast!);
    tempLast = minvd.pop();
    minvd.unshift(tempLast!);
    tempLast = maxvd.pop();
    maxvd.unshift(tempLast!);
    tempLast = maxtd.pop();
    maxtd.unshift(tempLast!);
    let lastLevelNodes = new Array<TrendTree>();
    let currentLevelNodes = new Array<TrendTree>();
    let nodeNum = 1
    const root = new TrendTree(null, true, 0, [mintd[0], minvd[0], maxvd[0], maxtd[0]], [0, 0, 0, 0]);
    lastLevelNodes.push(root);
    levelIndex[0] = new LevelIndexObj(0, true);
    levelIndex[0].addLoadedDataRange(root, [0, 0]);
    for (let i = 1; i <= Math.log2(data.length); i++) {
        for (let j = 0; j < 2 ** (i - 1); j++) {

            const lastNode = lastLevelNodes[j];

            //@ts-ignore
            const firstNode = new TrendTree(lastNode, true, lastNode.index, [mintd[j * 2 + 2 ** i], minvd[j * 2 + 2 ** i], maxvd[j * 2 + 2 ** i], maxtd[j * 2 + 2 ** i]], [0, 0, 0, 0]);
            //@ts-ignore
            const secondNode = new TrendTree(lastNode, false, lastNode.index, [mintd[j * 2 + 1 + 2 ** i], minvd[j * 2 + 1 + 2 ** i], maxvd[j * 2 + 1 + 2 ** i], maxtd[j * 2 + 1 + 2 ** i]], [0, 0, 0, 0]);

            currentLevelNodes.push(firstNode);
            currentLevelNodes.push(secondNode);
            nodeNum += 2
        }
        levelIndex[i] = new LevelIndexObj(currentLevelNodes[0].level, true);
        levelIndex[i].addLoadedDataRange(currentLevelNodes[0], [0, currentLevelNodes.length - 1]);
        for (let i = 0; i < currentLevelNodes.length - 1; i++) {
            currentLevelNodes[i].nextSibling = currentLevelNodes[i + 1];
            currentLevelNodes[i + 1].previousSibling = currentLevelNodes[i];

        }
        lastLevelNodes = currentLevelNodes;
        currentLevelNodes = [];
    }
    const levelDataManager = new LevelDataManager(levelIndex, tableName ? tableName : store.state.controlParams.currentTable);

    return {
        dataManager: levelDataManager,
        trendTree: root
    }
}
function constructMinMaxTrendTree(data: Array<any>, tableName?: string) {
    console.log(data.length);
    const levelIndex = new Array<LevelIndexObj>(Math.ceil(Math.log2(data.length)));
    //const dataManager=new LevelDataManager()

    const minvd: Array<number> = [];
    const maxvd: Array<number> = [];

    data.forEach(v => {

        minvd.push(v.minvd);
        maxvd.push(v.maxvd);
    });


    let tempLast = minvd.pop();
    minvd.unshift(tempLast!);
    tempLast = maxvd.pop();
    maxvd.unshift(tempLast!);
    let lastLevelNodes = new Array<TrendTree>();
    let currentLevelNodes = new Array<TrendTree>();
    let nodeNum = 1
    const root = new TrendTree(null, true, 0, [0, minvd[0], maxvd[0], 0], [0, minvd[1], maxvd[1], 0]);
    lastLevelNodes.push(root);
    levelIndex[0] = new LevelIndexObj(0, true);
    levelIndex[0].addLoadedDataRange(root, [0, 0]);
    for (let i = 1; i <= Math.log2(data.length); i++) {
        for (let j = 0; j < 2 ** (i - 1); j++) {

            const lastNode = lastLevelNodes[j];
            //@ts-ignore
            const yArray1: [number, number, number, number] = new Array(4);
            //@ts-ignore
            const yArray2: [number, number, number, number] = new Array(4);
            yArray1[0] = lastNode.yArray[0];
            yArray2[0] = lastNode.yArray[0] - lastNode.difference![0];
            if (lastNode.difference![1] >= 0) {
                yArray1[1] = lastNode.yArray[1] + lastNode.difference![1];
                yArray2[1] = lastNode.yArray[1];
            } else {
                yArray1[1] = lastNode.yArray[1];
                yArray2[1] = lastNode.yArray[1] - lastNode.difference![1];
            }
            if (lastNode.difference![2] >= 0) {
                yArray1[2] = lastNode.yArray[2];
                yArray2[2] = lastNode.yArray[2] - lastNode.difference![2];
            } else {
                yArray1[2] = lastNode.yArray[2] + lastNode.difference![2];
                yArray2[2] = lastNode.yArray[2];
            }
            yArray1[3] = lastNode.yArray[3] + lastNode.difference![3];
            yArray2[3] = lastNode.yArray[3];
            //@ts-ignore
            const firstNode = new TrendTree(lastNode, true, lastNode.index, yArray1, i === Math.log2(data.length) ? null : [0, minvd[j * 2 + 2 ** i], maxvd[j * 2 + 2 ** i], 0]);
            //@ts-ignore
            const secondNode = new TrendTree(lastNode, false, lastNode.index, yArray2, i === Math.log2(data.length) ? null : [0, minvd[j * 2 + 1 + 2 ** i], maxvd[j * 2 + 1 + 2 ** i], 0]);
            // if (j === 0) {
            //     levelIndex[i]=new LevelIndexObj(firstNode.level,true);
            //     levelIndex[i].addLoadedDataRange(firstNode,[0,])
            // }
            currentLevelNodes.push(firstNode);
            currentLevelNodes.push(secondNode);
            nodeNum += 2
        }
        levelIndex[i] = new LevelIndexObj(currentLevelNodes[0].level, true);
        levelIndex[i].addLoadedDataRange(currentLevelNodes[0], [0, currentLevelNodes.length - 1]);
        for (let i = 0; i < currentLevelNodes.length - 1; i++) {
            currentLevelNodes[i].nextSibling = currentLevelNodes[i + 1];
            currentLevelNodes[i + 1].previousSibling = currentLevelNodes[i];
            // const k1 = currentLevelNodes[i + 1].yArray[2] - currentLevelNodes[i].yArray[1];
            // const k2 = currentLevelNodes[i + 1].yArray[1] - currentLevelNodes[i].yArray[2];
            // currentLevelNodes[i].trendRange = k1 < k2 ? [currentLevelNodes[i + 1].yArray[2], currentLevelNodes[i].yArray[1], currentLevelNodes[i + 1].yArray[1], currentLevelNodes[i].yArray[2]] : [currentLevelNodes[i + 1].yArray[1], currentLevelNodes[i].yArray[2], currentLevelNodes[i + 1].yArray[2], currentLevelNodes[i].yArray[1]];
        }
        lastLevelNodes = currentLevelNodes;
        currentLevelNodes = [];
    }
    const levelDataManager = new LevelDataManager(levelIndex, tableName ? tableName : store.state.controlParams.currentTable);
    // levelDataManager.addNodeNum(nodeNum)
    // console.log(levelDataManager.curNodeNum,levelDataManager.cacheMap.size)
    return {
        dataManager: levelDataManager,
        trendTree: root
    }
}


function constructMissTrendTree(data: Array<any>, tableName?: string) {
    const levelIndex = new Array<LevelIndexObj>(Math.ceil(Math.log2(data.length)));
    //const dataManager=new LevelDataManager()
    const mintd: Array<number> = [];
    const minvd: Array<number> = [];
    const maxvd: Array<number> = [];
    const maxtd: Array<number> = [];
    data.forEach(v => {
        mintd.push(v.minid);
        minvd.push(v.minvd);
        maxvd.push(v.maxvd);
        maxtd.push(v.maxid);
    });

    let tempLast = mintd.pop();
    mintd.unshift(tempLast!);
    tempLast = minvd.pop();
    minvd.unshift(tempLast!);
    tempLast = maxvd.pop();
    maxvd.unshift(tempLast!);
    tempLast = maxtd.pop();
    maxtd.unshift(tempLast!);
    let lastLevelNodes = new Array<TrendTree>();
    let currentLevelNodes = new Array<TrendTree>();
    let nodeNum = 1
    const root = new TrendTree(null, true, 0, [mintd[0], minvd[0], maxvd[0], maxtd[0]], [mintd[1], minvd[1], maxvd[1], maxtd[1]]);
    lastLevelNodes.push(root);
    levelIndex[0] = new LevelIndexObj(0, true);
    levelIndex[0].addLoadedDataRange(root, [0, 0]);
    for (let i = 1; i <= Math.log2(data.length); i++) {
        for (let j = 0; j < 2 ** (i - 1); j++) {

            const lastNode = lastLevelNodes[j];
            let dif = lastNode.difference!;
            let curNodeType: "O" | "NULL" | "LEFTNULL" | "RIGHTNULL" = 'O';
            if (dif[1] === null && dif[2] === null) {
                curNodeType = "NULL";
            } else if (dif[0] === null && dif[1] === null) {
                curNodeType = "LEFTNULL"
            } else if (dif[2] === null && dif[3] === null) {
                curNodeType = "RIGHTNULL";
            }
            lastNode.nodeType = curNodeType;
            const yArray1: [any, any, any, any] = [undefined, undefined, undefined, undefined]
            const yArray2: [any, any, any, any] = [undefined, undefined, undefined, undefined]
            if (curNodeType === 'O') {
                yArray1[0] = lastNode.yArray[0];
                yArray2[0] = lastNode.yArray[0] - lastNode.difference![0];
                yArray1[3] = lastNode.yArray[3] + lastNode.difference![3];
                yArray2[3] = lastNode.yArray[3];
                if (lastNode.difference![1] < 0) {
                    yArray1[1] = lastNode.yArray[1];
                    yArray2[1] = lastNode.yArray[1] - lastNode.difference![1];
                } else {
                    yArray1[1] = lastNode.yArray[1] + lastNode.difference![1];
                    yArray2[1] = lastNode.yArray[1]
                }
                if (lastNode.difference![2] < 0) {
                    yArray1[2] = lastNode.yArray[2] + lastNode.difference![2];
                    yArray2[2] = lastNode.yArray[2];
                } else {
                    yArray1[2] = lastNode.yArray[2];
                    yArray2[2] = lastNode.yArray[2] - lastNode.difference![2];
                }
            } else if (curNodeType == "LEFTNULL") {
                yArray2[0] = lastNode.yArray[0]
                yArray2[1] = lastNode.yArray[1];
                yArray2[2] = lastNode.yArray[2];
                yArray2[3] = lastNode.yArray[3];
            } else if (curNodeType == "RIGHTNULL") {
                yArray1[0] = lastNode.yArray[0]
                yArray1[1] = lastNode.yArray[1];
                yArray1[2] = lastNode.yArray[2];
                yArray1[3] = lastNode.yArray[3];
            } else if (curNodeType === 'NULL') {
                //console.log("null node")
            }
            else {
                throw new Error("type error")

            }

            const firstNode = new TrendTree(lastNode, true, lastNode.index, yArray1, i === Math.log2(data.length) ? null : [mintd[j * 2 + 2 ** i], minvd[j * 2 + 2 ** i], maxvd[j * 2 + 2 ** i], maxtd[j * 2 + 2 ** i]]);
            if (lastNode.nodeType === 'LEFTNULL' || lastNode.nodeType === 'NULL') {
                firstNode.nodeType = 'NULL';
            }
            //@ts-ignore
            const secondNode = new TrendTree(lastNode, false, lastNode.index, yArray2, i === Math.log2(data.length) ? null : [mintd[j * 2 + 1 + 2 ** i], minvd[j * 2 + 1 + 2 ** i], maxvd[j * 2 + 1 + 2 ** i], maxtd[j * 2 + 1 + 2 ** i]]);
            if (lastNode.nodeType === 'RIGHTNULL' || lastNode.nodeType == 'NULL') {
                secondNode.nodeType = 'NULL';
            }
            // if (j === 0) {
            //     levelIndex[i]=new LevelIndexObj(firstNode.level,true);
            //     levelIndex[i].addLoadedDataRange(firstNode,[0,])
            // }
            currentLevelNodes.push(firstNode);
            currentLevelNodes.push(secondNode);
            nodeNum += 2
        }
        levelIndex[i] = new LevelIndexObj(currentLevelNodes[0].level, true);
        levelIndex[i].addLoadedDataRange(currentLevelNodes[0], [0, currentLevelNodes.length - 1]);
        for (let i = 0; i < currentLevelNodes.length - 1; i++) {
            currentLevelNodes[i].nextSibling = currentLevelNodes[i + 1];
            currentLevelNodes[i + 1].previousSibling = currentLevelNodes[i];
            // const k1 = currentLevelNodes[i + 1].yArray[2] - currentLevelNodes[i].yArray[1];
            // const k2 = currentLevelNodes[i + 1].yArray[1] - currentLevelNodes[i].yArray[2];
            // currentLevelNodes[i].trendRange = k1 < k2 ? [currentLevelNodes[i + 1].yArray[2], currentLevelNodes[i].yArray[1], currentLevelNodes[i + 1].yArray[1], currentLevelNodes[i].yArray[2]] : [currentLevelNodes[i + 1].yArray[1], currentLevelNodes[i].yArray[2], currentLevelNodes[i + 1].yArray[2], currentLevelNodes[i].yArray[1]];
        }
        lastLevelNodes = currentLevelNodes;
        currentLevelNodes = [];
    }
    const levelDataManager = new LevelDataManager(levelIndex, tableName ? tableName : store.state.controlParams.currentTable);
    // levelDataManager.addNodeNum(nodeNum)
    // console.log(levelDataManager.curNodeNum,levelDataManager.cacheMap.size)
    return {
        dataManager: levelDataManager,
        trendTree: root
    }
}
function constructMinMaxMissTrendTree(data: Array<any>, width: number, tableName?: string) {
    const initLevel = Math.ceil(Math.log2(width));
    //debugger
    const levelIndex = new Array<LevelIndexObj>(Math.ceil(Math.log2(data.length)) + 1);

    const minvd: Array<number> = [];
    const maxvd: Array<number> = [];

    data.forEach(v => {
        minvd.push(v.minvd);
        maxvd.push(v.maxvd);
    });


    let tempLast = minvd.pop();
    minvd.unshift(tempLast!);
    tempLast = maxvd.pop();
    maxvd.unshift(tempLast!);

    let lastLevelNodes = new Array<TrendTree>();
    let currentLevelNodes = new Array<TrendTree>();
    let nodeNum = 1;
    //@ts-ignore
    const root = new TrendTree(null, true, 0, [undefined, minvd[0], maxvd[0], undefined], [undefined, minvd[1], maxvd[1], undefined]);
    lastLevelNodes.push(root);
    levelIndex[0] = new LevelIndexObj(0, true);
    levelIndex[0].addLoadedDataRange(root, [0, 0]);

    let difIndex = 1;
    for (let i = 1; i <= initLevel; i++) {
        for (let j = 0; j < lastLevelNodes.length; j++) {
            const lastNode = lastLevelNodes[j];
            if (lastNode.nodeType === "NULL") {
                //debugger
                continue
            }
            if(difIndex===minvd.length){
                debugger
                throw new Error("diff index error")
            }
            let dif = [minvd[difIndex], maxvd[difIndex]];
            difIndex++;
            let curNodeType: "O" | "NULL" | "LEFTNULL" | "RIGHTNULL" = 'O';
            if (dif[0] === null && dif[1] === null) {
                throw new Error("data error")
                // curNodeType = "NULL";
            } else if (dif[0] === null) {
                curNodeType = "LEFTNULL"
                lastNode.gapFlag='L'
            } else if (dif[1] === null) {
                
                curNodeType = "RIGHTNULL";
                lastNode.gapFlag='R'
            }
            if (curNodeType !== "O") {
                lastNode.nodeType = curNodeType
            }
            const yArray1: [any, any, any, any] = [undefined, undefined, undefined, undefined]
            const yArray2: [any, any, any, any] = [undefined, undefined, undefined, undefined]
            if (curNodeType === 'O') {
                if (dif[0] < 0) {
                    yArray1[1] = lastNode.yArray[1];
                    yArray2[1] = lastNode.yArray[1] - dif[0];
                } else {
                    yArray1[1] = lastNode.yArray[1] + dif[0];
                    yArray2[1] = lastNode.yArray[1];
                }
                if (dif[1] < 0) {
                    yArray1[2] = lastNode.yArray[2] + dif[1];
                    yArray2[2] = lastNode.yArray[2];
                } else {
                    yArray1[2] = lastNode.yArray[2];
                    yArray2[2] = lastNode.yArray[2] - dif[1];
                }
            } else if (curNodeType == "LEFTNULL") {
                yArray2[1] = lastNode.yArray[1];
                yArray2[2] = lastNode.yArray[2];
            } else if (curNodeType == "RIGHTNULL") {
                yArray1[1] = lastNode.yArray[1];
                yArray1[2] = lastNode.yArray[2];
            } else if (curNodeType === 'NULL') {
                debugger
                //console.log("null node")
            }
            else {
                throw new Error("type error")

            }

            //@ts-ignore
            const firstNode = new TrendTree(lastNode, true, lastNode.index, yArray1, null, "O");
            if (curNodeType === "LEFTNULL") {
                firstNode.nodeType = 'NULL';
            }

            //@ts-ignore
            const secondNode = new TrendTree(lastNode, false, lastNode.index, yArray2, null,"O");
            if (curNodeType === "RIGHTNULL") {
                secondNode.nodeType = 'NULL';
            }
            currentLevelNodes.push(firstNode);
            currentLevelNodes.push(secondNode);
            nodeNum += 2


        }
        levelIndex[i] = new LevelIndexObj(currentLevelNodes[0].level, true);
        levelIndex[i].addLoadedDataRange(currentLevelNodes[0], [currentLevelNodes[0].index, currentLevelNodes[currentLevelNodes.length-1].index]);
        for (let i = 0; i < currentLevelNodes.length - 1; i++) {
            currentLevelNodes[i].nextSibling = currentLevelNodes[i + 1];
            currentLevelNodes[i + 1].previousSibling = currentLevelNodes[i];

        }
        lastLevelNodes = currentLevelNodes;
        currentLevelNodes = [];
    }
    if(difIndex!==minvd.length){
        debugger
        throw new Error("diff not uesed")
    }


    // debugger
    // for (let i = 1; i <= Math.ceil(Math.log2(data.length)); i++) {
    //     for (let j = 0; j < 2 ** (i - 1); j++) {

    //         const lastNode = lastLevelNodes[j];
    //         let dif = lastNode.difference!;
    //         let curNodeType: "O" | "NULL" | "LEFTNULL" | "RIGHTNULL" = 'O';
    //         if (dif[1] === null && dif[2] === null) {
    //             throw new Error("data error")
    //             // curNodeType = "NULL";
    //         } else if (dif[1] === null) {
    //             curNodeType = "LEFTNULL"
    //         } else if (dif[2] === null) {
    //             debugger
    //             curNodeType = "RIGHTNULL";
    //         }
    //         if (curNodeType !== "O") {
    //             lastNode.nodeType = curNodeType
    //         }
    //         const yArray1: [any, any, any, any] = [undefined, undefined, undefined, undefined]
    //         const yArray2: [any, any, any, any] = [undefined, undefined, undefined, undefined]
    //         if (curNodeType === 'O') {
    //             if (lastNode.difference![1] < 0) {
    //                 yArray1[1] = lastNode.yArray[1];
    //                 yArray2[1] = lastNode.yArray[1] - lastNode.difference![1];
    //             } else {
    //                 yArray1[1] = lastNode.yArray[1] + lastNode.difference![1];
    //                 yArray2[1] = lastNode.yArray[1];
    //             }
    //             if (lastNode.difference![2] < 0) {
    //                 yArray1[2] = lastNode.yArray[2] + lastNode.difference![2];
    //                 yArray2[2] = lastNode.yArray[2];
    //             } else {
    //                 yArray1[2] = lastNode.yArray[2];
    //                 yArray2[2] = lastNode.yArray[2] - lastNode.difference![2];
    //             }
    //         } else if (curNodeType == "LEFTNULL") {
    //             yArray2[1] = lastNode.yArray[1];
    //             yArray2[2] = lastNode.yArray[2];
    //         } else if (curNodeType == "RIGHTNULL") {
    //             yArray1[1] = lastNode.yArray[1];
    //             yArray1[2] = lastNode.yArray[2];
    //         } else if (curNodeType === 'NULL') {
    //             debugger
    //             //console.log("null node")
    //         }
    //         else {
    //             throw new Error("type error")

    //         }

    //         //@ts-ignore
    //         const firstNode = new TrendTree(lastNode, true, lastNode.index, yArray1, i === Math.log2(data.length) ? null : [undefined, minvd[j * 2 + 2 ** i], maxvd[j * 2 + 2 ** i], undefined]);
    //         if (lastNode.nodeType === "LEFTNULL") {
    //             debugger
    //             firstNode.nodeType = 'NULL';
    //         }
    //         //@ts-ignore
    //         const secondNode = new TrendTree(lastNode, false, lastNode.index, yArray2, i === Math.log2(data.length) ? null : [undefined, minvd[j * 2 + 1 + 2 ** i], maxvd[j * 2 + 1 + 2 ** i], undefined]);
    //         if (lastNode.nodeType === "RIGHTNULL") {
    //             debugger
    //             secondNode.nodeType = 'NULL';
    //         }
    //         currentLevelNodes.push(firstNode);
    //         currentLevelNodes.push(secondNode);
    //         nodeNum += 2
    //     }
    //     levelIndex[i] = new LevelIndexObj(currentLevelNodes[0].level, true);
    //     levelIndex[i].addLoadedDataRange(currentLevelNodes[0], [0, currentLevelNodes.length - 1]);
    //     for (let i = 0; i < currentLevelNodes.length - 1; i++) {
    //         currentLevelNodes[i].nextSibling = currentLevelNodes[i + 1];
    //         currentLevelNodes[i + 1].previousSibling = currentLevelNodes[i];

    //     }
    //     lastLevelNodes = currentLevelNodes;
    //     currentLevelNodes = [];
    // }
    //debugger
    const levelDataManager = new LevelDataManager(levelIndex, tableName ? tableName : store.state.controlParams.currentTable);
    return {
        dataManager: levelDataManager,
        trendTree: root
    }
}

export { constructRawMinMaxTrendTree, constructMinMaxMissTrendTree, constructMissTrendTree, initWaveletDecode, waveletDecode, constructTrendTree, constructHaarTree, constructMinMaxTrendTree }