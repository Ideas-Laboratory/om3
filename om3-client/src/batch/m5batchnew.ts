import LevelDataManager from "@/model/level-data-manager";
import { formatToRenderDataForTrend, getIndexTime } from "../helper/format-data";
import axios from "axios";

export async function batchGetData(dataManagers: Array<LevelDataManager>, level: number, start: number, end: number, maxLevel: number, width: number, params: { inter:string,  noRet: boolean }) {
    if(level>dataManagers[0].maxLevel){
        return  [];
    }
    if (params.inter !== "zoom_out") {
        let lastFullLevel = 2;
        let nextStart = start;
        let nextEnd = end;
        for (let i = level - 1; i > 2; i--) {
            nextStart = Math.floor(nextStart / 2);
            nextEnd = Math.floor(nextEnd / 2);
            let full = true;
            dataManagers.forEach(manager => {
                if (manager.levelIndexObjs[i]) {
                    full = full && manager.levelIndexObjs[i].isFull;
                }
            });
            if (full) {
                lastFullLevel = i;
                break;
            }
        }
        const allLosedDataInfo: Array<{ tn: string, lr: Array<Array<number>> }> = [];
        for (let i = lastFullLevel + 1; i <= level; i++) {
            nextStart = nextStart * 2;
            nextEnd = nextEnd * 2 + 1;
            dataManagers.forEach(manager => {
                const loseInfo = manager.checkLoadedDataInSingalLevel([[i, nextStart, nextEnd]]);
                if (loseInfo.length > 0) {
                    allLosedDataInfo.push({ tn: manager.dataName, lr: loseInfo });
                }

            });
        }
        const managerMap = new Map();
        for (let i = 0; i < dataManagers.length; i++) {
            managerMap.set(dataManagers[i].dataName, dataManagers[i]);
        }
        if (allLosedDataInfo.length > 0) {
            await batchLoadDataForMultiLine1(allLosedDataInfo, maxLevel, managerMap);
        }
    }
    const renderDatas = [];
    if (!params.noRet) {
        for (let i = 0; i < dataManagers.length; i++) {
            const data = dataManagers[i].levelIndexObjs[level].getDataByIndex(start, end);
            data.start = getIndexTime(level, data.start, maxLevel).startT;
            data.end = getIndexTime(level, data.end, maxLevel).endT;
            const { renderData, minv, maxv } = formatToRenderDataForTrend([maxLevel, level], data, width);

            renderDatas.push({ renderData, minv, maxv });
        }

    }

    return renderDatas;
    
}

async function batchLoadDataForMultiLine1(allLoedData: Array<{ tn: string, lr: Array<Array<number>> }>, maxLevel: number, dataManagers: Map<string, LevelDataManager>) {
    if (allLoedData.length === 0) {
        return [];
    }
    const { data } = await axios.post(`postgres/line_chart/multi_series_batch_load_data1`, {
        multi_series_load_data: allLoedData,
    });

    const result = data.data;
    const loseDataMap = new Map<string, Array<Array<number>>>();
    for (let i = 0; i < allLoedData.length; i++) {
        loseDataMap.set(allLoedData[i].tn, allLoedData[i].lr);
    }
    debugger
    console.log(result)
    if (result && result.length) {
        for (let i = 0; i < result.length; i++) {
            const currentLineRes = result[i]['d'];
            const resultArray = [];
            for (let j = 0; j < currentLineRes[0].length; j++) {
                resultArray.push({ l: maxLevel - currentLineRes[0][j], i: currentLineRes[1][j], dif: [currentLineRes[2][j], currentLineRes[3][j], currentLineRes[4][j], currentLineRes[5][j]] });
            }
            
            dataManagers.get(result[i]['tn'])?.constructTreeForBatchLoad1(loseDataMap.get(result[i]['tn'])!, resultArray)
        }
    }
}

