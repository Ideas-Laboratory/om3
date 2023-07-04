import { ws } from "../store"
import jsonpack from "jsonpack"
export function batchLoadAllDifWidthWs(losedDataInfo: Array<Array<Array<number>>>, dataName: string, url: string, maxLevel: number) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const sendData = {
        url: url,
        tn: dataName.includes(".") ? dataName.split(".")[1] : dataName,//store.state.controlParams.currentTable,
        data: JSON.stringify({ data: losedDataInfo }),
    }
    const packedData = jsonpack.pack(sendData)
    return new Promise((resolve, reject) => {
        ws.send(packedData)
        ws.onmessage = (e) => {
            // const reveiveData = e.data.toString();
            const startT = new Date().getTime()
            //@ts-ignore
            const result = jsonpack.unpack(e.data) as Array<Array<number>>;
            console.log("unpack time:", new Date().getTime() - startT)
            // const url = unpackedData["url"];
            const resultArray = [];
            if (result && result[0] && result[0].length > 0) {
                for (let i = 0; i < result[0].length; i++) {
                    resultArray.push({ l: maxLevel - result[0][i], i: result[1][i], dif: [0, result[2][i], result[3][i], 0] });
                }
            }
            resultArray.sort((a, b) => {
                if (a.l !== b.l) {
                    return a.l - b.l
                } else {
                    return a.i - b.i
                }
            })
            resolve(resultArray)
        }
    })
}

export function loadDifWithWs(losedDataInfo:Array<Array<number>>, dataName: string, url: string, maxLevel: number){
    if (losedDataInfo.length === 0) {
        return [];
    }
    const sendData = {
        url: url,
        tn: dataName.includes(".") ? dataName.split(".")[1] : dataName,//store.state.controlParams.currentTable,
        data: JSON.stringify({ data: losedDataInfo }),
    }
    return new Promise((resolve, reject) => {
        ws.send(JSON.stringify(sendData))
        ws.onmessage = (e) => {
            // const reveiveData = e.data.toString();
            const startT = new Date().getTime()
            //@ts-ignore
            const result = JSON.parse(e.data)
            console.log(e.data)
            console.log("unpack time:", new Date().getTime() - startT)
            // const url = unpackedData["url"];
            const resultArray = [];
            if (result && result[0] && result[0].length > 0) {
                for (let i = 0; i < result[0].length; i++) {
                    resultArray.push({ l: maxLevel - result[0][i], i: result[1][i], dif: [0, result[2][i], result[3][i], 0] });
                }
            }
            resultArray.sort((a, b) => {
                if (a.l !== b.l) {
                    return a.l - b.l
                } else {
                    return a.i - b.i
                }
            })
            resolve(resultArray)
        }
    })
}

export function batchLoadAllDifWidthBinaryWs(losedDataInfo: Array<Array<Array<number>>>, dataName: string, url: string, maxLevel: number) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const sendData = {
        url: url,
        tn: dataName.includes(".") ? dataName.split(".")[1] : dataName,//store.state.controlParams.currentTable,
        data: JSON.stringify({ data: losedDataInfo }),
    }
    const packedData = jsonpack.pack(sendData)
    return new Promise((resolve, reject) => {
        ws.send(packedData)
        ws.binaryType = 'arraybuffer'
        ws.onmessage = (e) => {
            //@ts-ignore
            const result = new Float32Array(e.data)
            const resultArray: Array<{ l: number, i: number, dif: Array<number> }> = [];
            if (result && result.length > 0) {
                for (let i = 0; i < result.length; i += 4) {
                    resultArray.push({ l: maxLevel - result[i], i: result[i + 1], dif: [0, result[i + 2], result[i + 3], 0] })
                }
            }
            resultArray.sort((a, b) => {
                if (a.l !== b.l) {
                    return a.l - b.l
                } else {
                    return a.i - b.i
                }
            })
            resolve(resultArray)
        }
    })
}

export function batchLoadAllDifParallelWidthBinaryWs(losedDataInfo: Array<Array<Array<number>>>, dataName: string, url: string, maxLevel: number) {
    if (losedDataInfo.length === 0) {
        return [];
    }
    const sendData = {
        url: url,
        tn: dataName.includes(".") ? dataName.split(".")[1] : dataName,//store.state.controlParams.currentTable,
        data: JSON.stringify({ data: losedDataInfo }),
    }
    const packedData = jsonpack.pack(sendData)
    const allPromiseResolveFunc: any = [];
    const allPromizes: any = [];
    for (let i = 0; i < losedDataInfo.length; i++) {
        allPromizes.push(new Promise((resolve, reject) => {
            allPromiseResolveFunc.push(resolve);
        }))
    }
    const resultArray: Array<{ l: number, i: number, dif: Array<number> }> = [];
    ws.binaryType = 'arraybuffer';
    let counter = 0
    const startTime = new Date().getTime();
    ws.onmessage = (e) => {
        if (counter >= losedDataInfo.length) {
            console.error("count greater than length")
            return
        }
        //@ts-ignore
        const result = new Float32Array(e.data)
        if(result.length%2!==0){
            throw new Error("data error")
        }
        if (result && result.length > 0) {
            for (let i = 0; i < result.length; i += 4) {
                resultArray.push({ l: maxLevel - result[i], i: result[i + 1], dif: [0, result[i + 2], result[i + 3], 0] })
            }
        }
        console.log(`ssim ${counter}:`, new Date().getTime() - startTime)

        allPromiseResolveFunc[counter](resultArray);
        counter++
        if (counter === 3) {
            console.log("95 ssim:", new Date().getTime() - startTime)
        }
    }
    ws.send(packedData);
    return new Promise((resolve, reject) => {
        Promise.all(allPromizes).then(() => {
            resultArray.sort((a, b) => {
                if (a.l !== b.l) {
                    return a.l - b.l
                } else {
                    return a.i - b.i
                }
            })
            resolve(resultArray)
        })
    })
}

