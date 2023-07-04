import { formatNonPowDataForViewChange } from "@/helper/format-data";
import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import { MultiTimeSeriesObj } from "@/store";
import bres from 'bresenham-line-algorithm';
//@ts-ignore

import { interpolatePiYG, interpolateYlOrBr } from "d3";

import * as d3 from 'd3';
import { batchViewChange, batchGetData } from "../batch/m5batch"
["#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5"];
const lineColor = {
    sp_drinkxcc6740_wavelet_1m: "17becf",
    sp_drinkxjb3156_wavelet_1m: "#dbdb8d",
    sp_drinkxdc6359_wavelet_1m: "#ff7f0e",
    sp_drinkxhv0618_wavelet_1m: "#ffbb78",
    sp_drinkxmc7070_wavelet_1m: "#2ca02c",
    sp_drinkxpc6771_wavelet_1m: "#98df8a",
    sp_drinkxjr8022_wavelet_1m: "#d62728",
    sp_drinkxbu4707_wavelet_1m: "#ff9896",
    sp_drinkxsa0297_wavelet_1m: "#9467bd",
    sp_drinkxsf3079_wavelet_1m: "#c5b0d5",
    sp_drinkxbk7610_wavelet_1m: "#8c564b",
    sp_drinkxmj8002_wavelet_1m: "#c7c7c7",
    sp_drinkxdk3500_wavelet_1m: "#7f7f7f",
}

function computeMinMax(multiTimeSeriesObj: MultiTimeSeriesObj){
    let min=Infinity;
    let max=-Infinity;
    for(let i=0;i<multiTimeSeriesObj.columnInfos.length;i++){
        if(multiTimeSeriesObj.dataManagers[i].isShow){
            const columInfo=multiTimeSeriesObj.columnInfos[i];
            for(let j=0;j<columInfo.length;j++){
                if(columInfo[j].vRange[0]<min){
                    min=columInfo[j].vRange[0];
                }
                
                if(columInfo[j].vRange[1]>max){
                    max=columInfo[j].vRange[1];
                }
                
            }
        }else{
            console.log(multiTimeSeriesObj.dataManagers[i].dataName)
        }
    }
    return {
        min,
        max
    }
}

export function drawMultiTimeSeries(multiTimeSeriesObj: MultiTimeSeriesObj) {
    const initTimeRange = [0, 2 ** multiTimeSeriesObj.maxLevel - 1];
    let isInit = false;
    let interactiveInfo = {
        startX: 0,
        startY: 0,
        offsetX: 0,
        offsetY: 0,
        isMouseDown: false,
        isMove: false,
    };
    let currentLevel = multiTimeSeriesObj.currentLevel;
    const pading = { top: 20, bottom: 20, left: 40, right: 20 };
    const svg = d3.select("#content-container").append("svg");
    svg
        .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
        .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${multiTimeSeriesObj.x},${multiTimeSeriesObj.y})`)
        .style("background-color", "#fff");

    const foreignId = `foreign${multiTimeSeriesObj.width}`;
    const foreigG = svg.append("g").attr("transfrom", `translate(${pading.left},${pading.top})`)
    let foreignObj: any = foreigG.append("foreignObject").attr("id", foreignId).attr("x", pading.left).attr("y", pading.top).attr('width', multiTimeSeriesObj.width).attr('height', multiTimeSeriesObj.height);
    const canvas = document.createElement("canvas");
    (canvas as any).__data__ = {}
    document.getElementById(foreignId)?.appendChild(canvas);
    canvas.width = multiTimeSeriesObj.width;
    canvas.height = multiTimeSeriesObj.height;

    let ctx = canvas.getContext("2d");
    const timeRangeScale = d3.scaleLinear().domain([1493733884409, 1493829248294]).range([0, 2 ** 20 - 1]);
    const xScale: any = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.width]);
    //let showTimeXScale: any = d3.scaleTime().domain([new Date(1493733884409), new Date(1493829248294)]).range([0, multiTimeSeriesObj.width]);
    let showTimeXScale: any = d3.scaleTime().domain([new Date(1422852480000), new Date(1645178340000)]).range([0, multiTimeSeriesObj.width]);
    // console.log(showTimeXScale.invert(200))
    let yScale: any = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height, 0]);
    console.log([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv])
    // const showTimeXScale: any = d3.scaleLinear().domain([multiTimeSeriesObj.timeRange[0], multiTimeSeriesObj.timeRange[1]]).range([0, multiTimeSeriesObj.width]);
    let xReScale = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.dataManagers[0].realDataRowNum-1]);
    let xAxis = d3.axisBottom(showTimeXScale);
    let yAxis = d3.axisLeft(yScale);
    const yReScale = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height, 0]);
    const timeBrushObj = d3.brushX().extent([[0, 10], [multiTimeSeriesObj.width, 40]]);
    const timeBoxBrushObj = d3.brush().extent([[pading.left, pading.top], [multiTimeSeriesObj.width + pading.left, multiTimeSeriesObj.height + pading.top]]);

    timeBoxBrushObj.on("end", timeBoxBrushed);

    timeBrushObj.on("end", brushed);
    timeBrushObj.on("start", () => {
        console.log("start")
    })
    const timeBoxG = svg.append("g").attr("transform", `translate(${pading.left},${pading.top + multiTimeSeriesObj.height - 20})`).call(timeBrushObj).call(timeBrushObj.move, [0, multiTimeSeriesObj.width]);
    foreigG.call(timeBoxBrushObj)
    let xAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top})`).attr("class", 'x axis').call(xAxis)
    let yAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);


    //@ts-ignore




    function updateCanvasWidth() {

        //@ts-ignore
        canvas.style.width = multiTimeSeriesObj.width;
        svg
            .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
            .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        foreignObj.attr("width", multiTimeSeriesObj.width);
        xScale.domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.width]);
        showTimeXScale.domain([multiTimeSeriesObj.timeRange[0], multiTimeSeriesObj.timeRange[1]]).range([0, multiTimeSeriesObj.width]);
        showTimeXScale.range([0, multiTimeSeriesObj.width]);
        if (xAxisG != null) {
            xAxisG.remove();
            xAxisG = svg.append("g").attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top})`).attr("class", 'x axis').call(xAxis)
        }
        timeBrushObj.extent([[0, 10], [multiTimeSeriesObj.width, 40]]);
        timeBoxG.call(timeBrushObj)
        xReScale.domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.dataManagers[i].realDataRowNum-1]);
        ctx = canvas.getContext("2d");
    }
    function draw() {
        // renderDensity(ctx, multiTimeSeriesObj, yScale, canvas)
        // return
       // debugger
        canvas.width = multiTimeSeriesObj.width;
        const curMinMax=computeMinMax(multiTimeSeriesObj);
        console.log(curMinMax)
        let yScale: any = d3.scaleLinear().domain([0, curMinMax.max]).range([multiTimeSeriesObj.height, 0]);
        if (multiTimeSeriesObj.pow) {
            const allRenderData = multiTimeSeriesObj.powRenderData;
            ctx?.clearRect(0, 0, multiTimeSeriesObj.width, multiTimeSeriesObj.height);
            //@ts-ignore
            //ctx?.strokeStyle="steelblue";
            for (let i = 0; i < allRenderData.length; i++) {
                ctx?.beginPath();
                const renderData = allRenderData[i].renderData
                for (let j = 0; j < renderData.length; j++) {
                    if (j === 0) {
                        ctx?.moveTo(renderData[j].x, yScale(renderData[j].y));
                    } else {
                        ctx?.lineTo(renderData[j].x, yScale(renderData[j].y));
                    }
                }
                ctx?.stroke();
            }

        } else {
            // renderDensity(ctx,multiTimeSeriesObj.columnInfos,yScale)
            // const dataManagers=multiTimeSeriesObj.dataManagers;
            // for(let i=0;i<dataManagers.length;i++){
            //     if(!dataManagers[i].isShow){
            //         continue;
            //     }
            //     const ctx.strokeStyle
            // }
            const columnInfos = multiTimeSeriesObj.columnInfos;
            ctx?.clearRect(0, 0, multiTimeSeriesObj.width, multiTimeSeriesObj.height);
            const colorArray1 = ["#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5"];
            for (let i = 0; i < columnInfos.length; i++) {
                if (multiTimeSeriesObj.dataManagers[i].isShow) {
                    formatNonPowDataForViewChange(columnInfos[i], multiTimeSeriesObj.width, 2 ** multiTimeSeriesObj.maxLevel, null);
                    if (multiTimeSeriesObj.dataManagers[i]) {
                        ctx?.beginPath();
                        //@ts-ignore
                        if (lineColor[columnInfos[i][0].dataName?.split(".")[1]]) {
                            //@ts-ignore
                            ctx.strokeStyle = lineColor[columnInfos[i][0].dataName?.split(".")[1]]
                        } else {
                            //@ts-ignore
                            ctx.strokeStyle = colorArray1[i%19];
                        }
                        //colorArray1[19 - i];
                        //@ts-ignore
                        ctx.strokeWidth = 2;
                        const columnInfo = columnInfos[i];
                        for (let j = 0; j < columnInfo.length; j++) {
                            if (columnInfo[j].startV !== undefined && columnInfo[j].endV !== undefined) {
                                if (columnInfo[i].startV! < columnInfo[i].endV!) {
                                    ctx?.moveTo(columnInfo[j].positionInfo.minX, yScale(columnInfo[j].vRange[0]));
                                    ctx?.lineTo(columnInfo[j].positionInfo.maxX, yScale(columnInfo[j].vRange[1]));
                                } else {
                                    ctx?.moveTo(columnInfo[j].positionInfo.minX, yScale(columnInfo[j].vRange[1]));
                                    ctx?.lineTo(columnInfo[j].positionInfo.maxX, yScale(columnInfo[j].vRange[0]));
                                }
                            } else {
                                if (columnInfo[j].minVTimeRange[0] < columnInfo[j].maxVTimeRange[0]) {
                                    ctx?.moveTo(columnInfo[j].positionInfo.minX, yScale(columnInfo[j].vRange[0]));
                                    ctx?.lineTo(columnInfo[j].positionInfo.maxX, yScale(columnInfo[j].vRange[1]));
                                } else {
                                    ctx?.moveTo(columnInfo[j].positionInfo.minX, yScale(columnInfo[j].vRange[1]));
                                    ctx?.lineTo(columnInfo[j].positionInfo.maxX, yScale(columnInfo[j].vRange[0]));
                                }
                            }
                            if (j <= columnInfo.length - 2 && columnInfo[j].endV !== undefined && columnInfo[j + 1] !== undefined) {
                                ctx?.moveTo(columnInfo[j].positionInfo.endX, yScale(columnInfo[j].endV!));
                                ctx?.lineTo(columnInfo[j + 1].positionInfo.startX, yScale(columnInfo[j + 1].startV!));
                            }
                        }
                        ctx?.stroke();
                    }

                }
            }
        }
        // console.log(multiTimeSeriesObj.dataManagers[0])
        // const hisWidth=multiTimeSeriesObj.width;
        // multiTimeSeriesObj.width=2**(multiTimeSeriesObj.currentLevel+1)
        // batchViewChange(multiTimeSeriesObj,{inter:"resize"}).then(()=>{
        //     multiTimeSeriesObj.width=hisWidth;
        //     console.log(multiTimeSeriesObj.dataManagers[0])
        // })
        // batchGetData(multiTimeSeriesObj.dataManagers,multiTimeSeriesObj.currentLevel+1,0,2**(multiTimeSeriesObj.currentLevel+1)-1,multiTimeSeriesObj.maxLevel,multiTimeSeriesObj.width,{ inter: "zoom_in", noRet: true }).then(res=>{
        //     multiTimeSeriesObj.currentLevel=multiTimeSeriesObj.currentLevel+1;
        // })
    }

    function resizeW(width: number) {
        const currentLevel = multiTimeSeriesObj.currentLevel;
        // multiTimeSeriesObj.currentLevel = 10;//currentLevel + 1
        multiTimeSeriesObj.width = width;
        updateCanvasWidth();

        if (currentLevel + 1 >= multiTimeSeriesObj.maxLevel - 1) {
            return
        }
        if (multiTimeSeriesObj.pow) {
            batchGetData(multiTimeSeriesObj.dataManagers, currentLevel + 1, 0, width - 1, multiTimeSeriesObj.maxLevel, width, { inter: "resize", noRet: false }).then(res => {
                multiTimeSeriesObj.pow = true;
                //@ts-ignore
                multiTimeSeriesObj.powRenderData = res;
                draw();
            })
        } else {
            //@ts-ignore
            canvas.style.width = multiTimeSeriesObj.width
            batchGetData(multiTimeSeriesObj.dataManagers, multiTimeSeriesObj.currentLevel, 0, 2 ** multiTimeSeriesObj.currentLevel - 1, multiTimeSeriesObj.maxLevel, width, { inter: "resize", noRet: true }).then(res => {
                console.log("resise fininsih")
                batchViewChange(multiTimeSeriesObj, { inter: "resize" }).then((allColumnInfos) => {
                    multiTimeSeriesObj.pow = false;
                    multiTimeSeriesObj.columnInfos = allColumnInfos;

                    console.timeEnd("view_change")

                    draw();
                })
            })

        }

    }

    function zoomIn(timeRange: Array<number>) {
        const currentLevel = multiTimeSeriesObj.currentLevel;
        multiTimeSeriesObj.currentLevel = 10;//currentLevel + 1
        const width = multiTimeSeriesObj.width;
        multiTimeSeriesObj.timeRange[1] = timeRange[1]//Math.floor(multiTimeSeriesObj.timeRange[1] / 2);
        multiTimeSeriesObj.timeRange[0] = timeRange[0]
        if (currentLevel + 1 >= multiTimeSeriesObj.maxLevel - 1) {
            return
        }

        

        if (multiTimeSeriesObj.pow) {
            batchGetData(multiTimeSeriesObj.dataManagers, currentLevel + 1, 0, width - 1, multiTimeSeriesObj.maxLevel, width, { inter: "zoom_in", noRet: false }).then(res => {
                multiTimeSeriesObj.pow = true;
                //@ts-ignore
                multiTimeSeriesObj.powRenderData = res;
                draw();
            })
        } else {
            const needLoadLevel = 2 ** Math.ceil(Math.log2(width))
            batchGetData(multiTimeSeriesObj.dataManagers, 10, 0, 2 ** 10 - 1, multiTimeSeriesObj.maxLevel, width, { inter: "zoom_in", noRet: true }).then(res => {
                console.log("zoom in get data finish")
               
                batchViewChange(multiTimeSeriesObj, { inter: "zoom_in" }).then((allColumnInfos) => {
                    multiTimeSeriesObj.pow = false;
                    multiTimeSeriesObj.columnInfos = allColumnInfos;
                    draw();
                })
            });

        }

    }

    function zoomOut(timeRange: Array<number>) {
        const currentLevel = multiTimeSeriesObj.currentLevel;
        multiTimeSeriesObj.currentLevel = 10;//currentLevel + 1
        const width = multiTimeSeriesObj.width;
        multiTimeSeriesObj.timeRange[1] = timeRange[1]//Math.floor(multiTimeSeriesObj.timeRange[1] / 2);
        multiTimeSeriesObj.timeRange[0] = timeRange[0]
        if (currentLevel + 1 >= multiTimeSeriesObj.maxLevel - 1) {
            return
        }
        if (multiTimeSeriesObj.pow) {
            batchGetData(multiTimeSeriesObj.dataManagers, currentLevel + 1, 0, width - 1, multiTimeSeriesObj.maxLevel, width, { inter: "zoom_in", noRet: false }).then(res => {
                multiTimeSeriesObj.pow = true;
                //@ts-ignore
                multiTimeSeriesObj.powRenderData = res;
                draw();

            })
        } else {

            batchGetData(multiTimeSeriesObj.dataManagers, 10, 0, 2 ** 10 - 1, multiTimeSeriesObj.maxLevel, width, { inter: "zoom_in", noRet: true }).then(res => {

                batchViewChange(multiTimeSeriesObj, { inter: "zoom_in" }).then((allColumnInfos) => {
                    multiTimeSeriesObj.pow = false;
                    multiTimeSeriesObj.columnInfos = allColumnInfos;
                    draw();
                })
            })

        }

    }

    //@ts-ignore
    function brushed({ selection }) {
        if (!isInit) {
            isInit = true
            return;
        }
        // console.log(showTimeXScale.invert(Math.floor(xReScale(selection[0] - pading.left))));
        // console.log(showTimeXScale.invert(Math.floor(xReScale(selection[1] - pading.left))));
        const timeRange = [Math.floor(xReScale(selection[0] - pading.left)), Math.floor(xReScale(selection[1] - pading.left))];
        if (timeRange[0] < 0) {
            timeRange[0] = 0;
        }
        if (timeRange[1] > 2 ** multiTimeSeriesObj.maxLevel - 1) {
            timeRange[1] = 2 ** multiTimeSeriesObj.maxLevel - 1;
        }
        // console.log(showTimeXScale.invert(xReScale.invert(timeRange[0])));
        // console.log(showTimeXScale.invert(xReScale.invert(timeRange[1])));
        zoomIn([timeRange[0], timeRange[1]])
    }

    //@ts-ignore
    function timeBoxBrushed({ selection }) {
        if (selection === null) {
            return;
        }
        // console.log(selection);
        const startX = selection[0][0] - 40;
        const endX = selection[1][0] - 40;
        // console.log(showTimeXScale.invert(startX));
        // console.log(showTimeXScale.invert(endX));
        const vMinY = selection[1][1] - 20;
        const vMaxY = selection[0][1] - 20;
        const vMin = yScale.invert(vMinY);
        const vMax = yScale.invert(vMaxY);
        const allColumnInfos = multiTimeSeriesObj.columnInfos;
        for (let i = 0; i < allColumnInfos.length; i++) {
            let isChoose = true;
            for (let j = Math.floor(startX); j <= Math.floor(endX); j++) {
                isChoose = isChoose && (allColumnInfos[i][j].vRange[0] >= vMin && allColumnInfos[i][j].vRange[1] <= vMax)
            }
            multiTimeSeriesObj.dataManagers[i].isShow = false;
            if (isChoose) {
                multiTimeSeriesObj.dataManagers[i].isShow = true
                //console.log(multiTimeSeriesObj.dataManagers[i].dataName);
            }

        }
        let minV = Infinity;
        let maxV = -Infinity;
        for (let i = 0; i < allColumnInfos.length; i++) {
            if (multiTimeSeriesObj.dataManagers[i].isShow) {
                for (let j = 0; j <= allColumnInfos[i].length; j++) {
                    minV = Math.min(minV, allColumnInfos[i][j] ? allColumnInfos[i][j].vRange[0] : minV);

                    maxV = Math.max(maxV, allColumnInfos[i][j] ? allColumnInfos[i][j].vRange[1] : maxV);
                }
            }

        }

        // multiTimeSeriesObj.maxv = -10;
        // multiTimeSeriesObj.minv = 10
        draw();
        //zoomIn([timeRangeScale(1493740845000),timeRangeScale(1493805314000)])
    }
    let isMouseover = false;
    let startOffsetX = 0;
    const dragRect = svg
        .append("rect")
        .attr("x", 0)
        .attr("y", 0)
        .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
        .attr("stroke", "black")
        .attr("stroke-width", 4)
        .attr("fill", "none")
        .on("mouseover", () => {
            document.body.style.cursor = 'ew-resize';
            isMouseover = true;
        })
        .on("mousedown", (e) => {
            if (isMouseover) {
                startOffsetX = e.offsetX;
                interactiveInfo.isMouseDown = true;
            }
        })
        .on("mouseup", () => {
            console.log()
        })
        .on("mousemove", (e) => {
            if (interactiveInfo.isMouseDown) {
                svg.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
                dragRect.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
            }
        })
        .on("mouseleave", () => {
            if (!interactiveInfo.isMouseDown) {
                document.body.style.cursor = 'default';
            }
        })

    document
        .getElementById("content-container")!
        .addEventListener("mousemove", (e) => {
            if (interactiveInfo.isMouseDown) {
                svg.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
                dragRect.attr("width", multiTimeSeriesObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
            }
        });

    document
        .getElementById("content-container")!
        .addEventListener("mouseup", (e) => {
            if (interactiveInfo.isMouseDown) {
                interactiveInfo.isMouseDown = false;
                isMouseover = false;
                document.body.style.cursor = 'default';
                let preWidth = multiTimeSeriesObj.width;
                multiTimeSeriesObj.width = multiTimeSeriesObj.width + (e.offsetX - startOffsetX);
                if (multiTimeSeriesObj.width === preWidth) {
                    return
                }
                resizeW(multiTimeSeriesObj.width)
            }
        });
    svg.on("contextmenu", (e) => {
        e.preventDefault();
        e.stopPropagation();
    });


    let i = 0;
    canvas.addEventListener("click", (e) => {
        // if (i < 50) {
        //     zoomIn([timeRangeScale(1493740845000), timeRangeScale(1493807899000)])
        //     i++;
        // }
    });
    draw();

}


class ObjectPool {
    buffer: Array<{ x: number, y: number }>
    objNum: number
    inusedNum: number
    constructor(initNum: number) {
        this.buffer = [];
        for (let i = 0; i < initNum; i++) {
            this.buffer.push({ x: 0, y: 0 });
        }
        this.objNum = initNum;
        this.inusedNum = 0;
    }
    getObj(x: number, y: number) {
        if (this.inusedNum >= this.objNum) {
            for (let i = 0; i < 100; i++) {
                this.buffer.push({ x: 0, y: 0 });
            }
            this.objNum = this.buffer.length;
        }
        this.buffer[this.inusedNum].x = x;
        this.buffer[this.inusedNum].y = y;
        this.inusedNum++;
        return this.buffer[this.inusedNum - 1];
    }
    clearInuse() {
        this.inusedNum = 0;
    }
}

const pool = new ObjectPool(10000);

function renderDensity(ctx: any, multiTimeSeriesObj: MultiTimeSeriesObj, yScale: any, canvas: any) {
    const height = canvas.height
    const resultArray: Array<any> = [];
    let colObjs = multiTimeSeriesObj.columnInfos;
    for (let j = 0; j < colObjs.length; j++) {
        if (!multiTimeSeriesObj.dataManagers[j].isShow) {
            continue;
        }
        const curColArray = colObjs[j];
        formatNonPowDataForViewChange(curColArray, multiTimeSeriesObj.width, 2 ** multiTimeSeriesObj.maxLevel, null);
        for (let i = 0; i < curColArray.length; i++) {
            if (curColArray[i].startV !== undefined && curColArray[i].endV !== undefined) {
                if (curColArray[i].startV! < curColArray[i].endV!) {
                    const x1 = Math.floor(curColArray[i].positionInfo.minX);
                    const y1 = Math.ceil(height - yScale(curColArray[i].vRange[0]));
                    const x2 = Math.floor(curColArray[i].positionInfo.maxX);
                    const y2 = Math.ceil(height - yScale(curColArray[i].vRange[1]));
                    let pointsList = bres.bresenhamLinePoints(x1, y1, x2, y2) as Array<{ x: number, y: number }>;
                    pointsList.forEach(v => {
                        resultArray.push(pool.getObj(v.x, v.y))
                    })
                    //resultArray.push(...pointsList);
                } else {
                    const x1 = Math.floor(curColArray[i].positionInfo.minX)
                    const y1 = Math.ceil(height - yScale(curColArray[i].vRange[1]));
                    const x2 = Math.floor(curColArray[i].positionInfo.maxX)
                    const y2 = Math.ceil(height - yScale(curColArray[i].vRange[0]));
                    let pointsList = bres.bresenhamLinePoints(x1, y1, x2, y2) as Array<{ x: number, y: number }>;
                    pointsList.forEach(v => {
                        resultArray.push(pool.getObj(v.x, v.y))
                    })
                    //resultArray.push(...pointsList);
                }
            } else {
                if (curColArray[i].minVTimeRange[0] < curColArray[i].maxVTimeRange[0]) {
                    const x1 = Math.floor(curColArray[i].positionInfo.minX)
                    const y1 = Math.ceil(height - yScale(curColArray[i].vRange[0]));
                    const x2 = Math.floor(curColArray[i].positionInfo.maxX)
                    const y2 = Math.ceil(height - yScale(curColArray[i].vRange[1]));
                    let pointsList = bres.bresenhamLinePoints(x1, y1, x2, y2) as Array<{ x: number, y: number }>;
                    pointsList.forEach(v => {
                        resultArray.push(pool.getObj(v.x, v.y))
                    })
                    //resultArray.push(...pointsList);
                } else {
                    const x1 = Math.floor(curColArray[i].positionInfo.minX)
                    const y1 = Math.ceil(height - yScale(curColArray[i].vRange[1]));
                    const x2 = Math.floor(curColArray[i].positionInfo.maxX)
                    const y2 = Math.ceil(height - yScale(curColArray[i].vRange[0]));
                    let pointsList = bres.bresenhamLinePoints(x1, y1, x2, y2) as Array<{ x: number, y: number }>;
                    pointsList.forEach(v => {
                        resultArray.push(pool.getObj(v.x, v.y))
                    })
                    //resultArray.push(...pointsList);
                }
            }
            if (i <= curColArray.length - 2 && curColArray[i].endV !== undefined && curColArray[i + 1] !== undefined) {

                const x1 = Math.floor(curColArray[i].positionInfo.endX)
                const y1 = Math.ceil(height - yScale(curColArray[i].endV));
                const x2 = Math.floor(curColArray[i + 1].positionInfo.startX)
                const y2 = Math.ceil(height - yScale(curColArray[i + 1].startV));
                let pointsList = bres.bresenhamLinePoints(x1, y1, x2, y2) as Array<{ x: number, y: number }>;
                pointsList.forEach(v => {
                    resultArray.push(pool.getObj(v.x, v.y))
                })
                //resultArray.push(...pointsList);
            }
        }
    }
    console.log("pool inuse,pool len:", pool.inusedNum, pool.buffer.length)
    //console.log(resultArray)
    //let d2 = density2d(resultArray, { x: 'x', y: 'y', bins: [canvas.width, canvas.height], extent: [[0, canvas.width], [0, canvas.height]], adjust: 0.2 });
    //const gr = d2.grid();
    // let max=0;
    // let min=10;
    // for(let i=0;i<600000;i++){
    //     if(gr[i]>max){
    //         max=gr[i]
    //     }
    //     if(gr[i]<min&&gr[i]>0){
    //         min=gr[i]
    //     }
    // }
    //console.log(max,min);

    // let p2=[...d2];
    //let p2=[...d2.points('x','y','z')];

    //let h2 = d2.heatmap({ color: interpolateYlOrBr, canvas: canvas });
    pool.clearInuse()
    //console.log(p2)

}