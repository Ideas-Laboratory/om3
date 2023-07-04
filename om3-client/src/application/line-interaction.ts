

import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import store, { ViewChangeLineChartObj } from "@/store";
import * as d3 from 'd3';
import { formatRenderDataForViewChange, formatNonPowDataForViewChange } from '../helper/format-data';
class InteractionInfo {
    type: string
    showInfo: Array<boolean>
    timeRange: Array<number>
    width: number
    level: number
    constructor(type: string) {
        this.type = type;
        this.showInfo = [];
        this.timeRange = [];
        this.width = 0;
        this.level = 0;
    }
    setShowInfo(showInfo: Array<boolean>) {
        this.showInfo = showInfo;
    }
    setRangeW(timeRange: Array<number>, width: number, level: number) {
        this.timeRange = [timeRange[0], timeRange[1]];
        this.width = width;
        this.level = level;
    }

}
let interactionStack: Array<InteractionInfo> = [];

export function drawViewChangeLineChart(lineChartObj: ViewChangeLineChartObj) {
    let realTimeStampRange: Array<number> = [];
    let nodeIndexRange: Array<number> = []
    realTimeStampRange = [lineChartObj.startTime,lineChartObj.endTime];

    nodeIndexRange = [lineChartObj.timeRange[0],lineChartObj.timeRange[1]];
   
    let isInit = false;
    let isResizing = false;
    let isRebacking = false;
    let interactiveInfo = {
        startX: 0,
        startY: 0,
        offsetX: 0,
        offsetY: 0,
        isMouseDown: false,
        isMove: false,
    };
    const pading = { top: 20, bottom: 80, left: 45, right: 45 };
    const svg = d3.select("#content-container").append("svg");
    svg
        .attr("width", lineChartObj.width + pading.left + pading.right)
        .attr("height", lineChartObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${lineChartObj.x},${lineChartObj.y})`)
        .style("background-color", "#fff");
    const foreignId = `foreign${lineChartObj.width + Math.random()}`;
    const foreigG = svg.append("g").attr("transfrom", `translate(${pading.left},${pading.top})`)
    let foreignObj: any = foreigG.append("foreignObject").attr("id", foreignId).attr("x", pading.left).attr("y", pading.top).attr('width', lineChartObj.width).attr('height', lineChartObj.height);
    const canvas = document.createElement("canvas");
    (canvas as any).__data__ = {}
    document.getElementById(foreignId)?.appendChild(canvas);
    canvas.width = lineChartObj.width;
    canvas.height = lineChartObj.height;
    let ctx = canvas.getContext("2d");

    const indexToTimeStampScale = d3.scaleLinear().domain([nodeIndexRange[0], nodeIndexRange[1]]).range([realTimeStampRange[0], realTimeStampRange[1]]);
    const xScale: any = d3.scaleLinear().domain([0, lineChartObj.width]).range([0, lineChartObj.width]);
    let showTimeXScale: any = d3.scaleTime().domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, lineChartObj.width]);
    let yScale: any = d3.scaleLinear().domain([lineChartObj.data.minv, lineChartObj.data.maxv]).range([lineChartObj.height, 0]);
    let xReScale = d3.scaleLinear().domain([0, lineChartObj.width]).range([0, lineChartObj.dataManager.realDataRowNum - 1]);
    let showXTimeScale: any = d3.scaleTime().domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, lineChartObj.width]);

    let zoomAxis = d3.axisBottom(showTimeXScale);
    let yAxis = d3.axisLeft(yScale)
    let xAxis = d3.axisBottom(showXTimeScale)
    const timeBrushObj = d3.brushX().extent([[0, 10], [lineChartObj.width, 40]]);

    timeBrushObj.on("end", brushed);
    timeBrushObj.on("start", () => {
        console.log("start")
    })
    const timeBoxG = svg.append("g").attr("transform", `translate(${pading.left},${pading.top + 50 + lineChartObj.height - 20})`).call(timeBrushObj).call(timeBrushObj.move, [0, lineChartObj.width]);
    let zoomAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${lineChartObj.height + pading.top + 50})`).attr("class", 'x axis').call(zoomAxis)
    let xAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${lineChartObj.height + pading.top})`).attr("class", 'x axis').call(xAxis)
    let yAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);


    function updateCanvasWidth() {

        //@ts-ignore
        canvas.style.width = lineChartObj.width;
        svg
            .attr("width", lineChartObj.width + pading.left + pading.right)
            .attr("height", lineChartObj.height + pading.top + pading.bottom)
        foreignObj.attr("width", lineChartObj.width);
        xScale.domain([0, lineChartObj.width]).range([0, lineChartObj.width]);
        showTimeXScale.domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, lineChartObj.width]);
        //showTimeXScale.range([0, lineChartObj.width]);
        if (zoomAxisG != null) {
            zoomAxisG.remove();
            zoomAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${lineChartObj.height + pading.top + 50})`).attr("class", 'x axis').call(zoomAxis)
        }
        timeBrushObj.extent([[0, 10], [lineChartObj.width, 40]]);
        timeBrushObj.on("end", brushed);
        timeBrushObj.on("start", () => {
            console.log("start")
        })
        const tempReScale=d3.scaleLinear().domain([0, lineChartObj.dataManager.realDataRowNum - 1]).range([0,lineChartObj.width]);

        timeBoxG.call(timeBrushObj).call(timeBrushObj.move, [tempReScale(lineChartObj.timeRange[0]), tempReScale(lineChartObj.timeRange[1])]);
        ctx = canvas.getContext("2d");
    }




    function draw(nonUniformColObjs?: Array<NoUniformColObj>,type?:string) {
        canvas.width = lineChartObj.width;
        yScale = d3.scaleLinear().domain([lineChartObj.data.minv, lineChartObj.data.maxv]).range([lineChartObj.height, 0]);
        yAxis = d3.axisLeft(yScale)
        if (yAxisG !== null && yAxisG !== undefined) {
            yAxisG.remove();
        }
        yAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);



        showXTimeScale = d3.scaleTime().domain([new Date(Math.floor(indexToTimeStampScale(lineChartObj.timeRange[0]))), new Date(Math.floor(indexToTimeStampScale(lineChartObj.timeRange[1])))]).range([0, lineChartObj.width]);
        xAxis = d3.axisBottom(showXTimeScale);
        if (xAxisG !== null && xAxisG !== undefined) {
            xAxisG.remove();
        }
        xAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${lineChartObj.height + pading.top})`).attr("class", 'x axis').call(xAxis)


        if (foreignObj == null && nonUniformColObjs) {
            foreignObj = svg.append("foreignObject")
                .attr("id", "foreign")
                .attr('x', pading.left)
                .attr('y', pading.top)
                .attr('width', lineChartObj.width)
                .attr('height', lineChartObj.height);
            const canvas = document.createElement("canvas");
            document.getElementById("foreign")?.appendChild(canvas);
            canvas.width = lineChartObj.width;
            canvas.height = lineChartObj.height;
            ctx = canvas.getContext("2d");
        }


        if (nonUniformColObjs && ctx) {
            formatNonPowDataForViewChange(nonUniformColObjs,lineChartObj.width,lineChartObj.maxLen,null)
            // console.log(nonUniformColObjs);
            ctx.clearRect(0, 0, lineChartObj.width, lineChartObj.height);
            ctx.beginPath();
            ctx.strokeStyle = "steelblue"
            
            
            for (let i = 0; i < nonUniformColObjs.length; i++) {
                if (nonUniformColObjs[i].isMis) {
                    continue
                }
                if (nonUniformColObjs[i].minVTimeRange[0] < nonUniformColObjs[i].maxVTimeRange[0]) {
                    ctx.moveTo(nonUniformColObjs[i].positionInfo.minX, yScale(nonUniformColObjs[i].vRange[0]));
                    ctx.lineTo(nonUniformColObjs[i].positionInfo.maxX, yScale(nonUniformColObjs[i].vRange[1]));
                } else {
                    ctx.moveTo(nonUniformColObjs[i].positionInfo.minX, yScale(nonUniformColObjs[i].vRange[1]));
                    ctx.lineTo(nonUniformColObjs[i].positionInfo.maxX, yScale(nonUniformColObjs[i].vRange[0]));
                }
                if (i <= nonUniformColObjs.length - 2 && nonUniformColObjs[i].endV !== undefined && nonUniformColObjs[i + 1] !== undefined) {
                    ctx.moveTo(nonUniformColObjs[i].positionInfo.endX, yScale(nonUniformColObjs[i].endV!));
                    ctx.lineTo(nonUniformColObjs[i + 1].positionInfo.startX, yScale(nonUniformColObjs[i + 1].startV!));
                }

            }
            const stack = [];
            for (let i = 0; i < nonUniformColObjs.length - 1; i++) {
                if (!nonUniformColObjs[i].isMis && nonUniformColObjs[i + 1].isMis) {
                    stack.push(nonUniformColObjs[i]);
                    for (let j = i + 1; j < nonUniformColObjs.length; j++) {
                        if (nonUniformColObjs[j - 1].isMis && !nonUniformColObjs[j].isMis) {
                            const co = stack.pop()
                            if (nonUniformColObjs[j].startV === undefined || co?.endV === undefined) {
                                console.error("error nonUniform");
                            }
                            ctx.moveTo(co!.positionInfo.endX, yScale(co!.endV));
                            if (nonUniformColObjs[j].startV !== undefined) {
                                ctx.lineTo(nonUniformColObjs[j].positionInfo.startX, yScale(nonUniformColObjs[j].startV!))
                            } else {
                                ctx.lineTo(nonUniformColObjs[j].positionInfo.minX, yScale((nonUniformColObjs[j].vRange[0] + nonUniformColObjs[j].vRange[1]) / 2))
                            }

                        }
                    }
                }
            }
            ctx.stroke();
        } else {
            console.log("error")
        }

    }

    // draw(!lineChartObj.isPow ? lineChartObj.nonUniformColObjs : undefined);


    //@ts-ignore
    function brushed({ selection }) {
        if (!isInit) {
            isInit = true
            return;
        }
        if (isResizing) {
            isRebacking = false;
            return;
        }
        if (isRebacking) {
            isRebacking = false;
            return
        }
        const timeRange = [Math.floor(xReScale(selection[0])), Math.floor(xReScale(selection[1]))];
        if (timeRange[0] < 0) {
            timeRange[0] = 0;
        }
        if (timeRange[1] > nodeIndexRange[1]) {
            timeRange[1] = nodeIndexRange[1]
        }
        const interInfo = new InteractionInfo("zoom")
        interInfo.setRangeW(lineChartObj.timeRange, lineChartObj.width, lineChartObj.currentLevel);
        interactionStack.push(interInfo);
        //debugger
        lineChartObj.dataManager.viewChangeInteractionFinal1(lineChartObj.currentLevel, lineChartObj.width, [timeRange[0], timeRange[1]], null,draw).then((columnsInfos) => {
            //lineChartObj.nonUniformColObjs = columnsInfos;
            //const nonUniformRenderData = formatNonPowDataForViewChange(columnsInfos, lineChartObj.width, lineChartObj.maxLen, yScale);
            lineChartObj.timeRange[0] = timeRange[0];
            lineChartObj.timeRange[1] = timeRange[1];
            //@ts-ignore
            //lineChartObj.nonUniformColObjs = nonUniformRenderData;

            draw(columnsInfos);
        })
    }

    function resizeW(width: number) {
        isResizing = true;

        const currentLevel = lineChartObj.currentLevel;

        lineChartObj.width = width;
        updateCanvasWidth();

        if (currentLevel + 1 >= lineChartObj.dataManager.maxLevel - 1) {
            return
        }
        //@ts-ignore
        canvas.style.width = lineChartObj.width;
        lineChartObj.dataManager.viewChangeInteractionFinal1(lineChartObj.currentLevel, lineChartObj.width, [lineChartObj.timeRange[0], lineChartObj.timeRange[1]], null,draw).then((columnsInfos) => {
            //const nonUniformRenderData = formatNonPowDataForViewChange(columnsInfos, lineChartObj.width, 2 ** lineChartObj.dataManager.maxLevel, yScale);
            //@ts-ignore
            //lineChartObj.nonUniformColObjs = nonUniformRenderData;
            draw(columnsInfos);
        })
    }
    let isMouseover = false;
    let startOffsetX = 0;
    const dragRect = svg
        .append("rect")
        .attr("x", 0)
        .attr("y", 0)
        .attr("height", lineChartObj.height + pading.top + pading.bottom)
        .attr("width", lineChartObj.width + pading.left + pading.right)
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
                svg.attr("width", lineChartObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
                dragRect.attr("width", lineChartObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
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
                svg.attr("width", lineChartObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
                dragRect.attr("width", lineChartObj.width + pading.right + pading.left + (e.offsetX - startOffsetX));
            }
        });

    document
        .getElementById("content-container")!
        .addEventListener("mouseup", (e) => {
            if (interactiveInfo.isMouseDown) {
                interactiveInfo.isMouseDown = false;
                isMouseover = false;
                document.body.style.cursor = 'default';
                let preWidth = lineChartObj.width;
                lineChartObj.width = lineChartObj.width + (e.offsetX - startOffsetX);
                if (lineChartObj.width === preWidth) {
                    return
                }
                const interInfo = new InteractionInfo("resize")
                interInfo.setRangeW(lineChartObj.timeRange, lineChartObj.width, lineChartObj.currentLevel);
                interactionStack.push(interInfo);
                resizeW(lineChartObj.width)
            }
        });

    return draw
}

function computeMinMax(columInfo: Array<NoUniformColObj>) {
    let min = Infinity;
    let max = -Infinity;

    for (let j = 0; j < columInfo.length; j++) {
        if (columInfo[j].vRange[0] < min) {
            min = columInfo[j].vRange[0];
        }

        if (columInfo[j].vRange[1] > max) {
            max = columInfo[j].vRange[1];
        }

    }

    return {
        min,
        max
    }
}
