

import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import store, { ViewChangeLineChartObj } from "@/store";
import * as d3 from 'd3';
import { formatRenderDataForViewChange, formatNonPowDataForViewChange } from '../helper/format-data';

export function drawViewChangeLineChart(lineChartObj: ViewChangeLineChartObj) {
    const initTimeRange = [0, 2 ** store.state.controlParams.tableMaxLevel - 1];
    console.log(lineChartObj);
    let interactiveInfo = {
        startX: 0,
        startY: 0,
        offsetX: 0,
        offsetY: 0,
        isMouseDown: false,
        isMove: false,
    };
    let currentLevel = Math.log2(lineChartObj.width);
    const pading = { top: 20, bottom: 20, left: 40, right: 20 };
    const svg = d3.select("#content-container").append("svg");
    svg
        .attr("width", lineChartObj.width + pading.left + pading.right)
        .attr("height", lineChartObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${lineChartObj.x},${lineChartObj.y})`)
        .style("background-color", "#fff");


    let lineG: any = null
    let xScale: any = d3
        .scaleLinear()
        .domain([0, lineChartObj.width])
        .range([0, lineChartObj.width]);
    const minV = 0;
    const maxV = 120;
    console.log(lineChartObj.data.minv, lineChartObj.data.maxv)
    const yScale = d3
        .scaleLinear()
        .domain([lineChartObj.data.minv, lineChartObj.data.maxv])
        .range([lineChartObj.height, 0]);
    const logicYScale = d3
        .scaleLinear()
        .domain([lineChartObj.data.minv, lineChartObj.data.maxv])
        .range([0, lineChartObj.height]);
    let xAxisG: any = null;
    let yAxisG: any = null;
    let foreignObj: any = null;
    let ctx: any = null;

    function draw(nonUniformColObjs?: Array<NoUniformColObj>) {
        if (Math.log2(lineChartObj.width) === Math.floor(Math.log2(lineChartObj.width))) {
            xScale.domain([0, lineChartObj.width]).range([0, lineChartObj.width]);
        } else {
            xScale.domain([0, lineChartObj.width]).range([0, lineChartObj.width]);
        }

        let xScaleShow = d3.scaleLinear().domain([0, lineChartObj.width]).range([0, lineChartObj.width]);

        const xAxis = d3.axisBottom(xScaleShow);
        const yAxis = d3.axisLeft(yScale);

        if (xAxisG != null) {
            xAxisG.remove();
            xAxisG = null;
        }
        if (yAxisG != null) {
            yAxisG.remove();
            yAxisG = null;
        }
        xAxis.ticks(0)
        yAxis.ticks(0)
        xAxisG = svg
            .append("g")
            .attr(
                "transform",
                `translate(${pading.left},${lineChartObj.height + pading.top})`
            ).attr("class", 'x axis')
            .call(xAxis);
        yAxisG = svg
            .append("g")
            .attr("transform", `translate(${pading.left},${pading.top})`)
            .attr("class", 'y axis')
            .call(yAxis);


        lineChartObj.data.powRenderData.forEach(v => {
            v.y = yScale(v.y)
        });
        lineChartObj.data.noPowRenderData.forEach(v => {
            v.y = yScale(v.y)
        })

        let line = d3
            .line()
            .x((d: any, i) => xScale(d.x))
            .y((d: any, i) => d.y);
        if (lineG != null) {
            lineG.remove();
            lineG = null;
        }
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


        if (nonUniformColObjs) {
            // console.log(nonUniformColObjs);
            ctx.clearRect(0, 0, lineChartObj.width, lineChartObj.height);
            ctx.beginPath();
            ctx.strokeStyle = "steelblue"
            const startRenderTime=new Date().getTime();
            for (let i = 0; i < nonUniformColObjs.length; i++) {
                if(nonUniformColObjs[i].isMis){
                    continue
                }

                if (nonUniformColObjs[i].startV !== undefined && nonUniformColObjs[i].endV !== undefined) {
                    if (nonUniformColObjs[i].startV! < nonUniformColObjs[i].endV!) {
                        ctx.moveTo(nonUniformColObjs[i].positionInfo.minX, yScale(nonUniformColObjs[i].vRange[0]));
                        ctx.lineTo(nonUniformColObjs[i].positionInfo.maxX, yScale(nonUniformColObjs[i].vRange[1]));
                    } else {
                        ctx.moveTo(nonUniformColObjs[i].positionInfo.minX, yScale(nonUniformColObjs[i].vRange[1]));
                        ctx.lineTo(nonUniformColObjs[i].positionInfo.maxX, yScale(nonUniformColObjs[i].vRange[0]));
                    }
                } else {
                    if (nonUniformColObjs[i].minVTimeRange[0] < nonUniformColObjs[i].maxVTimeRange[0]) {
                        ctx.moveTo(nonUniformColObjs[i].positionInfo.minX, yScale(nonUniformColObjs[i].vRange[0]));
                        ctx.lineTo(nonUniformColObjs[i].positionInfo.maxX, yScale(nonUniformColObjs[i].vRange[1]));
                    } else {
                        ctx.moveTo(nonUniformColObjs[i].positionInfo.minX, yScale(nonUniformColObjs[i].vRange[1]));
                        ctx.lineTo(nonUniformColObjs[i].positionInfo.maxX, yScale(nonUniformColObjs[i].vRange[0]));
                    }
                }
                if (i <= nonUniformColObjs.length - 2 && nonUniformColObjs[i].endV !== undefined && nonUniformColObjs[i + 1] !== undefined) {

                    // ctx.moveTo(nonUniformColObjs[i].positionInfo.endX, yScale(nonUniformColObjs[i].endV!));
                    // ctx.lineTo(nonUniformColObjs[i + 1].positionInfo.startX, yScale(nonUniformColObjs[i + 1].startV!));
                }

            }
            const stack=[];
            for(let i=0;i<nonUniformColObjs.length-1;i++){
                if(!nonUniformColObjs[i].isMis&&nonUniformColObjs[i+1].isMis){
                    stack.push(nonUniformColObjs[i]);
                    for(let j=i+1;j<nonUniformColObjs.length;j++){
                        if(nonUniformColObjs[j-1].isMis&&!nonUniformColObjs[j].isMis){
                           const co= stack.pop()
                           ctx.moveTo(co?.positionInfo.minX,yScale(co!.vRange[1]));
                           if(nonUniformColObjs[j].startV!==undefined){
                            ctx.lineTo(nonUniformColObjs[j].positionInfo.startX,yScale(nonUniformColObjs[j].startV!))
                           }else{
                            ctx.lineTo(nonUniformColObjs[j].positionInfo.minX, yScale((nonUniformColObjs[j].vRange[0]+nonUniformColObjs[j].vRange[1])/2))
                           }
                        
                        }
                    }
                }
            }
            ctx.stroke();
            console.log("render Time:",new Date().getTime()-startRenderTime)
        } else {
            console.log("pow")
            lineG =
                svg
                    .append("g")
                    .attr("transform", `translate(${pading.left},${pading.top})`)
                    .append("path")
                    .datum(() => {
                        if (Math.log2(lineChartObj.width) === Math.floor(Math.log2(lineChartObj.width))) {
                            return lineChartObj.data.powRenderData;
                        } else {
                            return lineChartObj.data.noPowRenderData;
                        }
                    })
                    .attr("fill", "none")
                    .attr("stroke", "steelblue")
                    .attr("stroke-width", 1.5)
                    .attr("stroke-linejoin", "round")
                    .attr("stroke-linecap", "round")
                    .attr("d", line);
        }

    }
 
    draw(!lineChartObj.isPow ? lineChartObj.nonUniformColObjs : undefined);
 

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
            // interactiveInfo.isMouseDown = false;
            // isMouseover = false;
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
            //lineChartObj.dataManager.getData(19,0,1)
            if (interactiveInfo.isMouseDown) {
                interactiveInfo.isMouseDown = false;
                isMouseover = false;
                document.body.style.cursor = 'default';
                let preWidth = lineChartObj.width;
                lineChartObj.width = lineChartObj.width + (e.offsetX - startOffsetX);
                if (lineChartObj.width === preWidth) {
                    return
                }
                currentLevel = Math.ceil(Math.log2(lineChartObj.width));
                currentLevel = 20

                lineChartObj.dataManager.getData(currentLevel, 0, 2 ** (20) - 1).then(res => {
                    // const { noPowResult, maxv, minv, powRenderData } = formatRenderDataForViewChange([lineChartObj.dataManager.maxLevel, Math.floor(Math.log2(lineChartObj.width))], res);
                    // lineChartObj.data.maxv = maxv;
                    // lineChartObj.data.minv = minv;
                    // lineChartObj.data.noPowRenderData = noPowResult;
                    // lineChartObj.data.powRenderData = powRenderData;
                    //@ts-ignore
                    currentLevel = Math.ceil(Math.log2(lineChartObj.width));
                    lineChartObj.params = [0, Math.floor(Math.log2(lineChartObj.width))];
                    // lineChartObj.dataManager.viewChangeInteraction(currentLevel, lineChartObj.width, [0, 2 ** lineChartObj.dataManager.maxLevel - 1], logicYScale).then(noUniformColObjs => {
                    //     ///console.log(noUniformColObjs);
                    //     const nonUniformRenderData = formatNonPowDataForViewChange(noUniformColObjs, lineChartObj.width, 2 ** lineChartObj.dataManager.maxLevel, yScale);
                    //     lineChartObj.data.noPowRenderData = nonUniformRenderData;
                    //     draw(noUniformColObjs);
                    //     //lineChartObj.dataManager.lruCacheDelete()
                    // });
                });
            }
        });
    svg.on("contextmenu", (e) => {
        lineChartObj.width = 2 ** Math.floor(Math.log2(lineChartObj.width));
        console.log(lineChartObj);
        svg.attr("width", lineChartObj.width + pading.right + pading.left);
        dragRect.attr("width", lineChartObj.width + pading.right + pading.left);
        draw();

        e.preventDefault();
        e.stopPropagation();
    });
   const randomInter:Array<Array<number>>=[]
   
    svg.on("click", () => {
      console.log("click")
    })

}

