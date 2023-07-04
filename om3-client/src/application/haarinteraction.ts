import { MultiHaarTimeSeriesObj } from "@/store";
import * as d3 from 'd3';

export function drawMultiHaarTimeSeries(multiTimeSeriesObj: MultiHaarTimeSeriesObj) {
    const initTimeRange = [0, 2 ** multiTimeSeriesObj.maxLevel - 1];

    let interactiveInfo = {
        startX: 0,
        startY: 0,
        offsetX: 0,
        offsetY: 0,
        isMouseDown: false,
        isMove: false,
    };
    //let currentLevel = multiTimeSeriesObj.currentLevel;
    const pading = { top: 20, bottom: 20, left: 40, right: 20 };
    const svg = d3.select("#content-container").append("svg");
    document
        .getElementById("content-container")!
        .addEventListener("mouseup", () => {
            interactiveInfo.isMouseDown = false;
        });
    svg
        .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
        .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${multiTimeSeriesObj.x},${multiTimeSeriesObj.y})`)
        .style("background-color", "#fff");

    const foreignId=`foreign${multiTimeSeriesObj.width}`;
    let foreignObj: any = svg.append("foreignObject").attr("id", foreignId).attr("x", pading.left).attr("y", pading.top).attr('width', multiTimeSeriesObj.width).attr('height', multiTimeSeriesObj.height);
    const canvas = document.createElement("canvas");
    document.getElementById(foreignId)?.appendChild(canvas);
    canvas.width = multiTimeSeriesObj.width;
    canvas.height = multiTimeSeriesObj.height;
    let ctx = canvas.getContext("2d");

    const xScale: any = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0,multiTimeSeriesObj.width]);
    const yScale: any = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height,0 ]);

    const xAxis = d3.axisBottom(xScale);
    const yAxis = d3.axisLeft(yScale);

    const xAxisG = svg.append("g").attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top})`).attr("class", 'x axis').call(xAxis);
    const yAxisG = svg.append("g").attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);

    function draw() {
            console.log(multiTimeSeriesObj);
            const allRenderData=multiTimeSeriesObj.allRenderData;
            ctx?.clearRect(0,0,multiTimeSeriesObj.width,multiTimeSeriesObj.height);
            //@ts-ignore
            //ctx?.strokeStyle="steelblue";
            for(let i=0;i<allRenderData.length;i++){
                ctx?.beginPath();
                const renderData=allRenderData[i].renderData
                for(let j=0;j<renderData.length;j++){
                    renderData[j].y=yScale(renderData[j].v)
                    if(j===0){
                        ctx?.moveTo(xScale(renderData[j].x),renderData[j].y);
                    }else{
                        ctx?.lineTo(xScale(renderData[j].x),renderData[j].y);
                    }
                }
                ctx?.stroke();
            }
    }
    draw();

}