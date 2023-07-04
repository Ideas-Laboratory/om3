import { emitter, MultiTimeSeriesM4Obj } from "@/store";
import * as d3 from 'd3';
//@ts-ignore
import * as seedRandom from "seedrandom";
import store from "../store"

//const isInit=false

export function drawMultiTimeSeriesM4(multiTimeSeriesObj: MultiTimeSeriesM4Obj) {

    const initTimeRange = [0, 2 ** multiTimeSeriesObj.maxLevel - 1];
    console.log(initTimeRange);

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

    const foreignId = `foreign${multiTimeSeriesObj.width}`;
    let foreignObj: any = svg.append("foreignObject").attr("id", foreignId).attr("x", pading.left).attr("y", pading.top).attr('width', multiTimeSeriesObj.width).attr('height', multiTimeSeriesObj.height);
    const canvas = document.createElement("canvas");
    document.getElementById(foreignId)?.appendChild(canvas);
    canvas.width = multiTimeSeriesObj.width;
    canvas.height = multiTimeSeriesObj.height;
    let ctx = canvas.getContext("2d");

    const xScale: any = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.width]);
    //multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv
    console.log("jelo")
    const yScale: any = d3.scaleLinear().domain([-43.33507, 38.054184]).range([multiTimeSeriesObj.height, 0]);

    const xAxis = d3.axisBottom(xScale);
    const yAxis = d3.axisLeft(yScale);

    const xAxisG = svg.append("g").attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top})`).attr("class", 'x axis').call(xAxis);
    const yAxisG = svg.append("g").attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);
    let timeRange = [0, 0];
    let allSum = 0;
    function draw() {
        timeRange = multiTimeSeriesObj.timeRange;
        console.log(timeRange)
        console.log(multiTimeSeriesObj);
        const allRenderData = multiTimeSeriesObj.renderData;
        ctx?.clearRect(0, 0, multiTimeSeriesObj.width, multiTimeSeriesObj.height);
        let sum = 0;

        //const colorArray=["#5677a4","#a7c9e6","#e68b39","#f5c283","#f5c283","#99d083","#b29b3c","#ecd06e","#5a9693","#8fbab6","#d4605b","#f1a29b","#77706e","#b8b0ad","#c97694","#f3c1d1","#aa7ba0","#cea7c7","#987862","#d2b6a7"]
        const colorArray1=["#1f77b4","#aec7e8","#ff7f0e","#ffbb78","#2ca02c","#98df8a","#d62728","#ff9896","#9467bd","#c5b0d5","#8c564b","#c49c94","#e377c2","#f7b6d2","#7f7f7f","#c7c7c7","#bcbd22","#dbdb8d","#17becf","#9edae5"];
        //@ts-ignore
        for (let i = 0; i < allRenderData.length; i++) {

            const rng = seedRandom(i)
            //@ts-ignore
            ctx.strokeStyle = colorArray1[19-i]//`rgb(${rng() * 255},${rng() * 255},${rng() * 255})`;
            ctx?.beginPath();
            const renderData = allRenderData[i].data
            sum += renderData.length;
            for (let j = 0; j < renderData.length; j++) {

                renderData[j].y = yScale(renderData[j].v)
                if (j === 0) {
                    ctx?.moveTo(renderData[j].x, renderData[j].y);
                } else {
                    ctx?.lineTo(renderData[j].x, renderData[j].y);
                }
            }
            ctx?.stroke();
        }
        console.log(sum);
        allSum += sum;
        console.log("all:", allSum);
    }
    function redraw(multObj: MultiTimeSeriesM4Obj) {
        multiTimeSeriesObj = multObj;
        draw();

    }
    //@ts-ignore
    emitter.on("update_multi_timeseries_m4_obj", redraw);
    draw();

    function downZoomIn() {
        let width = 2 ** 20
        let lastEnd = 2 ** 20;
        let lastStart = 0;
        //136597 935172
        return () => {
            width = width - 50000;
            let start = Math.floor(Math.random() * 50000) + lastStart;
            lastStart = start;
            let end = start + width - 1;
            const payload = {
                width: multiTimeSeriesObj.width,
                height: multiTimeSeriesObj.height,
                type: store.state.controlParams.currentTimeBoxType,
                start: start,
                end: end,
            };
            console.log(start, end);
            store.dispatch("loadMultiTimeSeriesM4Data", payload);
        }
    }
 

    function zoomIn() {
        console.log("zoom in")
        const payload = {
            width: multiTimeSeriesObj.width,
            height: multiTimeSeriesObj.height,
            type: store.state.controlParams.currentTimeBoxType,
            start: timeRange[0],
            end: timeRange[1] - 20000,
        };
        store.dispatch("loadMultiTimeSeriesM4Data", payload);
        //draw();
    }
    function zoomOut() {
        console.log("zooom out")
        const payload = {
            width: multiTimeSeriesObj.width,
            height: multiTimeSeriesObj.height,
            type: store.state.controlParams.currentTimeBoxType,
            start: timeRange[0],
            end: Math.floor(timeRange[1] * 2 + 1),
        };
        store.dispatch("loadMultiTimeSeriesM4Data", payload);
        //draw();
    }

    function pan(offset: number) {
        console.log("pan")
        const payload = {
            width: multiTimeSeriesObj.width,
            height: multiTimeSeriesObj.height,
            type: store.state.controlParams.currentTimeBoxType,
            start: timeRange[0] + offset,
            end: timeRange[1] + offset,
        };
        store.dispatch("loadMultiTimeSeriesM4Data", payload);
        // draw();
    }
    function resize(width: number) {
        console.log("resize")
        const payload = {
            width: width,
            height: multiTimeSeriesObj.height,
            type: store.state.controlParams.currentTimeBoxType,
            start: timeRange[0],
            end: timeRange[1],
        };
        store.dispatch("loadMultiTimeSeriesM4Data", payload);
        // draw();
    }
    function timeBox(info: { startX: number, width: number, vMin: number, vMax: number }) {
        console.time("timebox")
        const allRenderData = multiTimeSeriesObj.renderData;
        let sum = 0;
        //@ts-ignore
        for (let i = 0; i < allRenderData.length; i++) {
            const renderData = allRenderData[i].data
            let flag = true;
            for (let j = 0; j < renderData.length; j++) {

                //renderData[j].y = yScale(renderData[j].v)
                if (renderData[j].x >= info.startX && renderData[j].x <= info.startX + info.width) {
                    flag = flag && (renderData[j].v <= info.vMax && renderData[j].v >= info.vMin)
                }
            }
            if (flag) {
                sum++
            }
        }
        console.log(sum);
        console.timeEnd("timebox")

    }
    function doTimeBox(){
        const allTime=[];
        for(let i=0;i<50;i++){
            const startX=Math.random()*500;
            const width=Math.random()*200+300;
            const vMin=Math.random()*(multiTimeSeriesObj.maxv-multiTimeSeriesObj.minv)//+Math.random()*multiTimeSeriesObj.minv;
            const vMax=Math.random()*multiTimeSeriesObj.minv+800;
            const startT=new Date().getMilliseconds();
            timeBox({startX,width,vMin,vMax});
            allTime.push((new Date().getMilliseconds()-startT).toFixed(4));
        }
        console.log(allTime);
    }

    
    const zoomArray:Array<Array<number>>=[]
    function zoomInIn(index:number) {
            const zoomInfo=zoomArray[index];
            const payload = {
                width: multiTimeSeriesObj.width,
                height: multiTimeSeriesObj.height,
                type: store.state.controlParams.currentTimeBoxType,
                start: zoomInfo[0],
                end: zoomInfo[1],
            };
            store.dispatch("loadMultiTimeSeriesM4Data", payload);
    }
    let i=49;
    canvas.addEventListener("click", (e) => {
        doTimeBox();
       


    });





}