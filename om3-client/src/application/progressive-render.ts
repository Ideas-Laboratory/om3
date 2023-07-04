import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import store, { ViewChangeLineChartObj } from "@/store";
import * as d3 from 'd3';
import { formatRenderDataForViewChange, formatNonPowDataForViewChange } from '../helper/format-data';

export function drawViewChangeLineChart(lineChartObj: { width: number, height: number, x: number, y: number, minv: number, maxv: number }) {
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
    const yScale = d3
        .scaleLinear()
        .domain([lineChartObj.minv, lineChartObj.maxv])
        .range([lineChartObj.height, 0]);
    const logicYScale = d3
        .scaleLinear()
        .domain([lineChartObj.minv, lineChartObj.maxv])
        .range([0, lineChartObj.height]);
    let xAxisG: any = null;
    let yAxisG: any = null;
    let foreignObj: any = null;
    let ctx: any = null;
    let canvas :any= null
    const draw = function (nonUniformColObjs: Array<NoUniformColObj>,name:string) {
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
            canvas = document.createElement("canvas");
            document.getElementById("foreign")?.appendChild(canvas);
            canvas.width = lineChartObj.width;
            canvas.height = lineChartObj.height;
            ctx = canvas.getContext("2d");
        }
        // console.log(nonUniformColObjs);
        ctx.clearRect(0, 0, lineChartObj.width, lineChartObj.height);
        ctx.beginPath();
        ctx.imageSmoothingEnabled = false;
        ctx.fillStyle = "white";
        ctx.fillRect(0, 0, lineChartObj.width, lineChartObj.height);
        ctx.strokeStyle = "black"
        formatNonPowDataForViewChange(nonUniformColObjs, lineChartObj.width, 2 ** 25-1, yScale)
        for (let i = 0; i < nonUniformColObjs.length; i++) {
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

                ctx.moveTo(nonUniformColObjs[i].positionInfo.endX, yScale(nonUniformColObjs[i].endV!));
                ctx.lineTo(nonUniformColObjs[i + 1].positionInfo.startX, yScale(nonUniformColObjs[i + 1].startV!));
            }
        }
        //console.log(nonUniformColObjs)
        ctx.stroke();
        savePNG(canvas,name)

        //console.log("draw.......")

    }
    //console.log(lineChartObj.nonUniformColObjs)
    //draw(lineChartObj.nonUniformColObjs );

    return draw

}

function savePNG(canvas: any,name:string) {
    const imgURL = canvas.toDataURL("image/png");
    const dlLink = document.createElement("a")
    dlLink.download = `32m_resize_4000_${name}`
    dlLink.href = imgURL
    dlLink.dataset.downloadurl = ["image/png", dlLink.download, dlLink.href].join(":")
    document.body.appendChild(dlLink)
    dlLink.click()
    document.body.removeChild(dlLink)
}