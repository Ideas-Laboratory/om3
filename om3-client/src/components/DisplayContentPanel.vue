<template>
  <div class="display-content-panel ms-1">
    <device-control-panel></device-control-panel>
    <div id="content-container"></div>
  </div>
</template>
<script>
import { defineComponent, onMounted, ref, watch } from "vue";
import DeviceControlPanel from "./DeviceControlPanel.vue";
import * as d3 from "d3";
import store, { LineChartObj, emitter } from "../store";
import {drawWaveletContent,drawOrdinaryWaveletContent} from '../application/draw-svg-panel';
import {drawViewChangeLineChart} from "../application/line-interaction";
import {drawMultiTimeSeries as drawStocksTimeSeries} from "../application/multi-interaction"
import {drawMultiTimeSeriesM4} from "../application/m4interaction";
import {drawMultiHaarTimeSeries} from "../application/haarinteraction";

export default defineComponent({
  components: { DeviceControlPanel },
  setup() {
    function drawContent(lineChartObj) {
      console.log(lineChartObj);
    
      let interactiveInfo = {
        startX: 0,
        startY: 0,
        offsetX: 0,
        offsetY: 0,
        isMouseDown: false,
        isMove: false,
      };
      const pading = { top: 20, bottom: 20, left: 40, right: 20 };
      const svg = d3.select("#content-container").append("svg");
      document
        .getElementById("content-container")
        .addEventListener("mouseup", () => {
          interactiveInfo.isMouseDown = false;
        });
      svg
        .attr("width", lineChartObj.width + pading.left + pading.right)
        .attr("height", lineChartObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${lineChartObj.x},${lineChartObj.y})`)
        .on("mousedown", function (e) {
          interactiveInfo.isMouseDown = true;
          interactiveInfo.startX = e.x;
          interactiveInfo.startY = e.y;
        })
        .on("mousemove", function (e) {
          if (interactiveInfo.isMouseDown) {
            interactiveInfo.offsetX = e.x - interactiveInfo.startX;
            interactiveInfo.offsetY = e.y - interactiveInfo.startY;
            d3.select(this).attr(
              "transform",
              `translate(${lineChartObj.x + interactiveInfo.offsetX},${
                lineChartObj.y + interactiveInfo.offsetY
              })`
            );
            interactiveInfo.isMove = true;
          }
        })
        .on("mouseup", function () {
          if (interactiveInfo.isMouseDown) {
            lineChartObj.x += interactiveInfo.offsetX;
            lineChartObj.y += interactiveInfo.offsetY;
            interactiveInfo.isMouseDown = false;
            // interactiveInfo.isMove=false;
          }
        })
        .on("click", function (e) {
          if (!interactiveInfo.isMove) {
            if (lineChartObj.isChoosed) {
              d3.select(this)
                .style("border", "solid")
                .style("border-width", "1px")
                .style("border-color", "#fff");
              lineChartObj.isChoosed = false;
            } else {
              d3.select(this)
                .style("border", "solid")
                .style("border-width", "1px")
                .style("border-color", "#84fcb6");
              lineChartObj.isChoosed = true;
            }
          } else {
            interactiveInfo.isMove = false;
          }
        })
        .style("background-color", "#fff");
      let xScale = null;
      if (lineChartObj.isSample) {

        if(lineChartObj.algorithm==='haar'){
          xScale = d3
          .scaleLinear()
          .domain([0, lineChartObj.width*4])
          .range([0, lineChartObj.width]);
        }else{
          xScale = d3
          .scaleLinear()
          .domain([0, lineChartObj.width])
          .range([0, lineChartObj.width]);
        }
        
      } else {
        xScale = d3
          .scaleTime()
          .domain([lineChartObj.timeRange[0], lineChartObj.timeRange[1]])
          .range([0, lineChartObj.width]);
      }
      const yScale = d3
        .scaleLinear()//lineChartObj.data.min, lineChartObj.data.max
        .domain([lineChartObj.data.min, lineChartObj.data.max])
        .range([lineChartObj.height, 0]);

      const xAxis = d3.axisBottom(xScale);
      const yAxis = d3.axisLeft(yScale);
      xAxis.ticks(0)
      yAxis.ticks(0)

      const xAxisG = svg
        .append("g")
        .attr(
          "transform",
          `translate(${pading.left},${lineChartObj.height + pading.top})`
        )
        .call(xAxis);
      const yAxisG = svg
        .append("g")
        .attr("transform", `translate(${pading.left},${pading.top})`)
        .call(yAxis);
      const lineG = svg
        .append("g")
        .attr("transform", `translate(${pading.left},${pading.top})`);
      let line = null;
      if (lineChartObj.isSample) {
        line = d3
          .line()
          .x((d, i) => xScale(d.x))
          .y((d, i) => {
            d.y=yScale(d.y);
            return d.y;
          });

      } else {
        line = d3
          .line()
          .x((d, i) => d.x)
          .y((d, i) => lineChartObj.height-d.y);
      }
      console.log(lineChartObj.data.val)
      lineG
        .append("path")
        .datum(lineChartObj.data.val)
        .attr("fill", "none")
        .attr("stroke", "steelblue")
        .attr("stroke-width", 1.5)
        .attr("d", line);

      // svg
      //   .append("text")
      //   .text(
      //     lineChartObj.isSample
      //       ? `${lineChartObj.algorithm} size:${lineChartObj.data.val.length}`
      //       : `${lineChartObj.algorithm} size:${lineChartObj.data.val.length} ${lineChartObj.timeRange[0]
      //           .toISOString()
      //           .replace(/[TZ]/, " ")}-${lineChartObj.timeRange[1]
      //           .toISOString()
      //           .replace(/[TZ]/, " ")}`
      //   )
      //   .attr("x", lineChartObj.width / 2 + pading.left)
      //   .attr("y", pading.top)
      //   .attr("text-anchor", "middle")
      //   .attr("font-size", 10);

      svg
        .style("border", "solid")
        .style("border-width", "1px")
        .style("border-color", "#fff");
        svg.on("click",()=>{
          const payload={
            width:500
          }
          store.dispatch("loadDataForSampleByM41",payload)
        })
    }

    emitter.on("add_line_chart_obj", drawContent);
    emitter.on("add_wavelet_chart_obj",drawWaveletContent);
    emitter.on("add_view_change_query_obj",drawViewChangeLineChart);
    emitter.on("add_multi_timeseries_obj",drawStocksTimeSeries);
    emitter.on("add_multi_timeseries_m4_obj",drawMultiTimeSeriesM4);
    emitter.on("add_multi_haar_timeseries_obj",drawMultiHaarTimeSeries)
    //#e5e5e5
  }
});
</script>
<style scoped>
.display-content-panel {
  height: 100%;
  flex-grow: 1;
  background-color:"#fff" ;
}

</style>