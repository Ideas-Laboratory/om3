import store, { WaveletLineChartObj, SimpleBrushChartObj } from '../store/index'
import { WaveletTree } from '../helper/tree'
import { AxisDistortion, FisheyeDistortion } from '../helper/distortion'
import * as d3 from 'd3'
import axios from 'axios'
import { waveletDecode } from '../helper/wavlet-decoder'
import { formatDataForWaveletBrush } from '../helper/format-data'
import { computeTimeRangeChild } from '../helper/util'



export function drawWaveletContent(waveletObj: WaveletLineChartObj) {
    const initTimeRange = [0, 2 ** store.state.controlParams.tableMaxLevel - 1];
    console.log(waveletObj);
    let interactiveInfo = {
        startX: 0,
        startY: 0,
        offsetX: 0,
        offsetY: 0,
        isMouseDown: false,
        isMove: false,
    };
    const padding = { top: 20, bottom: 20, left: 40, right: 20 };
    const root: WaveletTree = waveletObj.root;
    const cols = waveletObj.width;
    let distortion = waveletObj.distortion;
    let renderData: Array<any> = [];
    root.clearListeners(true);
    const fisheye = FisheyeDistortion(d3.scaleLinear(), 0, 0).domain([0, cols]).range([0, cols]);
    //@ts-ignore
    const xScale = AxisDistortion(distortion).domain([0, cols]).range([0, cols]);
    //@ts-ignore
    const yScale = d3.scaleLinear().domain([root.yArray[2], root.yArray[3]]).range([waveletObj.height, 0]);

    const xAxis = d3.axisBottom(xScale);
    const yAxis = d3.axisLeft(yScale);
    let distortionCache = distortion;
    let cacheData: Array<any> = [];
    const svg = d3.select("#content-container").append("svg");
    svg.attr("width", cols + padding.left + padding.right)
        .attr("height", waveletObj.height + padding.top + padding.bottom)
        .attr("transform", `translate(${waveletObj.x},${waveletObj.y})`)
        .style("background-color", "#fff");

    svg.append("g").attr("class", "x axis").attr("transform", `translate(${padding.left},${waveletObj.height + padding.top})`).call(xAxis);
    svg.append("g").attr("class", "y axis").attr("transform", `translate(${padding.left},${padding.top})`).call(yAxis);

    const rerender = () => {
        //@ts-ignore
        let [pointer, , mergedDistortion] = distortion.reduce(([pointer, previous, d], v) => {
            if (v === previous) {
                const arr = d.pop();
                //@ts-ignore
                arr[1] += v;
                //@ts-ignore
                d.push(arr);
                return [pointer + v, v, d];
            }
            //@ts-ignore
            d.push([pointer, pointer + v, v, null]);
            return [pointer + v, v, d];
        }, [0, null, []]);
        mergedDistortion = mergedDistortion.map((d: any) => {
            return d.map((dd: number, i: number) => (i > 2 ? dd : (dd / pointer) * cols))
        });
        root.clearListeners(true);
        xScale.changeDistortion(distortion);
        //@ts-ignore
        svg.select(".x.axis").call(xAxis);
        if (cacheData) {
            renderData = cacheData;
        }
        mergedDistortion.forEach((distortionSlice: any) => {
            root.getDetailData(distortionSlice.slice(0, 2), distortionSlice[2], (partialData: any) => {
                distortionSlice[3] = partialData;
                if (!mergedDistortion.filter((m: any) => !m[3]).length) {
                    const data = mergedDistortion.reduce((p: any, v: any) => p.concat(v[3]), []).sort((a: any, b: any) => a.x - b.x);
                    cacheData = data;
                    renderData = data;
                }
            });
        });
    }
    let zoomDetail = 0;
    //@ts-ignore
    const calDistortion = (pointerX, refresh = false) => {
        if (refresh) {
            fisheye.focus(cols / 2);
            let pointer = 0;
            //@ts-ignore
            distortionCache = distortion.map((_, i) => {
                const fishRes = fisheye(i + 1);
                let result = fishRes - pointer;
                if (result > 1) {
                    result = 1 / Math.round(result);
                } else {
                    result = Math.round(1 / result);
                }
                pointer = fishRes;
                return result;
            });
        }
        const distortionEdgeValue = distortionCache[0];
        distortion = distortionCache.slice(Math.max(0, Math.round(cols / 2 - pointerX)), Math.round((cols * 3) / 2 - pointerX));
        if (pointerX < cols / 2) {
            distortion = distortion.concat(new Array(cols - distortion.length).fill(distortionEdgeValue));
        } else {
            distortion = new Array(cols - distortion.length).fill(distortionEdgeValue).concat(distortion);
        }
    }

    const zoomIn = (pointerX: number) => {
        fisheye.distortion(++zoomDetail);
        calDistortion(pointerX, true);
    }

    const zoomOut = (pointerX: number) => {
        if (zoomDetail === 0) return;
        fisheye.distortion(--zoomDetail);
        calDistortion(pointerX, true);
    }
    svg.on("mousemove", function (e) {
        if (waveletObj.isChoosed) {
            const pointerX = e.offsetX - 40;
            if (zoomDetail !== 0) {
                calDistortion(pointerX);
                rerender()
            }
        } else {
            if (interactiveInfo.isMouseDown) {
                interactiveInfo.offsetX = e.x - interactiveInfo.startX;
                interactiveInfo.offsetY = e.y - interactiveInfo.startY;
                d3.select(this).attr(
                    "transform",
                    `translate(${waveletObj.x + interactiveInfo.offsetX},${waveletObj.y + interactiveInfo.offsetY
                    })`
                );
                interactiveInfo.isMove = true;
            }
        }

    });

    svg.on("mousedown", function (e) {
        if (waveletObj.isChoosed) {
            const pointerX = e.offsetX - 40;
            switch (e.which) {
                case 1:
                    zoomIn(pointerX);
                    break;
                case 2:
                    zoomDetail = 0;
                    distortion = new Array(cols).fill(1);
                    distortionCache = distortion;
                    break;
                case 3:
                    zoomOut(pointerX);
                    break;
            }
            svg.select(".title").text(waveletObj.algorithm + " method:" + waveletObj.denoiseMethod + " threshold:" + waveletObj.denoiseThreshold + " level:" + zoomDetail)
            rerender();
        } else {
            interactiveInfo.isMouseDown = true;
            interactiveInfo.startX = e.x;
            interactiveInfo.startY = e.y;
        }

    });
    svg.on("mouseup", function (e) {
        if (interactiveInfo.isMouseDown && !waveletObj.isChoosed) {
            waveletObj.x += interactiveInfo.offsetX;
            waveletObj.y += interactiveInfo.offsetY;
            interactiveInfo.isMouseDown = false;
            interactiveInfo.isMove = false;
        }
    });
    svg.on("contextmenu", function (e) {
        if (waveletObj.isChoosed) {
            d3.select(this)
                .style("border", "solid")
                .style("border-width", "1px")
                .style("border-color", "#fff");
            waveletObj.isChoosed = false;
        } else {
            d3.select(this)
                .style("border", "solid")
                .style("border-width", "1px")
                .style("border-color", "#84fcb6");
            waveletObj.isChoosed = true;
        }
        e.preventDefault();
        e.stopPropagation();
    });
    // svg.on("wheel", function (e) {
    //     const pointerX = e.offsetX - 40;
    //     if (e.deltaY > 0) {
    //         zoomIn(pointerX);
    //     } else {
    //         zoomOut(pointerX);
    //     }
    //     rerender();
    // });
    svg.append("text")
        .attr("class", 'title')
        .text(waveletObj.algorithm + " method:" + waveletObj.denoiseMethod + " threshold:" + waveletObj.denoiseThreshold + " level:" + zoomDetail)
        .attr("x", waveletObj.width / 2 + padding.left)
        .attr("y", padding.top)
        .attr("text-anchor", "middle")
        .attr("font-size", 10);

    rerender();

    const line = d3.line().x((d: any) => xScale(d.x)).y((d: any) => yScale(d.y));

    let lineG: any = null;

    function drawToSvg(renderData: any, line: any) {
        if (lineG) {
            //@ts-ignore
            lineG.remove();
            lineG = null;
        }
        lineG = svg.append('g').attr("transform", `translate(${padding.left},${padding.top})`);
        lineG.append('path')
            .datum(renderData)
            .attr('fill', 'none')
            .attr('stroke', 'steelblue')
            .attr('stroke-width', 1.5)
            .attr("stroke-linejoin", "round")
            .attr("stroke-linecap", "round")
            .attr("d", line);

        //@ts-ignore
        // requestAnimationFrame(drawToSvg.bind(this,renderData,line));
    }

    setInterval(() => {
        drawToSvg(renderData, line)
    }, 100)

    //requestAnimationFrame(drawToSvg.bind(null,renderData,line));
}

export function drawOrdinaryWaveletContent(lineChartObj: SimpleBrushChartObj) {
    console.log(lineChartObj);
    const initTimeRange = [0, 2 ** store.state.controlParams.tableMaxLevel - 1];
    const maxLevel = store.state.controlParams.tableMaxLevel;
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
    document
        .getElementById("content-container")!
        .addEventListener("mouseup", () => {
            interactiveInfo.isMouseDown = false;
        });
    svg
        .attr("width", lineChartObj.width + pading.left + pading.right)
        .attr("height", lineChartObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${lineChartObj.x},${lineChartObj.y})`)
        // .on("mousedown", function (e) {
        //     interactiveInfo.isMouseDown = true;
        //     interactiveInfo.startX = e.x;
        //     interactiveInfo.startY = e.y;
        // })
        // .on("mousemove", function (e) {
        //     if (interactiveInfo.isMouseDown) {
        //         interactiveInfo.offsetX = e.x - interactiveInfo.startX;
        //         interactiveInfo.offsetY = e.y - interactiveInfo.startY;
        //         d3.select(this).attr(
        //             "transform",
        //             `translate(${lineChartObj.x + interactiveInfo.offsetX},${lineChartObj.y + interactiveInfo.offsetY
        //             })`
        //         );
        //         interactiveInfo.isMove = true;
        //     }
        // })
        // .on("mouseup", function () {
        //     if (interactiveInfo.isMouseDown) {
        //         lineChartObj.x += interactiveInfo.offsetX;
        //         lineChartObj.y += interactiveInfo.offsetY;
        //         interactiveInfo.isMouseDown = false;
        //         // interactiveInfo.isMove=false;
        //     }
        // })
        // .on("click", function (e) {
        //     if (!interactiveInfo.isMove) {
        //         if (lineChartObj.isChoosed) {
        //             d3.select(this)
        //                 .style("border", "solid")
        //                 .style("border-width", "1px")
        //                 .style("border-color", "#fff");
        //             lineChartObj.isChoosed = false;
        //         } else {
        //             d3.select(this)
        //                 .style("border", "solid")
        //                 .style("border-width", "1px")
        //                 .style("border-color", "#84fcb6");
        //             lineChartObj.isChoosed = true;
        //         }
        //     } else {
        //         interactiveInfo.isMove = false;
        //     }
        // })
        .style("background-color", "#fff");
    let lineG: any = null
    let xScale: any = d3
        .scaleLinear()
        .domain([0, lineChartObj.width])
        .range([0, lineChartObj.width]);
    let xTimeRangeScale = d3
        .scaleLinear()
        .domain(initTimeRange)
        .range([0, lineChartObj.width]);

    const yScale = d3
        .scaleLinear()
        .domain([lineChartObj.data.min, lineChartObj.data.max])
        .range([lineChartObj.height, 0]);
    let xAxisG: any = null;
    let yAxisG: any = null;
    function draw(timeRange: Array<number>) {

        xScale.domain([0, lineChartObj.width]).range([0, lineChartObj.width]);
        yScale.domain([lineChartObj.data.min, lineChartObj.data.max]).range([lineChartObj.height, 0]);

        xTimeRangeScale.domain(timeRange).range([0, lineChartObj.width]);
        const xAxis = d3.axisBottom(xTimeRangeScale);
        const yAxis = d3.axisLeft(yScale);

        if (xAxisG != null) {
            xAxisG.remove();
            xAxisG = null;
        }
        if (yAxisG != null) {
            yAxisG.remove();
            yAxisG = null;
        }
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
            .y((d: any, i) => yScale(d.y));
        if (lineG != null) {
            lineG.remove();
            lineG = null;
        }
        lineG =
            svg
                .append("g")
                .attr("transform", `translate(${pading.left},${pading.top})`)
                .append("path")
                .datum(lineChartObj.data.val)
                .attr("fill", "none")
                .attr("stroke", "steelblue")
                .attr("stroke-width", 1.5)
                .attr("stroke-linejoin", "round")
                .attr("stroke-linecap", "round")
                .attr("d", line);
    }
    draw(initTimeRange);
    svg
        .append("text")
        .text('brush line chart'
        )
        .attr("x", lineChartObj.width / 2 + pading.left)
        .attr("y", pading.top)
        .attr("text-anchor", "middle")
        .attr("font-size", 10);


    const rect = svg.append('rect')
        .attr('x', pading.left)
        .attr('y', pading.top)
        .attr('width', lineChartObj.width / 2)
        .attr('height', lineChartObj.height)
        .attr('stroke-width', 2)
        .attr('stroke', 'red')
        .attr('fill', 'none');


    svg.on('mousemove', function (e) {
        const offsetX = e.offsetX;
        let rectX = offsetX - lineChartObj.width / 4;
        if (rectX < pading.left) {
            rectX = pading.left;
        }
        if (rectX > pading.left + lineChartObj.width - lineChartObj.width / 2) {
            rectX = pading.left + lineChartObj.width - lineChartObj.width / 2;
        }
        rect.attr('x', rectX);
    });
    svg.on('click', function (e) {
        const offsetX = e.offsetX;
        let rectX = offsetX - lineChartObj.width / 4;
        if (offsetX < pading.left || offsetX > pading.left + lineChartObj.width) {
            console.log("failed");
            return
        }
        if (rectX < pading.left) {
            rectX = pading.left;
        }
        if (rectX > pading.left + lineChartObj.width - lineChartObj.width / 2) {
            rectX = pading.left + lineChartObj.width - lineChartObj.width / 2;
        }
        if (currentLevel == maxLevel) {
            return;
        }
        const combinedUrl = `${'postgres'}/line_chart/wavelet_brush_progressive?table_name=${store.state.controlParams.currentTable}&end=${lineChartObj.data.rowData[0][1] + lineChartObj.width - 1}&start=${lineChartObj.data.rowData[0][1]}&width=${lineChartObj.width}&current_level=${currentLevel}&offset=${rectX + lineChartObj.width / 4 - pading.left}`;
        console.log(rectX + lineChartObj.width / 4 - pading.left, [rectX - pading.left, rectX + lineChartObj.width / 2 - pading.left])
        axios.get(combinedUrl).then(res => {
            const decodeRes = waveletDecode(lineChartObj.data.rowData, { mintd: res.data[1], minvd: res.data[2], maxvd: res.data[3], maxtd: res.data[4] }, [rectX - pading.left, rectX + lineChartObj.width / 2 - pading.left]);
            currentLevel++;
            const { result, rowData, maxv, minv } = formatDataForWaveletBrush(decodeRes, [store.state.controlParams.tableMaxLevel, currentLevel]);
            lineChartObj.data.max = maxv;
            lineChartObj.data.min = minv;
            lineChartObj.data.rowData = [res.data[0], ...rowData];
            lineChartObj.data.val = result;
           
            store.commit("updateBrushLineChartObj", lineChartObj);
            console.log(res.data)
            const timeRange = computeTimeRangeChild(currentLevel, maxLevel, [res.data[0][1], res.data[0][1] + lineChartObj.width - 1]);
            console.log(timeRange)
            draw(timeRange);
        });
    })
    svg
        .style("border", "solid")
        .style("border-width", "1px")
        .style("border-color", "#fff");

}

