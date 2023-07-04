import { formatNonPowDataForViewChange } from "@/helper/format-data";
import { NoUniformColObj } from "@/model/non-uniform-col-obj";
import store, { MultiTimeSeriesObj } from "@/store";
import * as d3 from 'd3';
import { batchViewChange, batchGetData } from "../batch/m5batch";





let nameMap: any = [
    //green
    {
        "marutisktr": "汽车-#0e7735"
    },
    {
        "tatamotorssktr": "汽车-#329a51"
    },
    {
        "eichermotsktr": "汽车-#60ba6c"
    },
    {
        "heromotocosktr": "摩托-#94d391"
    },
    {
        "mmsktr": "汽车-#c0e6ba"
    },
    //oranges
    // {
    //     "bajfinancesktr": "金融-#b93d02"
    // },
    {
        "axisbanksktr": "金融-#d14904"
    },
    {
        "indusindbksktr": "金融-#e4580b"
    },
    // {
    //     "bajajfinsvsktr": "金融-#f87f2c"
    // },
    {
        "kotakbanksktr": "金融-#fda55e"
    },


    //color
    {
        "sbinsktr": "银行-#353535"
    },
    {
        "icicibanksktr": "银行-#4d4d4d"
    },
    {
        "hdfcsktr": "银行-#626262"
    },
    {
        "hdfcbanksktr": "银行-#757575"
    },
    //blue
    {
        "infysktr": "信息-#125ca4"
    },
    {
        "techmsktr": "信息-#3181bd"
    },
    {
        "hcltechsktr": "信息-#5ba3cf"
    },
    {
        "tcssktr": "信息-#8fc1de"
    },
    //purples
    {
        "coalindiasktr": "煤炭-#5c3696"
    },
    {
        "ongcsktr": "石油气-#684ea2"
    },
    {
        "iocsktr": "石油气-#7566ae"
    },
    {
        "bpclsktr": "石油-#827cb9"
    },
    {
        "powergridsktr": "电网-#928ec3"
    },
    {
        "ntpcsktr": "光伏-#a3a0cc"
    },

    //red
    {
        "britanniasktr": "食品-#b21218"
    },
    {
        "nestleindsktr": "雀巢-#dc2a25"
    },
    {
        "tataconsumsktr": "饮料-#f6573f"
    },
    //teals 
    {
        "drreddysktr": "制药-#127273"
    },
    {
        "sunpharmasktr": "制药-#2b8b8c"
    },
    {
        "divislabsktr": "制药-#4da5a4"
    },
    //greys
    {
        "jswsteelsktr": "钢铁-#353535"
    },
    {
        "tatasteelsktr": "钢铁-#4d4d4d"
    },
    {
        "ltsktr": "工程建筑-#626262"
    },
    {
        "uplsktr": "农药-#757575"
    },
    {
        "reliancesktr": "信实工业-#888888"
    },
    {
        "grasimsktr": "纺织-#9d9d9d"
    },
    //warmgreys
    {
        "ultracemcosktr": "水泥-#665c5a"
    },
    {
        "shreecemsktr": "水泥-#7e7673"
    },
    {
        "asianpaintsktr": "涂料-#98908c"
    },
    {
        "hindalcosktr": "铝铜-#b3aaa7"
    },


    {
        "itcsktr": "贸易-#d8b5a5"
    },
    {
        "adaniportssktr": "港口运营-#fcbfd2"
    },
    {
        "titansktr": "珠宝-#ffbf79"
    },
    {
        "hindunilvrsktr": "联合利华-#d9d9d9"
    },



]


let namedMap = new Map<any, any>()
for (let i = 0; i < nameMap.length; i++) {
    const key = Object.keys(nameMap[i])[0];
    namedMap.set(key, nameMap[i][key])
}



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



export function drawMultiTimeSeries(multiTimeSeriesObj: MultiTimeSeriesObj) {
    let realTimeStampRange: Array<number> = [multiTimeSeriesObj.startTimeStamp, multiTimeSeriesObj.endTimeStamp];
    let nodeIndexRange: Array<number> = [multiTimeSeriesObj.timeRange[0], multiTimeSeriesObj.timeRange[1]]

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
    //const pading = { top: 20, bottom: 80, left: 45, right: 250 };
    const pading = { top: 20, bottom: 80, left: 45, right: 20 };
    const svg = d3.select("#content-container").append("svg");
    svg
        .attr("width", multiTimeSeriesObj.width + pading.left + pading.right)
        .attr("height", multiTimeSeriesObj.height + pading.top + pading.bottom)
        .attr("transform", `translate(${multiTimeSeriesObj.x},${multiTimeSeriesObj.y})`)
        .style("background-color", "#fff");

    const foreignId = `foreign${multiTimeSeriesObj.width + Math.random()}`;
    const foreigG = svg.append("g").attr("transfrom", `translate(${pading.left},${pading.top})`)
    let foreignObj: any = foreigG.append("foreignObject").attr("id", foreignId).attr("x", pading.left).attr("y", pading.top).attr('width', multiTimeSeriesObj.width).attr('height', multiTimeSeriesObj.height);
    const canvas = document.createElement("canvas");
    (canvas as any).__data__ = {}
    document.getElementById(foreignId)?.appendChild(canvas);
    canvas.width = multiTimeSeriesObj.width;
    canvas.height = multiTimeSeriesObj.height;
    let ctx = canvas.getContext("2d");


    //const timeRangeScale = d3.scaleLinear().domain([1493733884409, 1493829248294]).range([0, 2 ** 20 - 1]);
    const indexToTimeStampScale = d3.scaleLinear().domain([nodeIndexRange[0], nodeIndexRange[1]]).range([realTimeStampRange[0], realTimeStampRange[1]]);
    const xScale: any = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.width]);
    let showTimeXScale: any = d3.scaleTime().domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, multiTimeSeriesObj.width]);
    let yScale: any = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height, 0]);

    let xReScale = d3.scaleLinear().domain([0, multiTimeSeriesObj.width]).range([0, multiTimeSeriesObj.dataManagers[0].realDataRowNum - 1]);
    let showXTimeScale: any = d3.scaleTime().domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, multiTimeSeriesObj.width]);


    let zoomAxis = d3.axisBottom(showTimeXScale);
    let yAxis = d3.axisLeft(yScale);
    if (store.state.controlParams.currentMode === 'Default') {
        yAxis = d3.axisLeft(yScale).tickFormat((val) => {
            //@ts-ignore
            return 100 * val + "%"
        })
    }

    let xAxis = d3.axisBottom(showXTimeScale)
    const yReScale = d3.scaleLinear().domain([multiTimeSeriesObj.minv, multiTimeSeriesObj.maxv]).range([multiTimeSeriesObj.height, 0]);
    const timeBrushObj = d3.brushX().extent([[0, 10], [multiTimeSeriesObj.width, 40]]);
    const timeBoxBrushObj = d3.brush().extent([[pading.left, pading.top], [multiTimeSeriesObj.width + pading.left, multiTimeSeriesObj.height + pading.top]]);

    timeBoxBrushObj.on("end", timeBoxBrushed);

    timeBrushObj.on("end", brushed);
    timeBrushObj.on("start", () => {
        console.log("start")
    })
    const timeBoxG = svg.append("g").attr("transform", `translate(${pading.left},${pading.top + 50 + multiTimeSeriesObj.height - 20})`).call(timeBrushObj).call(timeBrushObj.move, [0, multiTimeSeriesObj.width]);
    foreigG.call(timeBoxBrushObj)


    let zoomAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top + 50})`).attr("class", 'x axis').call(zoomAxis)
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
        showTimeXScale.domain([new Date(realTimeStampRange[0]), new Date(realTimeStampRange[1])]).range([0, multiTimeSeriesObj.width]);
        //showTimeXScale.range([0, multiTimeSeriesObj.width]);
        if (zoomAxisG != null) {
            zoomAxisG.remove();
            zoomAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top + 50})`).attr("class", 'x axis').call(zoomAxis)
        }
        timeBrushObj.extent([[0, 10], [multiTimeSeriesObj.width, 40]]);
        timeBrushObj.on("end", brushed);
        timeBrushObj.on("start", () => {
            console.log("start")
        })
        const tempReScale = d3.scaleLinear().domain([0, nodeIndexRange[1]]).range([0, multiTimeSeriesObj.width]);

        timeBoxG.call(timeBrushObj).call(timeBrushObj.move, [tempReScale(multiTimeSeriesObj.timeRange[0]), tempReScale(multiTimeSeriesObj.timeRange[1])]);
        ctx = canvas.getContext("2d");
    }

    let lengendG: any = null;


    function drawLengend(leftOffset: number, multiTimeSeriesObj: MultiTimeSeriesObj, colorArray: Array<string>) {

        if (lengendG !== null) {
            lengendG.remove()
        }
        //lengendG = svg.append('g').attr("width", 100).attr("height", 700).attr("transform", `translate(${leftOffset},${pading.top - 15})`);
        let showNum = 0;
        for (let i = 0; i < multiTimeSeriesObj.dataManagers.length; i++) {
            const dataManager = multiTimeSeriesObj.dataManagers[i];
            if (dataManager.isShow) {
                if (store.state.controlParams.currentMode === 'Default') {
                    let nameStrs = dataManager.dataName.split(".")[1].split("_")
                    if (nameStrs[1][nameStrs[1].length - 1] !== 'r') {
                        nameStrs[1] = nameStrs[1] + 'r';
                    }
                    if (!namedMap.has(nameStrs[1])) {
                        dataManager.isShow = false;
                        continue
                    }
                    // lengendG.append("rect").attr("x", 10).attr("y", showNum * 15).attr('width', 10).attr("height", 10).attr("fill", namedMap.get(nameStrs[1]).split("-")[1]).on("click", () => {
                    //     dataManager.isShow = !dataManager.isShow
                    //     draw();
                    // });

                    // lengendG.append('text').attr("x", 20).attr("y", showNum * 15 + 11).text(nameStrs[1].slice(0, nameStrs[1].lastIndexOf("sktr")).toUpperCase() )//+ namedMap.get(nameStrs[1]).split("-")[0]

                    showNum++;
                } else {
                    let nameStrs = dataManager.dataName.split(".")[1].split("_");
                    let showName = nameStrs[1]
                    for (let i = 2; i < nameStrs.length - 2; i++) {
                        showName = showName + "_" + nameStrs[i];
                    }
                    const showColor = colorArray[dataManager.md5Num! % 46];
                    // lengendG.append("rect").attr("x", 10).attr("y", showNum * 15).attr('width', 10).attr("height", 10).attr("fill", showColor).on("click", () => {
                    //     dataManager.isShow = !dataManager.isShow
                    //     draw();
                    // });

                    // lengendG.append('text').attr("x", 20).attr("y", showNum * 15 + 11).text(showName)

                    showNum++;
                }

            }
        }

    }


    function draw() {
        const colorArray1 = ["#b3de69", "#fdb462", "#80b1d3", "#fb8072", "#bebada", "#ffffb3", "#8dd3c7", "#1f77b4", "#aec7e8", "#ff7f0e", "#ffbb78", "#2ca02c", "#98df8a", "#d62728", "#ff9896", "#9467bd", "#c5b0d5", "#8c564b", "#c49c94", "#e377c2", "#f7b6d2", "#7f7f7f", "#c7c7c7", "#bcbd22", "#dbdb8d", "#17becf", "#9edae5", "#393b79", "#5254a3", "#6b6ecf", "#9c9ede", "#637939", "#8ca252", "#b5cf6b", "#cedb9c", "#8c6d31", "#bd9e39", "#e7ba52", "#843c39", "#ad494a", "#d6616b", "#e7969c", "#7b4173", "#a55194", "#ce6dbd", "#de9ed6"];

        drawLengend(multiTimeSeriesObj.width + pading.left + 10, multiTimeSeriesObj, colorArray1)
        canvas.width = multiTimeSeriesObj.width;

        const curMinMax = computeMinMax(multiTimeSeriesObj);
        yScale = d3.scaleLinear().domain([curMinMax.min, curMinMax.max]).range([multiTimeSeriesObj.height, 0]);
        yAxis = d3.axisLeft(yScale);
        if (store.state.controlParams.currentMode === 'Default') {
            yAxis = d3.axisLeft(yScale).tickFormat((val) => {
                //@ts-ignore
                return 100 * val + "%"
            })
        }
        if (yAxisG !== null && yAxisG !== undefined) {
            yAxisG.remove();
        }
        yAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${pading.top})`).attr("class", 'y axis').call(yAxis);



        showXTimeScale = d3.scaleTime().domain([new Date(Math.floor(indexToTimeStampScale(multiTimeSeriesObj.timeRange[0]))), new Date(Math.floor(indexToTimeStampScale(multiTimeSeriesObj.timeRange[1])))]).range([0, multiTimeSeriesObj.width]);
        xAxis = d3.axisBottom(showXTimeScale);
        if (xAxisG !== null && xAxisG !== undefined) {
            xAxisG.remove();
        }
        xAxisG = svg.append("g").attr('style', 'user-select:none').attr("transform", `translate(${pading.left},${multiTimeSeriesObj.height + pading.top})`).attr("class", 'x axis').call(xAxis)


        const columnInfos = multiTimeSeriesObj.columnInfos;
        ctx?.clearRect(0, 0, multiTimeSeriesObj.width, multiTimeSeriesObj.height);
        for (let i = 0; i < columnInfos.length; i++) {
            if (multiTimeSeriesObj.dataManagers[i].isShow) {
                formatNonPowDataForViewChange(columnInfos[i], multiTimeSeriesObj.width, 2 ** multiTimeSeriesObj.maxLevel, null);
                if (multiTimeSeriesObj.dataManagers[i]) {
                    ctx?.beginPath();

                    if (store.state.controlParams.currentMode === 'Default') {
                        //ctx.strokeStyle = colorArray1[i];
                        const dataManager = multiTimeSeriesObj.dataManagers[i];
                        let nameStrs = dataManager.dataName.split(".")[1].split("_")
                        if (nameStrs[1][nameStrs[1].length - 1] !== 'r') {
                            nameStrs[1] = nameStrs[1] + 'r';
                        }
                        //@ts-ignore
                        ctx.strokeStyle = namedMap.get(nameStrs[1]).split("-")[1]//colorArray1[i];
                    } else {
                        //@ts-ignore
                        ctx.strokeStyle = colorArray1[multiTimeSeriesObj.dataManagers[i].md5Num! % 46];
                    }


                    //@ts-ignore
                    ctx.strokeWidth = 1;
                    const columnInfo = columnInfos[i];
                    for (let i = 0; i < columnInfo.length; i++) {
                        if (columnInfo[i].isMis) {
                            continue
                        }
                        if (columnInfo[i].minVTimeRange[0] < columnInfo[i].maxVTimeRange[0]) {
                            ctx?.moveTo(columnInfo[i].positionInfo.minX, yScale(columnInfo[i].vRange[0]));
                            ctx?.lineTo(columnInfo[i].positionInfo.maxX, yScale(columnInfo[i].vRange[1]));
                        } else {
                            ctx?.moveTo(columnInfo[i].positionInfo.minX, yScale(columnInfo[i].vRange[1]));
                            ctx?.lineTo(columnInfo[i].positionInfo.maxX, yScale(columnInfo[i].vRange[0]));
                        }
                        if (i <= columnInfo.length - 2 && columnInfo[i].endV !== undefined && columnInfo[i + 1] !== undefined) {
                            ctx?.moveTo(columnInfo[i].positionInfo.endX, yScale(columnInfo[i].endV!));
                            ctx?.lineTo(columnInfo[i + 1].positionInfo.startX, yScale(columnInfo[i + 1].startV!));
                        }

                    }
                    const stack = [];
                    for (let i = 0; i < columnInfo.length - 1; i++) {
                        if (!columnInfo[i].isMis && columnInfo[i + 1].isMis) {
                            stack.push(columnInfo[i]);
                            for (let j = i + 1; j < columnInfo.length; j++) {
                                if (columnInfo[j - 1].isMis && !columnInfo[j].isMis) {
                                    const co = stack.pop()
                                    if (columnInfo[j].startV === undefined || co?.endV === undefined) {
                                        console.error("error nonUniform");
                                    }
                                    ctx?.moveTo(co!.positionInfo.endX, yScale(co!.endV));
                                    if (columnInfo[j].startV !== undefined) {
                                        ctx?.lineTo(columnInfo[j].positionInfo.startX, yScale(columnInfo[j].startV!))
                                    } else {
                                        ctx?.lineTo(columnInfo[j].positionInfo.minX, yScale((columnInfo[j].vRange[0] + columnInfo[j].vRange[1]) / 2))
                                    }

                                }
                            }
                        }
                    }
                    ctx?.stroke();
                }

            }
        }

    }

    function resizeW(width: number) {
        isResizing = true;
        //timeboxStack = [];
        const currentLevel = multiTimeSeriesObj.currentLevel;
        // multiTimeSeriesObj.currentLevel = 10;//currentLevel + 1
        multiTimeSeriesObj.width = width;
        updateCanvasWidth();

        if (currentLevel + 1 >= multiTimeSeriesObj.maxLevel - 1) {
            return
        }

        //@ts-ignore
        canvas.style.width = multiTimeSeriesObj.width
        batchGetData(multiTimeSeriesObj.dataManagers, multiTimeSeriesObj.currentLevel, 0, nodeIndexRange[1], multiTimeSeriesObj.maxLevel, width, { inter: "resize", noRet: true }).then(res => {
     
            batchViewChange(multiTimeSeriesObj, { inter: "resize" }).then((allColumnInfos) => {
                multiTimeSeriesObj.pow = false;
                multiTimeSeriesObj.columnInfos = allColumnInfos;



                draw();
            })
        })



    }

    function zoomIn(timeRange: Array<number>) {

        //timeboxStack = [];
        const currentLevel = multiTimeSeriesObj.currentLevel;
        multiTimeSeriesObj.currentLevel = 10;//currentLevel + 1
        const width = multiTimeSeriesObj.width;
        multiTimeSeriesObj.timeRange[1] = timeRange[1]//Math.floor(multiTimeSeriesObj.timeRange[1] / 2);
        multiTimeSeriesObj.timeRange[0] = timeRange[0]
        if (currentLevel + 1 >= multiTimeSeriesObj.maxLevel - 1) {
            return
        }


        const needLoadLevel = 2 ** Math.ceil(Math.log2(width))
        batchGetData(multiTimeSeriesObj.dataManagers, 10, 0, 2 ** 10 - 1, multiTimeSeriesObj.maxLevel, width, { inter: "zoom_in", noRet: true }).then(res => {

            batchViewChange(multiTimeSeriesObj, { inter: "zoom_in" }).then((allColumnInfos) => {
                multiTimeSeriesObj.pow = false;
                multiTimeSeriesObj.columnInfos = allColumnInfos;
                draw();
            })
        });



    }


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
        interInfo.setRangeW(multiTimeSeriesObj.timeRange, multiTimeSeriesObj.width, multiTimeSeriesObj.currentLevel);
        interactionStack.push(interInfo);
        zoomIn([timeRange[0], timeRange[1]])
    }





    let timeboxStack: Array<Array<boolean>> = [];

    //@ts-ignore
    function timeBoxBrushed({ selection }) {
        if (selection === null) {
            return;
        }
        const lastRes = [];
        for (let i = 0; i < multiTimeSeriesObj.dataManagers.length; i++) {
            lastRes.push(multiTimeSeriesObj.dataManagers[i].isShow);
        }
        //if(interactionStack[interactionStack.length-1].type==='timebox')
        const timeBoxStartTime = new Date().getTime();
        const interInfo = new InteractionInfo("timebox");
        interInfo.setShowInfo(lastRes);
        interactionStack.push(interInfo);
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
            let isChoose = multiTimeSeriesObj.dataManagers[i].isShow;
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
                const interInfo = new InteractionInfo("resize")
                interInfo.setRangeW(multiTimeSeriesObj.timeRange, multiTimeSeriesObj.width, multiTimeSeriesObj.currentLevel);
                interactionStack.push(interInfo);
                resizeW(multiTimeSeriesObj.width)
            }
        });
    svg.on("contextmenu", (e) => {

        if (interactionStack.length > 0) {
            const interInfo = interactionStack.pop();

            if (interInfo?.type === 'timebox') {
                const curStats = interInfo.showInfo;
                for (let i = 0; i < multiTimeSeriesObj.dataManagers.length; i++) {
                    multiTimeSeriesObj.dataManagers[i].isShow = curStats![i];
                }
                draw();
            } else if (interInfo?.type === 'resize') {

                resizeW(interInfo.width);
            } else if (interInfo?.type === 'zoom') {
                isRebacking = true;
                zoomIn(interInfo.timeRange)
            }
        }
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

function computeMinMax(multiTimeSeriesObj: MultiTimeSeriesObj) {
    let min = Infinity;
    let max = -Infinity;
    for (let i = 0; i < multiTimeSeriesObj.columnInfos.length; i++) {
        if (multiTimeSeriesObj.dataManagers[i].isShow) {
            const columInfo = multiTimeSeriesObj.columnInfos[i];
            for (let j = 0; j < columInfo.length; j++) {
                if (columInfo[j].vRange[0] < min) {
                    min = columInfo[j].vRange[0];
                }

                if (columInfo[j].vRange[1] > max) {
                    max = columInfo[j].vRange[1];
                }

            }
        } else {
            // console.log(multiTimeSeriesObj.dataManagers[i].dataName)
        }
    }
    return {
        min,
        max
    }
}
