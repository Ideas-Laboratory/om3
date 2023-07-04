//@ts-nocheck
import { WaveletTree } from "./tree";
import * as d3 from "d3";
import "../styles/index.scss";
import Axios from "axios";
import { AxisDistortion, FisheyeDistortion } from "./distortion";

if (process.env.NODE_ENV === "development") {
  require("../index.html");
}
/**
 * @type {HTMLCanvasElement}
 */
const canvas = document.getElementById("lines");
const context = canvas.getContext("2d");
const colormaps = [
  "#1f77b4",
  "#ff7f0e",
  "#2ca02c",
  "#d62728",
  "#9467bd",
  "#8c564b",
  "#e377c2",
  "#7f7f7f",
  "#bcbd22",
  "#17becf",
];
context.imageSmoothingEnabled = false;
let renderData = [];

// to draw aliasing line, debug use
function bresenhamLine(x, y, xx, yy) {
  var oldFill = context.fillStyle; // save old fill style
  context.fillStyle = context.strokeStyle; // move stroke style to fill
  xx = Math.floor(xx);
  yy = Math.floor(yy);
  x = Math.floor(x);
  y = Math.floor(y);
  // BRENSENHAM
  var dx = Math.abs(xx - x);
  var sx = x < xx ? 1 : -1;
  var dy = -Math.abs(yy - y);
  var sy = y < yy ? 1 : -1;
  var err = dx + dy;
  var errC; // error value
  var end = false;
  var x1 = x;
  var y1 = y;

  while (!end) {
    context.fillRect(x1, y1, 1, 1); // draw each pixel as a rect
    if (x1 === xx && y1 === yy) {
      end = true;
    } else {
      errC = 2 * err;
      if (errC >= dy) {
        err += dy;
        x1 += sx;
      }
      if (errC <= dx) {
        err += dx;
        y1 += sy;
      }
    }
  }
  context.fillStyle = oldFill; // restore old fill style
}

/**
 * @param {d3.ScaleLinear<number, number>} xScale
 * @param {d3.ScaleLinear<number, number>} yScale
 */
function drawCanvas(xScale, yScale) {
  const [, cols] = d3.extent(xScale.range());
  canvas.width = cols;
  canvas.style.width = cols + "px";
  context.imageSmoothingEnabled = false;
  context.clearRect(0, 0, cols, 500);
  if (!renderData || !renderData.length) {
    return;
  }
  console.log(renderData);
  for (let i in renderData) {
    const renderContent = renderData[i];

    context.globalAlpha = 1.0;
    context.strokeStyle = colormaps[i % colormaps.length];
    context.lineWidth = 1.0;

    // context.moveTo(xScale(renderData[0].x), yScale(renderData[0].y));
    // for (let point of renderData) {
    //   context.lineTo(xScale(point.x), yScale(point.y));
    // }
    // context.stroke();

    for (let i in renderContent) {
      if (i < renderContent.length - 1) {
        bresenhamLine(
          xScale(renderContent[i].x),
          yScale(renderContent[i].y),
          xScale(renderContent[~~i + 1].x),
          yScale(renderContent[~~i + 1].y)
        );
      }
    }
  }

  requestAnimationFrame(drawCanvas.bind(this, xScale, yScale));
}

function renderer(distortion, roots) {
  d3.select("#container svg").remove();
  roots.forEach((root) => root.clearListeners(true));
  const cols = distortion.length;
  let fisheye = FisheyeDistortion(d3.scaleLinear(), 0, 0)
    .domain([0, cols])
    .range([0, cols]);
  let xScale = AxisDistortion(distortion).domain([0, cols]).range([0, cols]),
    yScale = d3
      .scaleLinear()
      .domain([
        roots.reduce((p, v) => Math.min(p, v.yArray[2]), Infinity),
        roots.reduce((p, v) => Math.max(p, v.yArray[3]), -Infinity),
      ])
      .range([500, 0]);
  let xAxis = d3
    .axisBottom()
    .scale(xScale)
    .tickFormat((v) => Math.round((v / cols) * 32768));
  let xGrid = d3.axisBottom().scale(xScale).tickSize(-500).tickFormat("");
  let yAxis = d3.axisLeft().scale(yScale);
  let distortionCache = distortion;
  let cacheData = [];

  let svg = d3
    .select("#container")
    .append("svg")
    .attr("width", cols + 100)
    .attr("height", 540)
    .append("g")
    .attr("transform", "translate(40, 10)");
  // Add a background rect for mousemove.

  svg
    .append("rect")
    .attr("class", "background")
    .attr("width", cols)
    .attr("height", 500);

  // Add the x-axis.
  svg
    .append("g")
    .attr("class", "x grid")
    .attr("transform", "translate(0, 500)")
    .call(xGrid);
  svg
    .append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0, 500)")
    .call(xAxis);

  // Add the y-axis.
  svg.append("g").attr("class", "y axis").call(yAxis);

  const rerender = () => {
    let [pointer, , mergedDistortion] = distortion.reduce(
      ([pointer, previous, d], v) => {
        if (v === previous) {
          const arr = d.pop();
          arr[1] += v;
          d.push(arr);
          return [pointer + v, v, d];
        }
        d.push([pointer, pointer + v, v, null]);
        return [pointer + v, v, d];
      },
      [0, null, []]
    );
    mergedDistortion = mergedDistortion.map((d) =>
      d.map((dd, i) => (i > 2 ? dd : (dd / pointer) * cols))
    );
    roots.forEach((root) => root.clearListeners(true));
    xScale.changeDistortion(distortion);
    svg.select(".x.axis").call(xAxis);
    svg.select(".x.grid").call(xGrid);
    roots.forEach((root, i) => {
      if (cacheData[i]) {
        renderData[i] = cacheData[i];
      }
      mergedDistortion.forEach((distortionSlice) => {
        root.getDetailData(
          distortionSlice.slice(0, 2),
          distortionSlice[2],
          (partialData) => {
            distortionSlice[3] = partialData;
            if (!mergedDistortion.filter((m) => !m[3]).length) {
              const data = mergedDistortion
                .reduce((p, v) => p.concat(v[3]), [])
                .sort((a, b) => a.x - b.x);
              cacheData[i] = data;
              renderData[i] = data;
            }
          }
        );
      });
    });
  };

  let zoomDetail = 0;

  const calDistortion = (pointerX, refresh = false) => {
    if (refresh) {
      fisheye.focus(cols / 2);
      let pointer = 0;
      distortionCache = distortion.map((_, i) => {
        const fishRes = fisheye(i + 1);
        // const result = Math.pow(
        //   2,
        //   Math.round(Math.log2(1 / (fishRes - pointer)))
        // );
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
    distortion = distortionCache.slice(
      Math.max(0, Math.round(cols / 2 - pointerX)),
      Math.round((cols * 3) / 2 - pointerX)
    );
    if (pointerX < cols / 2) {
      distortion = distortion.concat(
        new Array(cols - distortion.length).fill(distortionEdgeValue)
      );
    } else {
      distortion = new Array(cols - distortion.length)
        .fill(distortionEdgeValue)
        .concat(distortion);
    }
  };

  const zoomIn = (pointerX) => {
    fisheye.distortion(++zoomDetail);
    calDistortion(pointerX, true);
  };

  const zoomOut = (pointerX) => {
    if (zoomDetail === 0) return;
    fisheye.distortion(--zoomDetail);
    calDistortion(pointerX, true);
  };

  svg.on("mousemove", function (e) {
    const pointerX = e.offsetX - 40;
    if (zoomDetail !== 0) {
      calDistortion(pointerX);
      rerender();
    }
  });

  svg.on("mousedown", function (e) {
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
    rerender();
  });

  svg.on("contextmenu", (e) => {
    e.preventDefault();
    e.stopPropagation();
  });

  svg.on("wheel", function (e) {
    const pointerX = e.offsetX - 40;
    if (e.deltaY > 0) {
      zoomIn(pointerX);
    } else {
      zoomOut(pointerX);
    }
    rerender();
  });

  rerender();

  requestAnimationFrame(drawCanvas.bind(null, xScale, yScale));

  return svg;
}

Promise.all(
  Array(4)
    .fill(0)
    .map((_, i) =>
      Axios.get(
        `http://101.200.56.16:3000/initwaveletinfo?database=${i}&width=1`
      )
    )
).then((result) => {
  if (result.length) {
    let roots = null;
    //#region default
    //*
    document.getElementById("submit").addEventListener("click", () => {
      let cols = document.getElementById("cols").value;
      if (!cols) return;
      cols = parseInt(cols);
      if (roots) {
        roots.forEach((root) => root.clearListeners(true));
      }
      roots = result.map((_, i) => new WaveletTree(null, true, i));
      roots.forEach((root, i) => {
        root.yArray = [0, 3, 1, 2].map((j) => result[i].data[j][0]);
        root.width = cols;
        root.x = cols / 2;
      });
      let distortion = new Array(cols).fill(1);
      renderer(distortion, roots);
      console.log(roots);
    });
    //*/
    //#endregion
    //#region wavelet auto-gen
    /*
      let cols = parseInt(window.location.search.slice(5));
      if (root) {
        root.clearListeners(true);
      }
      root = new WaveletTree();
      root.yArray = [0, 3, 1, 2].map((i) => data[i][0]);
      root.width = cols;
      root.x = cols / 2;
      let distortion = new Array(cols).fill(1);
      renderer(distortion, root);
      console.log(root);
      await new Promise((resolve) => {
        setTimeout(resolve, 1000);
      });
      const image = canvas
        .toDataURL("image/png")
        .replace("image/png", "image/octet-stream"); // here is the most important part because if you dont replace you will get a DOM 18 exception.
      const a = document.createElement("a");
      a.download = `b${cols}.png`;
      a.href = image;
      a.click();
      await new Promise((resolve) => {
        setTimeout(resolve, 100);
      });
      if (cols < 1024) {
        window.location.search = `?col=${cols + 1}`;
      }
      //*/
    //#endregion
    //#region raw auto-gen
    /*
      Axios.get("http://47.106.84.47:3000/detail?tStart=0&tEnd=99999").then(
        async ({ data }) => {
          for (let i = 512; i <= 1024; i++) {
            let cols = i;
            renderData = data.slice(0, 32768).map(({ t, v }) => ({
              x: (cols / 32768) * (t + 1) - 1 / 32768 / 2,
              y: v,
            }));
            console.log(data, renderData);
            drawCanvas(
              d3.scaleLinear().domain([0, cols]).range([0, cols]),
              d3.scaleLinear().domain([0, 310]).range([500, 0])
            );
            await new Promise((resolve) => {
              setTimeout(resolve, 100);
            });
            const image = canvas
              .toDataURL("image/png")
              .replace("image/png", "image/octet-stream"); // here is the most important part because if you dont replace you will get a DOM 18 exception.
            const a = document.createElement("a");
            a.download = `a${i}.png`;
            a.href = image;
            a.click();
            await new Promise((resolve) => {
              setTimeout(resolve, 100);
            });
          }
        }
      );
      //*/
    //#endregion
  }
});
