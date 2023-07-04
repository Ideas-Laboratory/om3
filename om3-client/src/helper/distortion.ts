//@ts-nocheck
import * as d3 from "d3";

export function AxisDistortion(d) {
  const scale = d3.scaleLinear();
  let cacheSumDistortion = null;
  let cacheDistortion = null;

  function fisheye(_) {
    const cols = d.length;
    let sumDistortion;
    if (cacheDistortion === d) {
      sumDistortion = cacheSumDistortion;
    } else {
      let [pointer, sumD] = d.reduce(
        ([p, s], v) => [p + v, s.concat([p + v])],
        [0, [0]]
      );
      const ratio = cols / pointer;
      sumD = sumD.map((d) => d * ratio);
      cacheSumDistortion = sumD;
      cacheDistortion = d;
      sumDistortion = sumD;
    }
    let x = scale(_);
    let startIndex = sumDistortion.findIndex(
      (d, i) => d <= x && (i >= cols - 1 || sumDistortion[i + 1] > x)
    );
    if (startIndex >= cols - 1) return cols;
    if (startIndex < 0) return 0;
    return (
      startIndex +
      (x - sumDistortion[startIndex]) /
        (sumDistortion[startIndex + 1] - sumDistortion[startIndex])
    );
  }

  fisheye.changeDistortion = function (_) {
    d = _;
    return fisheye;
  };

  fisheye.copy = function () {
    return AxisDistortion(JSON.parse(JSON.stringify(d)));
  };

  fisheye.domain = function (_) {
    if (!_) return scale.domain();
    scale.domain(_);
    return fisheye;
  };

  fisheye.range = function (_) {
    if (!_) return scale.range();
    scale.range(_);
    return fisheye;
  };

  fisheye.nice = scale.nice;
  fisheye.ticks = scale.ticks;
  fisheye.tickFormat = scale.tickFormat;

  return fisheye;
}

export function FisheyeDistortion(scale, d, a) {
  function fisheye(_) {
    let x = scale(_),
      left = x < a,
      range = d3.extent(scale.range()),
      min = range[0],
      max = range[1],
      m = left ? a - min : max - a;
    if (m == 0) m = max - min;
    return ((left ? -1 : 1) * m * (d + 1)) / (d + m / Math.abs(x - a)) + a;
  }

  fisheye.distortion = function (_) {
    if (!arguments.length) return d;
    d = +_;
    return fisheye;
  };

  fisheye.focus = function (_) {
    if (!arguments.length) return a;
    a = +_;
    return fisheye;
  };

  fisheye.copy = function () {
    return FisheyeDistortion(scale.copy(), d, a);
  };

  fisheye.domain = function (_) {
    if (!_) return scale.domain();
    scale.domain(_);
    return fisheye;
  };

  fisheye.range = function (_) {
    if (!_) return scale.range();
    scale.range(_);
    return fisheye;
  };

  fisheye.nice = scale.nice;
  fisheye.ticks = scale.ticks;
  fisheye.tickFormat = scale.tickFormat;
  return fisheye;
}
