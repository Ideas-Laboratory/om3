//@ts-nocheck
function seriesDensity(xBins, yBins) {
    let x0 = d => d.x0 || 0,
      ys = d => d,
      xDomain,
      yDomain,
      arcLengthNormalize = true,
      batchSize = null;
    let ret = data => {
      if (!Number.isInteger(xBins))
        throw new Error(`xBins must be an integer (got ${xBins})`);
      if (!Number.isInteger(yBins))
        throw new Error(`yBins must be an integer (got ${yBins})`);
      if (!xBins || !yBins)
        throw new Error(
          "computing density requires nonzero values for both xBins and yBins."
        );
      return (typeof batchSize == 'number'
        ? renderSeriesGenerator
        : renderSeries)(data, ret.options(data));
    };
    ret.options = data => {
      return {
        xBins,
        yBins,
        x0,
        ys,
        xDomain: xDomain || ret.defaultXDomain(data),
        yDomain: yDomain || ret.defaultYDomain(data),
        arcLengthNormalize,
        batchSize
      };
    };
    ret.xBins = function(_) {
      return arguments.length ? ((xBins = _), ret) : xBins;
    };
    ret.yBins = function(_) {
      return arguments.length ? ((yBins = _), ret) : yBins;
    };
    ret.x0 = function(_) {
      return arguments.length ? ((x0 = _), ret) : x0;
    };
    ret.ys = function(_) {
      return arguments.length ? ((ys = _), ret) : ys;
    };
    ret.xDomain = function(_) {
      return arguments.length ? ((xDomain = _), ret) : xDomain;
    };
    ret.yDomain = function(_) {
      return arguments.length ? ((yDomain = _), ret) : yDomain;
    };
    ret.arcLengthNormalize = function(_) {
      return arguments.length
        ? ((arcLengthNormalize = _), ret)
        : arcLengthNormalize;
    };
    ret.batchSize = function(_) {
      return arguments.length ? ((batchSize = _), ret) : batchSize;
    };
    // optimization opportunity: compute the extents in one pass
    ret.defaultXDomain = data => [
      d3.min(data, x0),
      d3.max(data, (series, i, a) => x0(series, i, a) + ys(series, i, a).length) -
        1
    ];
    ret.defaultYDomain = data => [
      d3.min(data, series => d3.min(ys(series))),
      d3.max(data, series => d3.max(ys(series)))
    ];
    ret.copy = function(_) {
      return seriesDensity(xBins, yBins)
        .x0(x0)
        .ys(ys)
        .xDomain(xDomain)
        .yDomain(yDomain)
        .arcLengthNormalize(arcLengthNormalize)
        .batchSize(batchSize);
    };
    return ret;
  }