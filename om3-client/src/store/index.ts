import { createStore } from 'vuex';
import * as mutations from './mutations';
import * as actions from './actions';
import mitt from 'mitt';
import { WaveletTree } from '@/helper/tree';
import TrendTree from '@/helper/tend-query-tree';
import LevelDataManager from '@/model/level-data-manager';
import { NoUniformColObj } from '@/model/non-uniform-col-obj';
import HaarDataManager from '@/model/haar-data-manager';
//import WebSocket from 'ws'



let allTimeStore: Array<Array<number>> = [];
export function pushTimeArray(curTimeArray: Array<number>) {
  const newA = curTimeArray.map(v => v)
  allTimeStore.push(newA);
}

export function getAvgTime() {
  const avgTimeArray = new Array(allTimeStore[0].length);
  // debugger
  for (let j = 0; j < allTimeStore[0].length; j++) {
    let curMin = Infinity;
    let curMax = -Infinity
    let sum = 0;
    for (let i = 0; i < allTimeStore.length; i++) {
      const curV = allTimeStore[i][j];
      sum += curV;

    }
    avgTimeArray[j] = (Math.floor(sum / (allTimeStore.length)))
  }
  allTimeStore = [];
  return avgTimeArray;
}

export function getMultAvgTime() {
  const avgTimeArray = [];
  for (let i = 0; i < allTimeStore.length; i++) {
    let sum = 0
    for (let j = 0; j < allTimeStore[0].length; j++) {
      sum += allTimeStore[i][j]
    }
    avgTimeArray.push(Math.floor(sum / allTimeStore[0].length))
  }
  return avgTimeArray
}

export const emitter = mitt();
export const ws = new WebSocket('ws://127.0.0.1:3001')
ws.onopen = e => {
  console.log("connect success", e)
}



export interface LineChartObj {
  id: string;
  width: number;
  height: number;
  x: number;
  y: number;
  data: { val: Array<any>, min: number, max: number };
  timeRange: [Date, Date];
  algorithm: string;
  isChoosed: boolean;
}

export interface SimpleLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  data: { val: Array<any>, max: number, min: number };
  timeRange: [number, number];
  algorithm: string;
  isChoosed: boolean;
  isSample: boolean
}

export interface SimpleBrushChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  data: { rowData: Array<Array<number>>, val: Array<any>, max: number, min: number };
  timeRange: [number, number];
  algorithm: string;
  isChoosed: boolean;
  isSample: boolean
}

export interface WaveletLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  //data: { val: Array<{x:number,y:number}>, max: number, min: number };
  timeRange: [number, number];
  algorithm: string;
  isChoosed: boolean;
  isSample: boolean;
  tag: string;
  root: WaveletTree;
  distortion: Array<number>;
  denoiseMethod?: string;
  denoiseThreshold?: number;
}

export interface TrendQueryLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  root: TrendTree;
  data: { renderData: Array<any>, maxv: number, minv: number };
  params: [number, number];
  dataManager: LevelDataManager;
  historyQueryStack: Array<Array<number>>;
  currentLevel: number;
}
export interface AngularQueryLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  root: TrendTree;
  data: { renderData: Array<any>, maxv: number, minv: number };
  params: [number, number];
  dataManager: LevelDataManager;
  historyQueryStack: Array<Array<number>>;
  currentLevel: number;
}
export interface TimeBoxQueryLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  data: Array<{ renderData: Array<any>, maxv: number, minv: number }>;
  params: [number, number];
  dataManagers: Array<LevelDataManager>;
  historyQueryStack: Array<Array<number>>;
  currentLevel: number;
}

export interface MultiTimeSeriesObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  powRenderData: Array<{ renderData: Array<any>, maxv: number, minv: number }>;
  params: [number, number];
  dataManagers: Array<LevelDataManager>;
  currentLevel: number;
  pow: boolean,
  columnInfos: Array<Array<NoUniformColObj>>,
  minv: number,
  maxv: number,
  maxLevel: number,
  className: string,
  lineAmount: number,
  startTimeStamp: number,
  endTimeStamp: number,
  timeIntervalMs: number,
}

export interface MultiHaarTimeSeriesObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  allRenderData: Array<{ renderData: Array<any>, maxv: number, minv: number }>;
  params: [number, number];
  dataManagers: Array<HaarDataManager>;
  currentLevel: number;
  minv: number,
  maxv: number,
  maxLevel: number
}
export interface MultiTimeSeriesM4Obj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  params: [number, number];
  renderData: Array<{ tn: string, data: Array<{ t: number, x: number, y: number, v: number }> }>
  maxLevel: number,
  minv: number,
  maxv: number,

}

export interface ValueFilterLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  root: TrendTree;
  data: { renderData: Array<any>, maxv: number, minv: number };
  params: [number, number];
  dataManager: LevelDataManager;
  historyQueryStack: Array<Array<number>>;
  currentLevel: number;
}
export interface TimeFilterLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  root: TrendTree;
  data: { renderData: Array<any>, maxv: number, minv: number };
  params: [number, number];
  dataManager: LevelDataManager;
  historyQueryStack: Array<Array<number>>;
  currentLevel: number;
}

export interface ViewChangeLineChartObj {
  id: string
  width: number;
  height: number;
  x: number;
  y: number;
  timeRange: [number, number];
  algorithm: string;
  root: TrendTree;
  data: { powRenderData: Array<any>, noPowRenderData: Array<any>, maxv: number, minv: number };
  params: [number, number];
  dataManager: LevelDataManager;
  historyQueryStack?: Array<Array<number>>;
  currentLevel: number;
  nonUniformColObjs?: Array<NoUniformColObj>
  isPow: boolean,
  maxLen:number,
  startTime:number,
  endTime:number,
}


export interface GlobalState {
  currentAlgorithm: string;
  rdpThreshold: number;
  allAlgoritem: Array<string>;
  allTables: Array<string>;
  allCustomTables: Array<string>,
  customTableMap: Map<string, any>,
  allDefaultTables: Array<string>,
  defaultTableMap: Map<string, any>
  allFlags: any;
  tableMaxLevels: any;
  lineChartObjs: Map<string, LineChartObj>;
  simpleLineChartObjs: Map<string, SimpleLineChartObj>;
  waveletLineChartObjs: Map<string, WaveletLineChartObj>;
  simpleBrushLineChartObjs: Map<string, SimpleBrushChartObj>;
  allMultiLineClassInfoMap: Map<string, any>;
  allCustomMultiLineClassInfoMap: Map<string, any>;
  controlParams: {
    currentMode: 'Default' | 'Custom';
    currentLineType: 'Single' | 'Multi'
    progressive:boolean;
    displayChanel: Array<string>;
    currentChanel: string;
    currentMultiLineClass: string;
    currentTimeBoxType: string;
    isDenoise: boolean;
    currentTable: string;
    currentCustomTable: string;
    tableMaxLevel: number;
    sampleMethods: Array<string>;
    currentSampleMethod: string;
    denoiseMethods: Array<string>;
    currentDenoiseMethod: string;
    denoiseThreshold: number;
    realStart: number;
    semanticInterval: number;
    semanticType: 'ms' | 's' | 'm' | 'h' | 'd' | 'W' | 'M' | 'Y'
  },
}

export default createStore<GlobalState>({
  state: {
    rdpThreshold: 5,
    currentAlgorithm: 'ViewChangeQuery',
    allAlgoritem: ['M4', 'MinMax', 'Min', 'Max', 'RDP', 'Avg'],
    allTables: [],
    allCustomTables: [],
    customTableMap: new Map(),
    allDefaultTables: [],
    defaultTableMap: new Map(),
    allFlags: {},
    allMultiLineClassInfoMap: new Map(),
    allCustomMultiLineClassInfoMap: new Map(),
    tableMaxLevels: { '1m': 20, '2m': 21, '4m': 22, '8m': 23, '1k': 24, '16m': 24, '32m': 25, '64m': 26, '1b': 27, '3b': 28, '10b': 30, 'sensor8': 23, 'test': 4 },
    lineChartObjs: new Map(),
    simpleLineChartObjs: new Map(),
    waveletLineChartObjs: new Map(),
    simpleBrushLineChartObjs: new Map(),
    controlParams: {
      progressive:false,
      currentMode: 'Default',
      currentLineType: 'Single',
      currentTable: '',
      currentCustomTable: "",
      currentTimeBoxType: 'stock46r',
      currentMultiLineClass: '',
      tableMaxLevel: 20,
      sampleMethods: ['ViewChangeQueryFinal'],
      displayChanel: ['All', 'MinT', 'MinV', 'MaxV', 'MaxT'],
      currentChanel: 'All',
      isDenoise: false,
      denoiseMethods: ['simple_hard', 'm4_hard', 'm4_hard_v2', 'm4_threshold_with_level', 'm4_wavelet_hard', 'soft'],
      currentDenoiseMethod: 'm4_hard',
      currentSampleMethod: 'ViewChangeQueryFinal',
      denoiseThreshold: 10,
      realStart: 1354114150080,
      semanticInterval: 6000,
      semanticType: 'h'
    }
  },
  mutations,
  actions,
  modules: {
  }
})
