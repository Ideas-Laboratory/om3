<template>
  <div class="device-control-panel">
    <div class="d-flex justify-content-center">
      <div class="btn-group">
        <ul class="device-dimensions-menu dropdown-menu">
          <li><a class="dropdown-item" href="#">Action</a></li>
          <li><a class="dropdown-item" href="#">Another action</a></li>
          <li><a class="dropdown-item" href="#">Something else here</a></li>
          <li>
            <hr class="dropdown-divider" />
          </li>
          <li><a class="dropdown-item" href="#">Separated link</a></li>
        </ul>
      </div>
      <i class="bi bi-arrow-clockwise"></i>
      <div class="d-flex ms-2">
        <input type="number" class="form-control form-control-sm dim-input" v-model="widthRef" />
        <span style="line-height: 31px">×</span>
        <input type="number" class="form-control form-control-sm dim-input" v-model="heightRef" />
        <button type="button" class="btn btn-sm btn-secondary ms-2">
          <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-arrow-clockwise"
            viewBox="0 0 16 16">
            <path fill-rule="evenodd" d="M8 3a5 5 0 1 0 4.546 2.914.5.5 0 0 1 .908-.417A6 6 0 1 1 8 2v1z" />
            <path
              d="M8 4.466V.534a.25.25 0 0 1 .41-.192l2.36 1.966c.12.1.12.284 0 .384L8.41 4.658A.25.25 0 0 1 8 4.466z" />
          </svg>
        </button>
      </div>
      <div class="ms-2" v-if="false">
        <el-date-picker v-model="dateRangeRef" type="datetimerange" range-separator="To"
          :start-placeholder="'2019-07-13 00:00:00'" :end-placeholder="'2020-04-29 00:00:00'">
        </el-date-picker>
      </div>
      <button id="create_panel_btn" type="button" class="btn btn-secondary ms-2" @click.prevent="handleCreatePanel">
        create
      </button>
    </div>
  </div>
</template>
<script>
import store from "@/store";
import { computed, defineComponent, ref } from "vue";
import * as dateFormat from "date-format";

export default defineComponent({
  setup() {
    const dateRangeRef = ref([
      new Date("2019-07-11 00:00:00"),
      new Date("2020-05-19 00:00:00"),
    ]);
    const widthRef = ref(600);
    const heightRef = ref(600);
    const currentDB = computed(() => {
      return store.state.controlParams.currentDB;
    });

    const handleCreatePanel = () => {
      const maxLevel = store.state.controlParams.tableMaxLevel;

      if (
        store.state.controlParams.currentLineType==='Single'&&
        store.state.controlParams.currentSampleMethod === "ViewChangeQueryFinal"
      ) {
        const payload = {
          startTime: 0,
          endTime: 2 ** maxLevel - 1,
          width: widthRef.value,
          height: heightRef.value,
          name: "load_shape_search",
        };
        store.dispatch("loadViewChangeQueryWSMinMaxMissDataInitData", payload);
        return;
      }else if(store.state.controlParams.currentLineType==='Multi'){
        const payload = {
          startTime: 0,
          endTime: 2 ** maxLevel - 1,
          width: widthRef.value,
          height: heightRef.value,
          name: "load_shape_search",
        };
        store.dispatch("loadMultiTimeSeriesInitData", payload);
        return;
      }else{
        console.log("multi wavle");
          const payload = {
            width: widthRef.value,
            height: heightRef.value,
            type: store.state.controlParams.currentTimeBoxType,
          };
          store.dispatch("loadMultiTimeSeriesInitData", payload);
      }



    };

    return {
      dateRangeRef,
      handleCreatePanel,
      widthRef,
      heightRef,
      currentDB,
    };
  },
});
</script>
<style scoped>
.device-control-panel {
  min-height: 25px;
  background-color: #fff;
}

.device-dimensions-menu-show {
  position: absolute;
  inset: 0px auto auto 0px;
  margin: 0px;
  transform: translate(0px, 40px);
}

.dim-input {
  max-width: 4rem;
}
</style>