<template>
  <!-- <div id="nav">
    <router-link to="/">Home</router-link> |
    <router-link to="/about">About</router-link>
  </div> -->
  <div class="d-flex flex-column h-100">
    <global-header></global-header>
    <div class="gloal-content flex-grow-1 d-flex flex-row mx-2">
      <left-side-data-control-panel></left-side-data-control-panel>
      <dispaly-content-panel></dispaly-content-panel>
    </div>
  </div>
</template>

<script lang="ts">
import { defineComponent } from "vue";
import GlobalHeader from "./components/GlobalHeader.vue";
import LeftSideDataControlPanel from "./components/LeftSideDataControlPanel.vue";
import DispalyContentPanel from "./components/DisplayContentPanel.vue";
import "bootstrap/dist/css/bootstrap.min.css";
import store from "./store";

export default defineComponent({
  components: { GlobalHeader, LeftSideDataControlPanel, DispalyContentPanel },
  setup(){
    store.dispatch("loadDefaultTableAndInfo");
    store.dispatch("getAllFlags");
    
    
  }
});
</script>




