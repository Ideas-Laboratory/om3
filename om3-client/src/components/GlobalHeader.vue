<template>
  <nav class="navbar navbar-light bg-light">
    <div class="container-fluid">
      <a class="navbar-brand" href="#">
       
       OM<sup>3</sup>
      </a>

      <a href="https://docs.google.com/document/d/1OFRmg6ShyKj1i4w2zbHGIOyjUxGR7tcvreKuWCIerHk/edit?usp=sharing">user guide</a>
    
      <!-- <button type="button" class="btn btn-secondary" @click.prevent="openImportPanel">ImportData</button> -->
    </div>
   
  </nav>
</template>
<script>
import { defineComponent, ref } from "vue";
export default defineComponent({
  name: "GlobalHeader",
  components: {},
  props: {},
  setup() {
    const isVisible = ref(false);
    const closePanel = () => {
      isVisible.value = !isVisible.value;
    };
    const openImportPanel=()=>{
      isVisible.value=true
    }
    return {
      isVisible,
      closePanel,
      openImportPanel
    };
  },
});
</script>
<style scoped>
.navbar{
  min-width: 1520px;
}
</style>