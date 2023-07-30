<template>
  <div class="data-control-panel radius">
    <div class="choose_container choose_mode_container ms-1">
      <el-radio-group v-model="chooseMode" size="medium" @change="handleModeChange">
        <el-radio-button label="Default"></el-radio-button>
        <el-radio-button label="Custom"></el-radio-button>
      </el-radio-group>
    </div>

    <div class="dbsetting-control-container mt-2" v-if="chooseMode === 'Custom'">
      <div>
        <el-switch v-model="isOpenDbSetup" name="DB Setup" />
        <label>{{ "DB Setup" }}</label>
      </div>
      <div class="mt-2 " v-if="isOpenDbSetup">
        <el-input v-model="customDBHostName" type="string" placeholder="DB Host Name" />
        <el-input v-model="customUserName" type="string" placeholder="User Name" />
        <el-input v-model="customDBPassword" type="password" placeholder="DB Password" />
        <el-input v-model="customDBName" type="string" placeholder="DB Name" />
        <el-input v-model="customDBPort" type="number" placeholder="port" />
        <div><el-button class="btn mt-2" v-on:click="handleTestConn" type="primary">{{ testConnectResult }}</el-button>
        </div>
        <div><el-button class="btn mt-2 " v-on:click="createCustomDBConn" type="primary">{{ createDBConn }}</el-button>
        </div>
        <div><el-button class="btn mt-2" v-on:click="initOM3DBEnv" type="primary">{{ initOm3DB }}</el-button></div>
        <div>
          <el-popconfirm title="This will clear all om3 table, are you sure you want to do this?"
            @confirm="clearOM3DBEnv">
            <template #reference>
              <el-button class="btn mt-2" type="primary">{{ clearOm3DB }}</el-button>
            </template>
          </el-popconfirm>
        </div>
        <div><el-button class="btn mt-2" v-on:click="clearIndexFlag" type="primary">Clear Flag</el-button></div>
      </div>
    </div>

    <div class="om3-transform-control-container mt-2" v-if="chooseMode === 'Custom'">
      <div>
        <el-switch v-model="isOpenTransform" name="OM3 Transform" />
        <label>{{ "Data Transform" }}</label>
      </div>
      <div class="mt-2 " v-if="isOpenTransform">

        <div><el-button class="btn mt-2" text @click="dialogFormVisible = true" type="primary"
            v-if="chooseLineType === 'Single'">Choose Data</el-button>
        </div>
        <el-dialog v-model="dialogFormVisible" title="Single Line Transform">
          <label>Please choose a table,which inclues two columns(t interger,v double precision)(t should be a positive
            integer starting from zero)</label>
          <el-form>
            <el-form-item label="Table">
              <el-select v-model="singleLineTableName" placeholder="Please select a table for transform">
                <el-option v-for="(item, idx) in allCustomDBTables" :key="idx" :label="item" :value="item"></el-option>
              </el-select>
            </el-form-item>
            <label>Please select a date and time, which will be linearly mapped to t during visualization </label>
            <el-form-item label="Date Range">
              <el-date-picker v-model="lineDateRange" type="daterange" start-placeholder="Start Date"
                end-placeholder="End Date" :default-value="[new Date(2010, 9, 1), new Date(2010, 10, 1)]" />
            </el-form-item>
            <el-form-item label="Time Range">
              <div class="example-basic">
                <el-time-picker v-model="lineStartTime" arrow-control placeholder="Start Time" />
                <el-time-picker v-model="lineEndTime" arrow-control placeholder="End Time" />
              </div>
            </el-form-item>
          </el-form>
          <template #footer>
            <span class="dialog-footer">
              <el-button @click="dialogFormVisible = false">Cancel</el-button>
              <el-button type="primary" v-loading.fullscreen.lock="fullscreenLoading"
                v-on:click="performTransformForSingeLine">
                Confirm
              </el-button>
            </span>
          </template>
        </el-dialog>


        <div><el-button class="btn mt-2" text @click="multiLineTransformDialogVisible = true" type="primary"
            v-if="chooseLineType === 'Multi'">Choose Data</el-button>
        </div>
        <el-dialog v-model="multiLineTransformDialogVisible" title="Multi Line Transform">

          <label>Please choose tables,which inclues two columns(t interger,v double precision)(t should be a positive
            integer starting from zero). All the data you choose should be the same length</label>
          <el-form>
            <el-form-item label="Table">
              <el-select class="multi-line-select" v-model="multiLineTableNames" multiple
                placeholder="Please select table for transform">
                <el-option v-for="(item, idx) in allCustomDBTables" :key="idx" :label="item" :value="item"></el-option>
              </el-select>
            </el-form-item>
            <el-input class="mb-4" v-model="customMultiLineClassName"
              placeholder="Please define a group for the lines you select" clearable />
            <label>Please select a date and time, which will be linearly mapped to t during visualization </label>
            <el-form-item label="Date Range">
              <el-date-picker v-model="lineDateRange" type="daterange" start-placeholder="Start Date"
                end-placeholder="End Date" :default-value="[new Date(2010, 9, 1), new Date(2010, 10, 1)]" />
            </el-form-item>
            <el-form-item label="Time Range">
              <div class="example-basic">
                <el-time-picker v-model="lineStartTime" arrow-control placeholder="Start Time" />
                <el-time-picker v-model="lineEndTime" arrow-control placeholder="End Time" />
              </div>
            </el-form-item>
          </el-form>
          <template #footer>
            <span class="dialog-footer">
              <el-button @click="multiLineTransformDialogVisible = false">Cancel</el-button>
              <el-button type="primary" v-loading.fullscreen.lock="fullscreenLoading"
                v-on:click="perfromTransformForMulitLine">
                Confirm
              </el-button>
            </span>
          </template>
        </el-dialog>

      </div>
    </div>

    <div class="choose_container choose_line_type_container ms-1">
      <el-radio-group v-model="chooseLineType" size="medium" @change="handleLineTypeChange">
        <el-radio-button label="Single"></el-radio-button>
        <el-radio-button label="Multi"></el-radio-button>
      </el-radio-group>

    </div>
    <div class="progressive-container ms-1 mt-2" v-if="chooseLineType==='Single'">
      <span>Progressive</span>
        <el-switch v-model="progressive" name="Progressive"  @change="handleProgressiveChange"/>
    </div>



    <div class="table-choose-container mt-2 ms-1" v-if="chooseLineType == 'Single'">
      <el-select v-model="currentTable" placeholder="Select" size="medium" v-if="chooseMode === 'Default'"
        @change="handleTableChange">
        <el-option v-for="(item, idx) in allDefaultTables" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
      <el-select v-model="currentTable" placeholder="Select" size="medium" v-on:click="loadCustomTableAndInfo"
        v-if="chooseMode === 'Custom'" @change="handleTableChange">
        <el-option v-for="(item, idx) in allCustomTables" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
    </div>

    <div class="class-choose-container mt-2 ms-1" v-if="chooseLineType == 'Multi'">
      <el-select v-model="currentMultiClass" placeholder="Select" size="medium" @change="handleMultiLineClassChange"
        v-if="chooseMode === 'Default'">
        <el-option v-for="(item, idx) in Array.from(allMultLineClass.keys())" :key="idx" :label="item" :value="item">
        </el-option>
      </el-select>
      <el-select v-model="currentMultiClass" placeholder="Select" size="medium" @change="handleMultiLineClassChange"
        v-if="chooseMode === 'Custom'">
        <el-option v-for="(item, idx) in Array.from(allCustomMultiLineClass.keys())" :key="idx" :label="item"
          :value="item">
        </el-option>
      </el-select>
    </div>




  </div>
</template>
<script>
import store from "@/store";
import { defineComponent, ref, computed, watch, reactive } from "vue";
import { ElLoading } from 'element-plus'
import { ElNotification } from 'element-plus'
import { clearFlagDB } from "@/indexdb";
export default defineComponent({
  data() {
    return {
      testConnectResult: "Test Connection",
      createDBConn: "Create Connection",
      initOm3DB: "Init OM3 DB",
      clearOm3DB: "Clear OM3 DB",
      customDBHostName: "",
      customDBPassword: '',
      customDBName: '',
      customDBPort:5432,
      customUserName: "",
      isOpenDbSetup: false,
      isOpenTransform: false,
      lineDateRange: [new Date(2010, 9, 1), new Date(2010, 10, 1)],
      lineStartTime: new Date(2010, 9, 1),
      lineEndTime: new Date(2010, 10, 1),
      fullscreenLoading: false,
      singleLineTableName: "",
      multiLineTableNames: [],
      customMultiLineClassName: "",
    }

  },
  components: {

  },
  methods: {
    updateTestConnRes(res) {
      this.testConnectResult = res
    },
    updateDBCreateConn(res) {
      this.createDBConn = res;
    },
    updateInitDB(res) {
      this.initOm3DB = res
    },
    updateClearDB(res) {
      this.clearOm3DB = res;
    },
    openFullScreenLoading() {
      this.fullscreenLoading = true;
    },
    closeFullScreenLoading() {
      this.fullscreenLoading = false;
    },
    openNotification(title, msg, type) {
      ElNotification({
        title: title,
        message: msg,
        type: type,
      })
    },
    handleTestConn() {
      store.dispatch("testCustomDBConn", { hostName: this.customDBHostName, possword: this.customDBPassword, dbName: this.customDBName, userName: this.customUserName,dbPort:this.customDBPort }).then((res) => {
        const result = res.data['data']['result'];
        if (result === 'success') {
          this.storeDBConfig()
          this.updateTestConnRes("Test Success")
          this.openNotification("Test Connection", "Test Connection Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateTestConnRes("Test Fail")
          this.openNotification("Test Connection", res.data['msg'], "error")
        }
      })
    },
    storeDBConfig() {
      localStorage.setItem("customDBHostName", this.customDBHostName)
      localStorage.setItem("customDBPassword", this.customDBPassword)
      localStorage.setItem("customDBName", this.customDBName)
      localStorage.setItem("customUserName", this.customUserName)
      localStorage.setItem("customDBPort",this.customDBPort)
    },
    restoreDBConfig() {
      const customDBHostName = localStorage.getItem("customDBHostName")
      const customDBPassword = localStorage.getItem("customDBPassword");
      const customDBName = localStorage.getItem("customDBName");
      const customDBPort=localStorage.getItem("customDBPort")
      const customUserName = localStorage.getItem("customUserName");
      if (customDBHostName) {
        this.customDBHostName = customDBHostName;
      }
      if (customDBPassword) {
        this.customDBPassword = customDBPassword;
      }
      if (customDBName) {
        this.customDBName = customDBName;
      }
      if (customUserName) {
        this.customUserName = customUserName;
      }
      if(customDBPort){
        this.customDBPort=customDBPort
      }
    },
    createCustomDBConn() {
      store.dispatch("createCustomDBConn", { hostName: this.customDBHostName, possword: this.customDBPassword, dbName: this.customDBName, userName: this.customUserName,dbPort:this.customDBPort }).then((res) => {
        const result = res.data['data']['result'];
        if (result === 'success') {
          this.updateDBCreateConn("Create Success")
          this.openNotification("Create Connection", "Create Connection Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateDBCreateConn("Create Fail")
          this.openNotification("Create Connection", res.data['msg'], "error")
        }
      })
    },
    initOM3DBEnv() {
      store.dispatch("initOM3DB").then((res) => {
        console.log(res)
        const result = res.data['result'];
        if (result === 'success') {
          this.updateInitDB("Init Success")
          this.openNotification("Init OM3 ENV", "Init OM3 ENV Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateInitDB("Init Fail")
          this.openNotification("Init OM3 ENV", res.data['msg'], "error")
        }
      })
    },
    clearOM3DBEnv() {
      console.log("clear om3 env")
      store.dispatch("clearOM3Table").then((res) => {
        console.log(res)
        const result = res.data['result'];
        if (result === 'success') {
          this.updateClearDB("Clear Success")
          this.openNotification("Clear OM3 ENV", "Clear OM3 ENV Success", "success")
        } else {

          console.error(res.data['msg'])
          this.updateClearDB("Clear Fail")
          this.openNotification("Clear OM3 ENV", res.data['msg'], "error")
        }
      })
    },
    performTransformForSingeLine() {
      let startDateStr = this.lineDateRange[0].toISOString().split("T")[0];
      let endDateStr = this.lineDateRange[1].toISOString().split("T")[0];
      let startTimeStr = this.lineStartTime.toISOString().split("T")[1].split('Z')[0];
      let endTimeStr = this.lineEndTime.toISOString().split("T")[1].split('Z')[0];
      const startFullTime = startDateStr + " " + startTimeStr;
      const endFullTime = endDateStr + " " + endTimeStr;
      this.openFullScreenLoading();
      store.dispatch("performTransformForSingeLine", { startTime: startFullTime, endTime: endFullTime, tableName: this.singleLineTableName }).then((res) => {
        if (res.data['code'] === 200) {
          console.log("single line transform success")
          //  add tishi
        } else {
          console.error(res.data['msg'])
        }
        this.closeFullScreenLoading()
        this.dialogFormVisible = false;
      })
    },
    perfromTransformForMulitLine() {
      let startDateStr = this.lineDateRange[0].toISOString().split("T")[0];
      let endDateStr = this.lineDateRange[1].toISOString().split("T")[0];
      let startTimeStr = this.lineStartTime.toISOString().split("T")[1].split('Z')[0];
      let endTimeStr = this.lineEndTime.toISOString().split("T")[1].split('Z')[0];
      const startFullTime = startDateStr + " " + startTimeStr;
      const endFullTime = endDateStr + " " + endTimeStr;
      this.openFullScreenLoading()
      store.dispatch("performTransformForMultiLine", { startTime: startFullTime, endTime: endFullTime, tableNames: Array.from(this.multiLineTableNames.values()), multiLineClassName: this.customMultiLineClassName }).then((res) => {
        if (res.data['code'] === 200) {
          console.log("multi line transform success")
          //  add tishi
        } else {
          console.error(res.data['msg'])
        }
        this.closeFullScreenLoading()
        this.multiLineTransformDialogVisible = false;
      })
      console.log(startFullTime, endFullTime, this.customMultiLineClassName, Array.from(this.multiLineTableNames.values()))
    }


  },

  watch: {
    isOpenDbSetup(newV, oldV) {
      if (newV) {
        this.restoreDBConfig()
      } else {
        this.updateDBCreateConn("Create Connection")
        this.updateTestConnRes("Test Connection")
      }
    },
    isOpenTransform(newV, oldV) {
      if (newV) {
        store.dispatch("getAllCustomTables").then((res) => {
          if (res['code'] === 200) {
            this.allCustomDBTables = res['data']['result'];
          } else {
            console.error(res['msg'])
          }
        })
      }
    }
  },
  setup() {
    const progressive = ref(store.state.controlParams.progressive);
    const chooseMode = ref("Default");
    const chooseLineType = ref("Single");
    const dialogFormVisible = ref(false);

    const multiLineTransformDialogVisible = ref(false)

    const allAlgoritem = store.state.allAlgoritem;

    const currentSampleAlgorithm = ref(
      store.state.controlParams.currentSampleMethod
    );

    const currentTable = ref(store.state.controlParams.currentTable);

    const currentCustomTable = ref(store.state.controlParams.currentCustomTable)
    const currentMultiClass = ref(store.state.controlParams.currentMultiLineClass);



    const allSampleAlgoritem = store.state.controlParams.sampleMethods;

    let allTables = computed(() => {
      return store.state.allTables;
    });
    const allCustomTables = computed(() => {
      return store.state.allCustomTables;
    })

    const allDefaultTables = computed(() => {
      return store.state.allDefaultTables;
    })

    let allMultLineClass = computed(() => {
      return store.state.allMultiLineClassInfoMap
    })

    let allCustomMultiLineClass = computed(() => {
      return store.state.allCustomMultiLineClassInfoMap
    })



    const handleSampleMethodChange = () => {
      store.commit("alterSampleMethod", currentSampleAlgorithm.value);
    };


    const handleModeChange = () => {
      store.commit("alterMode", chooseMode.value);
      chooseLineType.value = "Single"
      store.commit("alterLineType", chooseLineType.value);
    }
    const handleLineTypeChange = () => {
      store.commit("alterLineType", chooseLineType.value)
      store.dispatch("getAllMultiLineClassInfo");
    }
    const handleMultiLineClassChange = () => {
      store.commit("alterCurrentMulitLineClass", currentMultiClass.value)
    }

    const handleTableChange = () => {
      store.commit("alterTable", currentTable);
    };

    const handleCustomTableChange = () => {
      store.commit("alterCustomTable", currentCustomTable.value)
    }

    const loadCustomTableAndInfo = () => {
      store.dispatch("loadCustomTableAndInfo");
    }

    const handleProgressiveChange = () => {
      store.commit("alterProgressive", progressive.value);
    }

    const clearIndexFlag=()=>{
      clearFlagDB()
    }



    return {
      allAlgoritem,
      chooseMode,
      chooseLineType,
      handleModeChange,
      currentSampleAlgorithm,
      allSampleAlgoritem,
      handleSampleMethodChange,
      currentTable,
      allTables,
      allDefaultTables,
      handleTableChange,
      handleLineTypeChange,
      allMultLineClass,
      currentMultiClass,
      handleMultiLineClassChange,
      dialogFormVisible,
      handleCustomTableChange,
      loadCustomTableAndInfo,
      allCustomTables,
      allCustomMultiLineClass,
      multiLineTransformDialogVisible,
      progressive,
      handleProgressiveChange,
      clearIndexFlag
  
    };
  },
});
</script>
<style scoped>
.data-control-panel {
  min-width: 200px;
  max-width: 210px;
  flex-grow: 0.15;
}

.choose_container {
  margin-top: 10px;
}

.btn {
  width: 100%;
  margin-left: 0px;
}

.multi-line-select {
  width: 100%;
}
</style>