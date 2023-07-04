import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import store from './store'
import axios from 'axios'
import { v4 as uuidv4 } from 'uuid';

import ElementPlus from 'element-plus';


import 'element-plus/dist/index.css';

axios.defaults.baseURL="http://127.0.0.1:3000"//

let myCookie=localStorage.getItem("om3_cookie");
if(myCookie===null||myCookie===''){
    myCookie='user_cookie='+uuidv4()
    localStorage.setItem("om3_cookie",myCookie);
}
console.log("mycookie:",myCookie)
axios.defaults.headers.common['Authorization'] = myCookie

//computeDensity()

createApp(App).use(ElementPlus).use(store).use(router).mount('#app');


