const jsonpack = require("jsonpack/main");
const {routerMap}=require("../routes/ws_handlers")

function unPackDataAndRouter(data,ws){
    const unpackedData=JSON.parse(data)//jsonpack.unpack(data)
    const url=unpackedData["url"];
    if(url==""||!url){
        return
    }
    const hander=routerMap.get(url)
    hander(unpackedData,ws)
}

module.exports={unPackDataAndRouter}