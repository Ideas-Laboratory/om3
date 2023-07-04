const {WebSocketServer} = require("ws");
const jsonpack = require("jsonpack/main");
const {Init}=require("../routes/ws_handlers");
const {unPackDataAndRouter} = require("../helper/pack_rounter")

const wss = new WebSocketServer({
    host:"0.0.0.0",
    port:3001,
})
Init()


// const a={hello:"123"}
// const packedData=jsonpack.pack(JSON.stringify(a))
// console.log(packedData)
// const unPackData=jsonpack.unpack(packedData)
// console.log(unPackData)


wss.on("connection",function connection(ws){
    console.log("connect")
    ws.on("message",function message(data){
       unPackDataAndRouter(data.toString(),ws)
    })
})


