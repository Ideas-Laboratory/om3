import store from "@/store";



let flagMap:any={}
let isFlagSet=false;
export function setFlagMap(curflagMap:any){
    isFlagSet=true
    Object.keys(curflagMap).forEach(v=>{
        flagMap[v]=curflagMap[v];
    })
    
}

export function getFlag(name:string){
    if(!isFlagSet){
        throw new Error("flag unset")
    }
    let finalName=name;
    if(name.includes(".")){
        finalName=name.split(".")[1]
    }
    if(store.state.controlParams.currentMode==='Custom'){
        finalName="custom_"+finalName
    }
    return flagMap[finalName]
}