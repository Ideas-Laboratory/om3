import density from "line-density"
import ndarray from "ndarray"

const data=ndarray([1,2,3,2,1,2,1,6,7],[3,3])

export async function computeDensity() {
    const lineDensity=await density(data,{start:0,stop:3,step:1},{start:0,stop:3,step:1})
    console.log(lineDensity);
} 
