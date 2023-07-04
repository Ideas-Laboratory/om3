const express = require('express');
const router = express.Router();


const fileMap={
    "single_line_raw":"single_raw",
    "multi_line_raw":"multi_raw",
    "test":"hhh.js"
}

router.get("/download",function(req,res){
    const query=req.query
    const fileName=query['filename'];
    console.log(query)
    if(fileName===undefined||fileMap[fileName]===undefined){
        res.status(404).send('File not found');
    }
    const filePath = `./public/data/${fileMap[fileName]}`; // 文件路径
    
  res.download(filePath, function(err){
    if (err) {
        console.log(err)
      // 如果出现错误，例如文件不存在，返回 404 错误
      res.status(404).send('File not found');
    }
  });
})
module.exports = router;