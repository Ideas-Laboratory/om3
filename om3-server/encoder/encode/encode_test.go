package encode

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestNonuniformMinMaxEncode_Encode(t *testing.T) {
	//db,_:= task.InitDB()
	//encoder:=New(db)
	//encoder.Encode(&task.Task{
	//	RawTableName: "raw_data.mock_test_data",
	//	TargetTableName: "raw_data.mock_test_data_om3",
	//	TempTableName: "raw_data.mock_test_data_temp",
	//	CurLevel: 1,
	//	StartIndex: 0,
	//	EndIndex: 1,
	//},4,"default")
	file, err := os.Open("./test")
	if err != nil {
		fmt.Println("无法打开文件:", err)

	}
	defer file.Close()

	// 创建 bufio 读取器
	reader := bufio.NewReader(file)
	for ;;{
		lineRes,_,err:=reader.ReadLine()
		if err !=nil{
			if err==io.EOF{
				break
			}
			fmt.Println(err)
		}


		if string(lineRes)=="EOF" || len(lineRes)==0{
			break
		}
		fmt.Println(string(lineRes))
	}
}
