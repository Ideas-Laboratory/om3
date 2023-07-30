package main

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"io"
	"log"
	"net/http"
	"om3-encoder/encode"
	"om3-encoder/util"
	"os"
	"runtime"
	"strconv"
	"time"
)

func GetDB(hostName,userName,password,dbName ,port string) (*gorm.DB,error){
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:              5*time.Second,   // Slow SQL threshold
			LogLevel:                   logger.Silent, // Log level
			IgnoreRecordNotFoundError: true,           // Ignore ErrRecordNotFound error for logger
			ParameterizedQueries:      true,           // Don't include params in the SQL log
			Colorful:                  false,          // Disable color
		},
	)
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                 fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s",hostName,userName,password,dbName,port),
		PreferSimpleProtocol: true,
	}),&gorm.Config{Logger: newLogger})

	db=db.Session(&gorm.Session{SkipDefaultTransaction: true})


		if err != nil {
			fmt.Println("db init eror")
			return nil,err
		}

	return db, nil
}

var DBMap=make(map[string]*gorm.DB)
var DBConfig=make(map[string]string)


func HandleEncode(w http.ResponseWriter, r *http.Request)  {
	rawTableName:=r.URL.Query().Get("raw_table_name")
	tempTableName:=r.URL.Query().Get("temp_table_name")
	targetTableName:=r.URL.Query().Get("target_table_name")
	maxLevel,_:=strconv.Atoi(r.URL.Query().Get("max_level"))
	mode:=r.URL.Query().Get("mode")
	userCookie:=r.Header.Get("authorization")
	if len(rawTableName)==0||len(tempTableName)==0||len(targetTableName)==0||len(userCookie)==0||maxLevel<1{
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":400,
			"msg":"参数错误",
		}))
		return
	}
	fmt.Printf("raw_table_name:%s,temp_table_name:%s,target_table_name:%s,max_level:%v,mode:%s",rawTableName,tempTableName,targetTableName,maxLevel,mode)
	cpuNum:=runtime.NumCPU()
	fmt.Printf("your cpu num is %d, om3 will use ",cpuNum)
	if cpuNum>4{
		cpuNum=cpuNum-2
	}
	db,ok:=DBMap[userCookie]
	if !ok{
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":400,
			"msg":"db not create connection",
		}))
		return
	}

	encodeManager:= encode.InitTaskManager(encode.WorkManagerConf{DB:db,MaxLevel: maxLevel,RawTableName: rawTableName,TargetTableName: targetTableName,TempTableName: tempTableName,WorkNum: cpuNum,Mode: mode})
	encodeManager.Run()
	io.WriteString(w,util.MarshalMap(map[string]interface{}{
		"code":200,
		"msg":"encode success",
	}))
	return

}

func HandleTestDBConnection(w http.ResponseWriter, r *http.Request)  {
	hostName:=r.URL.Query().Get("host_name")
	password:=r.URL.Query().Get("password")
	dbName := r.URL.Query().Get("db_name")
	userName:= r.URL.Query().Get("user_name")
	port:= r.URL.Query().Get("port")
	userCookie:=r.Header.Get("authorization")
	dbConfig:=fmt.Sprintf("host_name:%s, password:%s, db_name:%s, user_name:%s, port:%s, cookie:%s",hostName,password,dbName,userName,port,userCookie)
	fmt.Println(dbConfig)
	if len(hostName)==0||len(password)==0||len(dbName)==0||len(userName)==0||len(port)==0||len(userCookie)==0{
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":400,
			"msg":"input error",
		}))
		fmt.Println("HandleTestDBConnection input error")
		return
	}
	oldConfig,ok1:=DBConfig[userCookie]
	oldDB,ok2:=DBMap[userCookie]
	if ok1&&ok2 && oldConfig==dbConfig && TestDBIsUseful(oldDB){
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":200,
			"msg":"success",
		}))
		fmt.Println("test success")
		return
	}
	if _,ok:=DBMap[userCookie];ok{
		delete(DBMap,userCookie)
		delete(DBConfig,userCookie)
	}
	newDB,err:=GetDB(hostName,userName,password,dbName,port)
	if err !=nil{
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":400,
			"msg":"db create error",
		}))
		fmt.Println("test db create error")
		return
	}

	if TestDBIsUseful(newDB){
		DBMap[userCookie]=newDB
		DBConfig[userCookie]=dbConfig
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":200,
			"msg":"success",
		}))
		fmt.Println("test success")
		return
	}
	io.WriteString(w,util.MarshalMap(map[string]interface{}{
		"code":400,
		"msg":"error",
	}))
	fmt.Println("test unknown error")
	return
}

func TestDBIsUseful(db *gorm.DB) bool {
	rows,err:=db.Raw("select 1 as num").Rows()
	if err !=nil{
		fmt.Println("TestDBIsUseful err ",err.Error())
		return false
	}
	allRow,_:=rows.Columns()
	fmt.Println(allRow)
	if len(allRow)==1{
		return true
	}
	return false
}



func HandleCreateDB(w http.ResponseWriter, r *http.Request)  {
	hostName:=r.URL.Query().Get("host_name")
	password:=r.URL.Query().Get("password")
	dbName := r.URL.Query().Get("db_name")
	userName:= r.URL.Query().Get("user_name")
	port:= r.URL.Query().Get("port")
	userCookie:=r.Header.Get("authorization")
	if len(hostName)==0||len(password)==0||len(dbName)==0||len(userName)==0||len(port)==0||len(userCookie)==0{
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":400,
			"msg":"参数错误",
		}))
		fmt.Println("HandleCreateDB input error")
		return
	}
	dbConfig:=fmt.Sprintf("host_name:%s, password:%s, db_name:%s, user_name:%s, port:%s, cookie:%s",hostName,password,dbName,userName,port,userCookie)
	oldConfig,ok1:=DBConfig[userCookie]
	oldDB,ok2:=DBMap[userCookie]
	if ok1&&ok2 && oldConfig==dbConfig && TestDBIsUseful(oldDB){
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":200,
			"msg":"success",
		}))
		fmt.Println("HandleCreateDB success")
		return
	}
	if _,ok:=DBMap[userCookie];ok{
		delete(DBMap,userCookie)
		delete(DBConfig,userCookie)
	}
	newDB,err:=GetDB(hostName,userName,password,dbName,port)
	if err !=nil{
		io.WriteString(w,util.MarshalMap(map[string]interface{}{
			"code":400,
			"msg":"db create error",
		}))
		fmt.Println("HandleCreateDB get db error",err.Error())
		return
	}
	DBMap[userCookie]=newDB
	DBConfig[userCookie]=dbConfig
	fmt.Println(dbConfig)
	io.WriteString(w,util.MarshalMap(map[string]interface{}{
		"code":200,
		"msg":"success",
	}))
	fmt.Println("HandleCreateDB success")
	return
}
func startServer()  {
	defer func() {
		err:=recover()
		fmt.Println(err)
	}()
	http.HandleFunc("/om3_encode",HandleEncode)
	http.HandleFunc("/createCustomDBConn",HandleCreateDB)
	http.HandleFunc("/testDBConnection",HandleTestDBConnection)
	err:=http.ListenAndServe(":3330",nil)
	if err !=nil{
		fmt.Println("server failed!")
	}
}
func main() {
	startServer()
}