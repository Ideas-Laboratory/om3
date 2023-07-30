package encode

import (
	"bufio"
	"fmt"
	"gorm.io/gorm"
	"io"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func CombineOM3Flag(om3Name string, mode string) error {
	name := om3Name
	if strings.Contains(name, ".") {
		name = strings.Split(name, ".")[1]
	}
	fileName := name
	if mode == "Custom" {
		fileName = "custom_" + name
	}
	fmt.Println("mode:",mode)
	fileNamePath := "./" + fileName
	env:=os.Getenv("DOCKER_ENV")
	var filePath string
	if env=="docker_env"{
		filePath = fmt.Sprintf("./flags/%s.flagz", fileName)
	}else{
		filePath=fmt.Sprintf("../flags/single_line/%s.flagz", fileName)
	}
	fmt.Println("out path:",filePath)


	file, err := os.Open(fileNamePath)
	if err != nil {
		fmt.Println("无法打开文件:", err)
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	fileNames := make([]string, 0, 10)
	for {
		lineRes, _, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println(err)
		}
		if string(lineRes) == "EOF" || len(lineRes) == 0 {
			break
		}
		fileNames = append(fileNames, string(lineRes))
	}
	usedFileNames := make([]string, len(fileNames))
	for _, tempName := range fileNames {
		useNameIndex := strings.Split(strings.Split(tempName, "**")[1], ".")[0]
		indx, _ := strconv.Atoi(useNameIndex)
		usedFileNames[indx] = tempName
	}
	outputFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("无法打开输出文件:", err)
		return err
	}
	defer outputFile.Close()
	for _, tempName := range usedFileNames {
		// 读取输入文件的数据
		inputData, err := ioutil.ReadFile(tempName)
		if err != nil {
			fmt.Println("无法读取输入文件:", err)
			return err
		}
		// 将数据追加写入输出文件
		_, err = outputFile.Write(inputData)
		if err != nil {
			fmt.Println("无法写入数据:", err)
			return err
		}
		os.Remove(tempName)
	}
	os.Remove(fileNamePath)
	return nil
}

var dB *gorm.DB

var dbOnce sync.Once


type Task struct {
	RawTableName    string
	TempTableName   string
	TargetTableName string
	TableName       string
	StartIndex      int64
	EndIndex        int64
	CurLevel        int
	WorkID          string
	Status          string
	Procedure       string
	TaskType        string
	createTime      time.Time
}

type TaskManager struct {
	RawTableName         string
	TempTableName        string
	TargetTableName      string
	DataLen              int64
	CurLevel             int
	CurIndex             int64
	WorkNum              int
	EachProcessDataNum   int64
	EachProcessDataLevel int
	WorkDisChan          chan *Task
	WorkSubmitChan       chan *Task
	Status               string
	waitLevelLast        sync.WaitGroup
	works                []*Worker
	MaxLevel             int
	Mode                 string
	DB                   *gorm.DB
}

type WorkManagerConf struct {
	RawTableName    string
	TempTableName   string
	TargetTableName string
	MaxLevel        int
	Mode            string
	WorkNum         int
	DB *gorm.DB
}

func InitTaskManager(workManageConf WorkManagerConf) TaskManager {
	eachLevel := 20
	eachLevelNum :=1048576
	if workManageConf.MaxLevel <= 21 {
		eachLevel = workManageConf.MaxLevel
		eachLevelNum = int(math.Pow(2, float64(workManageConf.MaxLevel)))
	}


	// 计算DataLen
	return TaskManager{
		WorkSubmitChan:       make(chan *Task, 1),
		works:                make([]*Worker, 0, workManageConf.WorkNum),
		RawTableName:         workManageConf.RawTableName,
		TempTableName:        workManageConf.TempTableName,
		TargetTableName:      workManageConf.TargetTableName,
		CurLevel:             workManageConf.MaxLevel,
		MaxLevel:             workManageConf.MaxLevel,
		CurIndex:             0,
		EachProcessDataNum:   int64(eachLevelNum),
		EachProcessDataLevel: eachLevel,
		WorkDisChan:          make(chan *Task, 1),
		Status:               "init",
		WorkNum:              workManageConf.WorkNum,
		DB:                   workManageConf.DB,
		Mode:                 workManageConf.Mode,
	}
}

func (t *TaskManager) Run() {
	var finishGroup sync.WaitGroup
	var workGroup sync.WaitGroup
	finishGroup.Add(1)
	workGroup.Add(t.WorkNum)

	for i := 0; i < t.WorkNum; i++ {
		workID := fmt.Sprintf("work_%v", i)
		work := NewWorker(workID, t.WorkDisChan, t.WorkSubmitChan, t)
		t.works = append(t.works, work)

		go func() {
			work.Run()
			workGroup.Done()
		}()

	}
	go func() {
		t.ProcessTaskSubmit()
		finishGroup.Done()
	}()

	t.DistributeTask()
	close(t.WorkDisChan)
	workGroup.Wait()

	close(t.WorkSubmitChan)
	finishGroup.Wait()
	err := CombineOM3Flag(t.TargetTableName, t.Mode)
	if err != nil {
		fmt.Println("combine om3 flags err:", err)
	}

}

func (t *TaskManager) CreateNextTask() (*Task, bool) {
	startIndex := t.CurIndex
	endIndex := t.CurIndex + t.EachProcessDataNum - 1
	curLevelMaxIndex := int64(math.Pow(2, float64(t.CurLevel))) - 1
	hasMore := true

	taskType := "common"
	if endIndex >= curLevelMaxIndex {
		t.WaitLevelFinish()

		taskType = "level_last"
		endIndex = curLevelMaxIndex
		if t.CurLevel == 0 {
			t.Status = "Finish"
			hasMore = false
		}

	} else {
		t.CurIndex = endIndex + 1
	}
	fmt.Println("create tasks")
	task := &Task{
		RawTableName:    t.RawTableName,
		TempTableName:   t.TempTableName,
		TargetTableName: t.TargetTableName,
		CurLevel:        t.CurLevel,
		StartIndex:      startIndex,
		EndIndex:        endIndex,
		Status:          "wait",
		Procedure:       "processing",
		TaskType:        taskType,
		createTime:      time.Now(),
	}
	if taskType == "level_last" && t.CurLevel >= t.EachProcessDataLevel {
		t.CurLevel = t.CurLevel - t.EachProcessDataLevel
		t.CurIndex = 0
		//hasMore=false
	}
	if taskType == "level_last" &&task.CurLevel <= t.EachProcessDataLevel{
		hasMore=false
	}

	return task, hasMore
	// 创建当前任务
	// 判断是否是当前层次最后一个任务
	// 如果是最后一个任务，则Wait 最后一个任务提交，在此之前不可以再创建
}

func (t *TaskManager) ProcessTaskSubmit() {
	for task := range t.WorkSubmitChan {
		if task.TaskType == "level_last" {
			for _, work := range t.works {
				work.WaitWorkFinish()
			}

			t.EndWait()
		}
	}
}

func (t *TaskManager) DistributeTask() {
	for {
		task, hasMore := t.CreateNextTask()

		t.WorkDisChan <- task
		if !hasMore {
			break
		}
		fmt.Println("task dis")
		t.NeedWait()
	}
	fmt.Println("encode finish")
}
func (t *TaskManager) WaitLevelFinish() {
	t.waitLevelLast.Add(1)

}
func (t *TaskManager) EndWait() {
	t.waitLevelLast.Done()

}
func (t *TaskManager) NeedWait() {
	t.waitLevelLast.Wait()
}

type Worker struct {
	WorkerID        string
	Status          string
	TaskReceiveChan chan *Task
	TaskSubmitChan  chan *Task
	statusLock      sync.RWMutex
	manager         *TaskManager
}

func NewWorker(id string, taskReceiveChan, taskSubmitChan chan *Task, manager *TaskManager) *Worker {
	return &Worker{
		WorkerID:        id,
		Status:          "normal",
		TaskReceiveChan: taskReceiveChan,
		TaskSubmitChan:  taskSubmitChan,
		manager:         manager,
	}
}

func (w *Worker) StartWork() {
	w.statusLock.Lock()
}

func (w *Worker) EndWork() {
	w.statusLock.Unlock()
}

func (w *Worker) WaitWorkFinish() {
	w.statusLock.Lock()
	w.statusLock.Unlock()
}

func (w *Worker) Run() {

	fmt.Println("work run,", w.WorkerID)
	for task := range w.TaskReceiveChan {
		task.WorkID=w.WorkerID
		w.StartWork()
		if task.Status != "wait" {
			fmt.Println("error-------")
		}
		encoder := New(w.manager.DB)
		encoder.Encode(task, w.manager.MaxLevel, w.manager.Mode)
		fmt.Println(fmt.Sprintf("%+v", task), time.Since(task.createTime))
		task.Status = "submit"
		w.EndWork()
		w.TaskSubmitChan <- task
	}

}
