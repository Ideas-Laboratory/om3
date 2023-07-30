package encode

import (
	"fmt"
	"gorm.io/gorm"
	"math"
	"os"
	"strings"
	"sync"
)

var fileLock sync.Mutex

func appendFileName(fileName string, om3TableName string) error {
	fileLock.Lock()
	defer func() {
		fileLock.Unlock()
	}()
	outputFile, err := os.OpenFile("./"+om3TableName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println("appendFileName 无法打开输出文件:", err)
		return err
	}
	defer outputFile.Close()

	// 将数据追加写入输出文件
	_, err = outputFile.Write([]byte(fmt.Sprintf("%s\r\n",  fileName)))
	if err != nil {
		fmt.Println("appendFileName 无法写入数据:", err)
		return err
	}
	return nil
}
func computeTableFlag(data []*ObjEncode, curIndex int64, m3TableName string) {

	bufLen := len(data)
	arrayBuffer := make([]byte, bufLen)

	for j := 0; j < len(data); j += 2 {
		if data[j] == nil && data[j+1] == nil {
			continue
		} else if data[j] == nil {
			arrayBuffer[j] = 1
			arrayBuffer[j+1] = 0
			continue
		} else if data[j+1] == nil {
			arrayBuffer[j] = 1
			arrayBuffer[j+1] = 1
			continue
		}
		if data[j].v < data[j+1].v {
			arrayBuffer[j] = 0
			arrayBuffer[j+1] = 0
		} else {
			arrayBuffer[j] = 0
			arrayBuffer[j+1] = 1
		}
	}

	filePath := fmt.Sprintf("./flags/%s**%v.flagz", m3TableName, curIndex)
	writeFile, err := os.Create(filePath)
	if err != nil {
		fmt.Println("computeTableFlag Error Open file:", err)
	}
	defer writeFile.Close()
	_, err = writeFile.Write(arrayBuffer)

	if err != nil {
		fmt.Println("computeTableFlag Error writing file:", err)
		return
	}
	err = appendFileName( filePath, m3TableName)
	if err != nil {
		fmt.Println("computeTableFlag file name input error:", err)
	}
	fmt.Println("computeTableFlag compute ordering flags finished:", m3TableName)
}

type MinMaxM struct {
	I    int64
	MaxV float64
	MinV float64
}

type NonuniformMinMaxEncode struct {
	db *gorm.DB
}

type ObjEncode struct {
	v float64
}

var objPool *sync.Pool
var poolInitOnce sync.Once

func GetSyncPool() *sync.Pool {
	poolInitOnce.Do(func() {
		objPool = &sync.Pool{
			New: func() interface{} {
				return &ObjEncode{}
			},
		}
	})
	return objPool
}

type DifObj struct {
	isNil bool
	v     float64
}

var diffPool *sync.Pool
var diffPoolInitOnce sync.Once

func GetDiffPool() *sync.Pool {
	diffPoolInitOnce.Do(func() {
		diffPool = &sync.Pool{
			New: func() interface{} {
				return &DifObj{}
			},
		}
	})
	return diffPool
}

func New(db *gorm.DB) *NonuniformMinMaxEncode {
	return &NonuniformMinMaxEncode{
		db: db,
	}
}
func (n *NonuniformMinMaxEncode) Encode(task *Task, maxLevel int, mode string) {
	name := task.TargetTableName

	querySQL := fmt.Sprintf("select i,minv as min_v,maxv as max_v from %s where  l=%v and i>=%v and i<=%v", task.TempTableName, task.CurLevel, task.StartIndex, task.EndIndex)
	if task.CurLevel == maxLevel {
		querySQL = fmt.Sprintf("select t as i,v as min_v,v as max_v from %s where t>= %v and t<=%v", task.RawTableName, task.StartIndex, task.EndIndex)
	}
	if strings.Contains(name, ".") {
		name = strings.Split(name, ".")[1]
	}
	fileName := name
	if mode == "Custom" {
		fileName = "custom_" + name
	}
	fmt.Println(fileName)
	var minMaxMs []*MinMaxM
	fmt.Println(querySQL)
	n.db.Raw(querySQL).Scan(&minMaxMs)
	iterLen := task.EndIndex - task.StartIndex + 1
	minVs := make([]*ObjEncode, iterLen, iterLen)
	maxVs := make([]*ObjEncode, iterLen, iterLen)

	for _, minMaxM := range minMaxMs {
		minV := GetSyncPool().Get().(*ObjEncode)
		maxV := GetSyncPool().Get().(*ObjEncode)
		minV.v = minMaxM.MinV
		maxV.v = minMaxM.MaxV
		minVs[minMaxM.I-task.StartIndex] = minV
		maxVs[minMaxM.I-task.StartIndex] = maxV
	}
	if task.CurLevel == maxLevel {
		computeTableFlag(minVs, task.StartIndex/(task.EndIndex-task.StartIndex+1), fileName)
	}

	curL := 1
	iterL := int(math.Log2(float64(iterLen)))
	for l := curL; l <= iterL; l++ {
		fmt.Println("compute level:", l)
		curArrayLen := int(math.Pow(2, float64(iterL-l)))
		curMinVDiff := make([]*DifObj, curArrayLen)
		curMaxVDiff := make([]*DifObj, curArrayLen)

		curMinV := make([]*ObjEncode, curArrayLen)
		curMaxV := make([]*ObjEncode, curArrayLen)
		curLevelComputeNum := int(math.Pow(2, float64(iterL-l+1)))
		for i := 0; i < curLevelComputeNum; i = i + 2 {

			curMinDif := GetDiffPool().Get().(*DifObj)
			curMaxDif := GetDiffPool().Get().(*DifObj)
			// Min
			if minVs[i] == nil && minVs[i+1] != nil {
				curMinV[i/2] = minVs[i+1]
				curMinDif.isNil = true
			} else if minVs[i] != nil && minVs[i+1] == nil {
				curMinV[i/2] = minVs[i]
				curMinDif.isNil = true
			} else if minVs[i] == nil && minVs[i+1] == nil {
				curMinV[i/2] = nil
				curMinDif.isNil = true
			} else {
				if minVs[i].v < minVs[i+1].v {
					curMinV[i/2] = minVs[i]
					GetSyncPool().Put(minVs[i+1])
				} else {
					curMinV[i/2] = minVs[i+1]
					GetSyncPool().Put(minVs[i])
				}
				curMinDif.isNil = false
				curMinDif.v = minVs[i].v - minVs[i+1].v
			}
			curMinVDiff[i/2] = curMinDif

			//max
			if maxVs[i] == nil && maxVs[i+1] != nil {
				curMaxV[i/2] = maxVs[i+1]
				curMaxDif.isNil = true
			} else if maxVs[i] != nil && maxVs[i+1] == nil {
				curMaxV[i/2] = maxVs[i]
				curMaxDif.isNil = true
			} else if maxVs[i] == nil && maxVs[i+1] == nil {
				curMaxV[i/2] = nil
				curMinDif.isNil = true
			} else {
				if maxVs[i].v > maxVs[i+1].v {
					curMaxV[i/2] = maxVs[i]
					GetSyncPool().Put(maxVs[i+1])
				} else {
					curMaxV[i/2] = maxVs[i+1]
					GetSyncPool().Put(maxVs[i])
				}
				curMaxDif.isNil = false
				curMaxDif.v = maxVs[i].v - maxVs[i+1].v
			}
			curMaxVDiff[i/2] = curMaxDif
		}
		minVs = curMinV
		maxVs = curMaxV

		insertSQL := fmt.Sprintf("insert into %s(i,minvd,maxvd) values", task.TargetTableName)
		j := 0
		for j < len(curMaxVDiff) {
			if task.CurLevel == maxLevel && l == 1 {
				break
			}
			usedL := task.CurLevel - l
			indexOffset := task.StartIndex / int64(math.Pow(2, float64(l)))
			tempSQL := ""
			if j+10000 < len(curMaxVDiff) {
				for k := j; k < j+10000; k++ {
					if curMinVDiff[k].isNil && curMaxVDiff[k].isNil {
						continue
					}
					curIndex := int64(math.Pow(2, float64(usedL))) + indexOffset + int64(k)
					curInsertMinDif := "NULL"
					curInsertMaxDif := "NULL"
					if !curMinVDiff[k].isNil {
						curInsertMinDif = fmt.Sprintf("%v", curMinVDiff[k].v)
					}
					if !curMaxVDiff[k].isNil {
						curInsertMaxDif = fmt.Sprintf("%v", curMaxVDiff[k].v)
					}
					if len(tempSQL) == 0 {
						tempSQL = tempSQL + fmt.Sprintf(" (%v,%s,%s)", curIndex, curInsertMinDif, curInsertMaxDif)
					} else {
						tempSQL = tempSQL + fmt.Sprintf(",(%v,%s,%s)", curIndex, curInsertMinDif, curInsertMaxDif)
					}
					GetDiffPool().Put(curMinVDiff[k])
					GetDiffPool().Put(curMaxVDiff[k])
				}
			} else {
				for k := j; k < len(curMaxVDiff); k++ {
					if curMinVDiff[k].isNil && curMaxVDiff[k].isNil {
						continue
					}
					curIndex := int64(math.Pow(2, float64(usedL))) + indexOffset + int64(k)
					curInsertMinDif := "NULL"
					curInsertMaxDif := "NULL"
					if !curMinVDiff[k].isNil {
						curInsertMinDif = fmt.Sprintf("%v", curMinVDiff[k].v)
					}
					if !curMaxVDiff[k].isNil {
						curInsertMaxDif = fmt.Sprintf("%v", curMaxVDiff[k].v)
					}
					if len(tempSQL) == 0 {
						tempSQL = tempSQL + fmt.Sprintf(" (%v,%s,%s)", curIndex, curInsertMinDif, curInsertMaxDif)
					} else {
						tempSQL = tempSQL + fmt.Sprintf(",(%v,%s,%s)", curIndex, curInsertMinDif, curInsertMaxDif)
					}

					GetDiffPool().Put(curMinVDiff[k])
					GetDiffPool().Put(curMaxVDiff[k])
				}
			}
			j += 10000
			if len(tempSQL) == 0 {
				continue
			}
			useSQL := insertSQL + tempSQL
			//fmt.Println(useSQL)
			n.db.Exec(useSQL)
		}
	}
	if len(minVs) == 1 && len(maxVs) == 1 {
		tempMinMaxIndex := task.StartIndex / iterLen
		tempL := task.CurLevel - iterL
		insertTempMinMaxSQL := fmt.Sprintf("insert into %s(l,i,minv,maxv) values(%v,%v,%v,%v)", task.TempTableName, tempL, tempMinMaxIndex, minVs[0].v, maxVs[0].v)
		if tempL == 0 && tempMinMaxIndex == 0 {
			insertTempMinMaxSQL = fmt.Sprintf("insert into %s(i,minvd,maxvd) values(%v,%v,%v)", task.TargetTableName, -1, minVs[0].v, maxVs[0].v)
		}
		n.db.Exec(insertTempMinMaxSQL)

		//插入MinMax
	} else {
		fmt.Println("error.......")
		fmt.Println("minVs len:",len(minVs),"maxVs len:",len(maxVs))
	}

}
