package encode

import "testing"

func TestCombineOM3Flag(t *testing.T) {
	//CombineOM3Flag("test","default")
	encodeManager := InitTaskManager(WorkManagerConf{MaxLevel: 23, RawTableName: "raw_data.mock_guassian_sin_8m", TargetTableName: "om3.mock_guassian1_sin_om3_8m", TempTableName: "om3.mock_guassian1_temp", WorkNum: 4})
	encodeManager.Run()

}