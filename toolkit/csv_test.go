package csvtk

import (
	"fmt"
	"testing"
)

func Test_CSV(t *testing.T) {

	reader, err := NewCSVReader("1.csv", 4096, 4096)
	if err != nil {
		fmt.Println(err)
	}
	reader.Run()
	fmt.Println(reader)

}

func Test_OutPut(t *testing.T) {
	flags := InputFlags{
		PrintFreq: 1,
		Total:     100,
		Buffsize:  4096,
		Lines:     true,
		Files:     []string{"1.csv"},
	}
	err := Output(flags, "test.csv")
	fmt.Println(err)
}

func Test_Format(t *testing.T) {
	flags := FormatInput{
		Fields:     []int{1},
		IgnoreCase: true,
		OutFile:    "test.csv",
	}
	err := From("1.csv").Format(flags)
	fmt.Println(err)
}

func Test_Collapse(t *testing.T) {
	flags := CollapseInput{
		Fields:    []int{1},
		Separater: "-",
		OutFile:   "test.csv",
		VfieldStr: "goy",
	}
	err := From("1.csv").Collapse(flags)
	fmt.Println(err)
}
