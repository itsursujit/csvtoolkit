package csvtk

import (
	"encoding/csv"
	"fmt"
	"github.com/shenwei356/xopen"
	"regexp"
	"strings"
	"time"
)

type Files string
func From(files string) Files {
	return Files(files)
}

type FormatInput struct {
Fields []int
IgnoreCase bool
Colnames []string
OutFile string
}

func(fps Files)Format(input FormatInput) string{

	//config := getConfigs(cmd)//todo:sachin: Might expose it too
	files := fps

	fields := input.Fields
	var fieldsMap map[int]struct{}
	var fieldsOrder map[int]int      // for set the order of fields
	var colnamesOrder map[string]int // for set the order of fields

	ignoreCase := input.IgnoreCase

	if len(fields) > 0 {
		fields2 := make([]int, len(fields))
		fieldsMap = make(map[int]struct{}, len(fields))
		for i, f := range fields {
			fieldsMap[f] = struct{}{}
			fields2[i] = f
		}
		fields = fields2

		fieldsOrder = make(map[int]int, len(fields))
		i := 0
		for _, f := range fields {
			fieldsOrder[f] = i
			i++
		}

	} else {
		fieldsOrder = make(map[int]int, len(input.Colnames))
		colnamesOrder = make(map[string]int, len(input.Colnames))
	}
	if input.OutFile == "" {
		input.OutFile = "temp" + time.Now().String() + ".csv"
	}
	outfh, _ := xopen.Wopen(input.OutFile)

	defer outfh.Close()

	writer := csv.NewWriter(outfh)

	file := string(files)
	csvReader, _ := NewCSVReader(file, 4096, 4096)
	csvReader.Run()

	parseHeaderRow := true // parsing header row
	var colnames2fileds map[string]int   // column name -> field
	var colnamesMap map[string]*regexp.Regexp

	checkFields := true
	var items []string

	printMetaLine := true
	for chunk := range csvReader.Ch {

		if printMetaLine && len(csvReader.MetaLine) > 0 {
			outfh.WriteString(fmt.Sprintf("sep=%s\n", string(writer.Comma)))
			printMetaLine = false
		}

		for _, record := range chunk.Data {
			if parseHeaderRow { // parsing header row
				colnames2fileds = make(map[string]int, len(record))
				for i, col := range record {
					if ignoreCase {
						col = strings.ToLower(col)
					}
					colnames2fileds[col] = i + 1
				}
				colnamesMap = make(map[string]*regexp.Regexp, len(input.Colnames))
				i := 0
				for _, col := range input.Colnames {
					if ignoreCase {
						col = strings.ToLower(col)
					}
					colnamesMap[col] = fuzzyField2Regexp(col)
					colnamesOrder[col] = i
					i++
				}
				}

				if len(fields) == 0 { // user gives the colnames
					fields = []int{}
					for _, col := range record {
						var ok bool

						_, ok = colnamesMap[col]
						if ok {
							fields = append(fields, colnames2fileds[col])
							fieldsOrder[colnames2fileds[col]] = colnamesOrder[col]
						}
					}

				}

				fieldsMap = make(map[int]struct{}, len(fields))
				for _, f := range fields {
					fieldsMap[f] = struct{}{}
				}

				parseHeaderRow = false

			if checkFields {
				fields2 := []int{}
				for f := range record {
					_, ok := fieldsMap[f+1]
					if ok {
						fields2 = append(fields2, f+1)
					}

				}
				fields = fields2

				items = make([]string, len(fields))

				checkFields = false
			}

			for i, f := range fields {
				items[i] = record[f-1]
			}
			writer.Write(items)
		}
	}

	writer.Flush()
	return input.OutFile
}


