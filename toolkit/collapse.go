package csvtk

import (
	"encoding/csv"
	"fmt"
	"github.com/shenwei356/util/stringutil"
	"github.com/shenwei356/xopen"
	"regexp"
	"strings"
	"time"
)

type CollapseInput struct {
	Fields    []int
	VfieldStr string
	Separater string
	Colnames  []string
	OutFile   string
}

func (fps Files) Collapse(input CollapseInput) Files {

	separater := input.Separater

	fields, colnames := input.Fields, input.Colnames
	var fieldsMap map[int]struct{}
	var fieldsOrder map[int]int      // for set the order of fields
	var colnamesOrder map[string]int // for set the order of fields
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
		fieldsOrder = make(map[int]int, len(colnames))
		colnamesOrder = make(map[string]int, len(colnames))
	}

	if input.OutFile == "" {
		input.OutFile = "temp" + time.Now().String() + ".csv"
	}

	outfh, _ := xopen.Wopen(input.OutFile)
	defer outfh.Close()

	writer := csv.NewWriter(outfh)

	key2data := make(map[string][]string, 10000)
	orders := make(map[string]int, 10000)

	file := fps
	csvReader, _ := NewCSVReader(string(file), 4096, 4096)
	csvReader.Run()

	var colnames2fileds map[string]int // column name -> field
	var colnamesMap map[string]*regexp.Regexp

	checkFields := true
	var items []string
	var key string
	var N int
	var ok bool

	printMetaLine := true
	for chunk := range csvReader.Ch {

		if printMetaLine && len(csvReader.MetaLine) > 0 {
			outfh.WriteString(fmt.Sprintf("sep=%s\n", string(writer.Comma)))
			printMetaLine = false
		}

		parseHeaderRow := true
		for _, record := range chunk.Data {
			N++
			if parseHeaderRow { // parsing header row
				colnames2fileds = make(map[string]int, len(record))
				for i, col := range record {
					colnames2fileds[col] = i + 1
				}
				colnamesMap = make(map[string]*regexp.Regexp, len(colnames))
				i := 0
				for _, col := range colnames {

					colnamesMap[col] = fuzzyField2Regexp(col)
					colnamesOrder[col] = i
					i++
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
			}
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

			key = strings.Join(items[0:len(items)-1], "_shenwei356_")
			if _, ok = key2data[key]; !ok {
				key2data[key] = make([]string, 0, 1)
			}
			key2data[key] = append(key2data[key], items[len(items)-1])
			orders[key] = N
		}
	}

	orderedKey := stringutil.SortCountOfString(orders, false)
	for _, o := range orderedKey {
		items = strings.Split(o.Key, "_shenwei356_")
		items = append(items, strings.Join(key2data[o.Key], separater))
		writer.Write(items)
	}

	writer.Flush()
	return Files(input.OutFile)

}
