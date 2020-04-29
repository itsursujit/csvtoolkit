package csvtk

import (
	"regexp"
	"strings"
)

func fuzzyField2Regexp(field string) *regexp.Regexp {
	if strings.IndexAny(field, "*") >= 0 {
		field = strings.Replace(field, "*", ".*?", -1)
	}

	field = "^" + field + "$"
	re, _ := regexp.Compile(field)
	//checkError(err)
	return re
}
