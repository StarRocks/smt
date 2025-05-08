package common

import (
	"bytes"
	"encoding/gob"
	"regexp"

	"github.com/dlclark/regexp2"
)

func LongestCommonXfix(strs []string, pre bool) string {
	if len(strs) == 0 {
		return ""
	}
	xfix := strs[0]
	if len(strs) == 1 {
		return xfix
	}
	for _, str := range strs[1:] {
		xfixl := len(xfix)
		strl := len(str)
		if xfixl == 0 || strl == 0 {
			return ""
		}
		maxl := xfixl
		if strl < maxl {
			maxl = strl
		}
		if pre {
			for i := 0; i < maxl; i++ {
				if xfix[i] != str[i] {
					xfix = xfix[:i]
					break
				}
			}
		} else {
			for i := 0; i < maxl; i++ {
				xi := xfixl - i - 1
				si := strl - i - 1
				if xfix[xi] != str[si] {
					xfix = xfix[xi+1:]
					break
				}
			}
		}
	}
	return xfix
}

func DeepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func RegMatchString(pattern, str string) bool {
	reDB, err := regexp.Compile(pattern)
	if err != nil {
		matched, err := regexp2.MustCompile(pattern, 0).MatchString(str)
		return err == nil && matched
	}
	return reDB.Match([]byte(str))
}
