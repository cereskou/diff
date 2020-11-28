package main

import (
	"fmt"
	"os"
	"strings"

	"ditto.co.jp/diff/swap"
	"github.com/karrick/godirwalk"
)

//Scan -
type Scan struct {
	Path  string
	Table string
	cache *swap.Cache
}

//ScanDirectory -
func (s *Scan) ScanDirectory() error {
	offset := len(s.Path)
	//os path separator
	separator := string(os.PathSeparator)
	//walk local directory
	values := make([]string, 0)
	err := godirwalk.Walk(s.Path, &godirwalk.Options{
		Callback: func(name string, de *godirwalk.Dirent) error {
			//hidden file
			if name[0:1] == "." {
				return nil
			}
			st, err := os.Stat(name)
			if err != nil {
				return err
			}
			//root -> skip
			if name == s.Path || name+separator == s.Path {
				return nil
			}
			//相対パス取得
			relative := name[offset:]

			//insert to table
			f := &swap.File{
				Path:  relative,
				Size:  st.Size(),
				IsDir: st.IsDir(),
				Md5:   "",
			}
			dir := 0
			if f.IsDir {
				dir = 1
			}
			val := fmt.Sprintf("(%q,%v,%v,%q)", f.Path, f.Size, dir, f.Md5)
			values = append(values, val)
			if len(values) > 100 {
				sql := fmt.Sprintf("insert into %v(path,filesize,isdir,md5) values", s.Table)
				sql += strings.Join(values, ",")
				err := s.cache.Exec(sql)
				if err != nil {
					return err
				}
				values = make([]string, 0)
			}

			return nil
		},
		ErrorCallback: func(name string, err error) godirwalk.ErrorAction {
			if err.Error() == "UserBreak" {
				return godirwalk.Halt
			}

			return godirwalk.SkipNode
		},
		Unsorted: true,
	})
	if err != nil {
		return err
	}
	if len(values) > 0 {
		sql := fmt.Sprintf("insert into %v(path,filesize,isdir,md5) values", s.Table)
		sql += strings.Join(values, ",")
		err := s.cache.Exec(sql)
		if err != nil {
			return err
		}
	}

	return nil
}
