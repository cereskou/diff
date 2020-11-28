package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"ditto.co.jp/diff/swap"
	"ditto.co.jp/diff/utils"
	"github.com/dustin/go-humanize"
	"github.com/jessevdk/go-flags"
	_ "github.com/mattn/go-sqlite3"
)

//options -
type options struct {
	Source  string `short:"s" long:"source" description:"source directory"`
	Target  string `short:"t" long:"target" description:"target directory"`
	Output  string `short:"o" long:"output" description:"output file"`
	Version bool   `short:"v" long:"version" description:"show version"`
}

func main() {
	var opts options
	if cmds, err := flags.Parse(&opts); err != nil {
		os.Exit(-1)
	} else {
		if len(cmds) > 0 {
			opts.Source = cmds[0]
			if len(cmds) > 1 {
				opts.Target = cmds[1]
			}
		}
	}

	if opts.Version {
		fmt.Println("diff - compare directories")
		os.Exit(0)
	}
	if !utils.Exists(opts.Source) {
		fmt.Printf("%v not found.\n", opts.Source)
		os.Exit(-1)
	}
	if !utils.Exists(opts.Target) {
		fmt.Printf("%v not found.\n", opts.Target)
		os.Exit(-1)
	}

	if opts.Source == opts.Target {
		fmt.Println("Disallow self compare")
		os.Exit(-1)
	}

	cache := swap.NewCache()
	if cache.Error != nil {
		fmt.Println(cache.Error)
		os.Exit(-1)
	}
	defer cache.Close()

	scans := make([]*swap.Path, 0)
	sp := &swap.Path{
		Name:  opts.Source,
		Table: "S",
	}
	tp := &swap.Path{
		Name:  opts.Target,
		Table: "T",
	}
	err := cache.Create(sp.Table)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	err = cache.Create(tp.Table)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	scans = append(scans, sp)
	scans = append(scans, tp)
	//create diff table
	err = cache.Exec(swap.CREATEDIFF)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	//create diff_count
	err = cache.Exec(swap.CREATEDIFFCOUNT)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	fmt.Println("scaning...")
	wg := new(sync.WaitGroup)
	for i, f := range scans {
		wg.Add(1)

		go func(num int, n *swap.Path) {
			defer wg.Done()

			scan := &Scan{
				Path:  n.Name,
				Table: n.Table,
				cache: cache,
			}
			err := scan.ScanDirectory()
			if err != nil {
				fmt.Println(err)
			}

		}(i, f)
	}
	wg.Wait()
	fmt.Println("scan end")

	fmt.Println(opts.Source)
	err = showDetails("S", cache)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	fmt.Println(opts.Target)
	err = showDetails("T", cache)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	fmt.Println("comparing...")

	err = cache.CreateDiff("S", "T")
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	err = cache.CreateDiffCount()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	count, err := cache.CountAll("DIFF")
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
	if count > 0 {
		fmt.Println("two directories are NOT identical.")
	} else {
		fmt.Println("two directories are identical.")
	}

	if opts.Output != "" {
		f, err := os.Create(opts.Output)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		defer f.Close()
		fmt.Printf("Diff file: %v\n", count)
		fmt.Printf("write to %v", opts.Output)

		//UTF-8 BOM
		f.Write([]byte{0xEF, 0xBB, 0xBF})

		w := csv.NewWriter(f)
		end := make(chan int)
		files := make(chan *swap.Pair)
		go func() {
			lines := make([][]string, 0)
			for fi := range files {
				line := make([]string, 0)
				l := make([]string, 0)
				r := make([]string, 0)
				if fi.S != nil {
					path := ""
					path = filepath.Join(opts.Source, fi.S.Path)
					//path
					l = append(l, path)
					//size
					l = append(l, fmt.Sprintf("%v", fi.S.Size))
					//dir
					l = append(l, fmt.Sprintf("%v", fi.S.IsDir))
				} else {
					l = append(l, "")
					l = append(l, "")
					l = append(l, "")
				}

				if fi.T != nil {
					path := ""
					path = filepath.Join(opts.Target, fi.T.Path)
					//path
					r = append(r, path)
					//size
					r = append(r, fmt.Sprintf("%v", fi.T.Size))
					//dir
					r = append(r, fmt.Sprintf("%v", fi.T.IsDir))
				} else {
					r = append(r, "")
					r = append(r, "")
					r = append(r, "")
				}

				//left
				line = append(line, l...)
				//right
				line = append(line, r...)

				lines = append(lines, line)

				//output
				if len(lines) > 100 {
					w.WriteAll(lines)
					w.Flush()

					lines = make([][]string, 0)
				}
			}

			//output
			if len(lines) > 0 {
				w.WriteAll(lines)
				w.Flush()

				lines = make([][]string, 0)
			}

			end <- 1
		}()

		err = cache.Diff(files)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}

		close(files)
		//wait
		<-end
		fmt.Println(" ")
	}

	// files := make(chan *swap.File)
	// go func() {
	// 	err := cache.DiffOutput("S", "T", files)
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return
	// 	}
	// }()
	// close(files)

	fmt.Println("done")
}

func showDetails(name string, cache *swap.Cache) error {
	count, err := cache.Count(name)
	if err != nil {
		return err
	}
	dcount, err := cache.CountDir(name)
	if err != nil {
		return err
	}
	sql := "select sum(filesize) from " + name + " where isdir='0'"
	fsize, err := cache.Sum(sql)
	if err != nil {
		return err
	}
	fmt.Printf("  File Count: %v (Dir: %v)\n", count, dcount)
	fmt.Printf("  File Size : %v (%v)\n", humanize.IBytes(uint64(fsize)), humanize.Comma(fsize))

	return nil
}
