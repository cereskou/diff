package swap

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

//Cache - sqlite3 database
type Cache struct {
	sync.Mutex
	Error error
	db    *sql.DB
	ins   *sql.Stmt
}

//NewCache -
func NewCache() *Cache {
	dbase := &Cache{
		Error: nil,
	}
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		dbase.Error = err

		return dbase
	}
	dbase.db = db

	return dbase
}

//Close -
func (d *Cache) Close() error {
	return d.db.Close()
}

//DB -
func (d *Cache) DB() *sql.DB {
	return d.db
}

//Create -
func (d *Cache) Create(tablename string) error {
	sql := fmt.Sprintf(CREATETBL, tablename)
	_, err := d.DB().Exec(sql)
	if err != nil {
		return err
	}

	return nil
}

//Exec -
func (d *Cache) Exec(sql string) error {
	d.Lock()
	defer d.Unlock()
	_, err := d.db.Exec(sql)
	if err != nil {
		return err
	}

	return nil
}

//Query -
func (d *Cache) Query(tname string) error {
	sql := fmt.Sprintf("SELECT * FROM %v", tname)
	rows, err := d.db.Query(sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		f := File{}
		err = rows.Scan(&f.Path, &f.Size, &f.IsDir, &f.Md5)
		if err != nil {
			return err
		}
		fmt.Println(f.Path, f.Size)
	}

	return nil
}

//Sum -
func (d *Cache) Sum(sql string) (val int64, err error) {
	row := d.db.QueryRow(sql)
	err = row.Scan(&val)
	if err != nil {
		val = 0
		return
	}
	return
}

//CountAll -
func (d *Cache) CountAll(tname string) (count int, err error) {
	sql := fmt.Sprintf("select count(*) from %v", tname)
	rows := d.db.QueryRow(sql)
	err = rows.Scan(&count)
	if err != nil {
		count = -1
		return
	}
	return
}

//Count -
func (d *Cache) Count(tname string) (count int, err error) {
	sql := fmt.Sprintf("select count(*) from %v where isdir=0", tname)
	rows := d.db.QueryRow(sql)
	err = rows.Scan(&count)
	if err != nil {
		count = -1
		return
	}
	return
}

//CountDir -
func (d *Cache) CountDir(tname string) (count int, err error) {
	sql := fmt.Sprintf("select count(*) from %v where isdir=1", tname)
	rows := d.db.QueryRow(sql)
	err = rows.Scan(&count)
	if err != nil {
		count = -1
		return
	}
	return
}

//CreateDiff -
func (d *Cache) CreateDiff(sname string, tname string) error {
	sql := `insert into DIFF select * from (
		select 0 as kb,"path",filesize,isdir,md5 from $S m where not exists(select 1 from $T n where(m."path"=n."path" and m.filesize=n.filesize and m.isdir=n.isdir))
		union
		select 1 as kb,"path",filesize,isdir,md5 from $T n where not exists(select 1 from $S m where(m."path"=n."path" and m.filesize=n.filesize and m.isdir=n.isdir))
	)
	`
	sql = strings.ReplaceAll(sql, "$S", sname)
	sql = strings.ReplaceAll(sql, "$T", tname)

	return d.Exec(sql)
}

//CreateDiffCount -
func (d *Cache) CreateDiffCount() error {
	sql := `insert into diff_count select * from (
		select "path", count("path") from DIFF where isdir='0' group by "path" having count("path") > 1
		)`
	return d.Exec(sql)
}

//DiffCount -
func (d *Cache) DiffCount(sname string, tname string) (count int, err error) {
	sql := `select count(*) from (
		select "path",filesize,isdir,md5 from $S m where not exists(select 1 from $T n where(m."path"=n."path" and m.filesize=n.filesize and m.isdir=n.isdir))
		union
		select "path",filesize,isdir,md5 from $T n where not exists(select 1 from $S m where(m."path"=n."path" and m.filesize=n.filesize and m.isdir=n.isdir))
		)`
	sql = strings.ReplaceAll(sql, "$S", sname)
	sql = strings.ReplaceAll(sql, "$T", tname)

	rows := d.db.QueryRow(sql)
	err = rows.Scan(&count)
	if err != nil {
		count = -1
		return
	}
	return
}

//Diff -
func (d *Cache) Diff(output chan *Pair) (err error) {
	sqlx := `select df.*, dc.filecount from diff df left join diff_count dc on (df."path"=dc."path") order by "path"`
	rows, err := d.db.Query(sqlx)
	if err != nil {
		return err
	}
	defer rows.Close()

	fc := 0
	p := &Pair{
		S: nil,
		T: nil,
	}
	for rows.Next() {
		f := File{}
		var sc sql.NullInt32

		err = rows.Scan(&f.KB, &f.Path, &f.Size, &f.IsDir, &f.Md5, &sc)
		if err != nil {
			return
		}
		filecount := 0
		if sc.Valid {
			filecount = int(sc.Int32)
		}
		if filecount > 1 {
			if f.KB == 0 {
				p.S = &f
				fc++
			} else if f.KB == 1 {
				p.T = &f
				fc++
			}
			if fc == 2 {
				output <- p

				//clear
				fc = 0
				p = &Pair{
					S: nil,
					T: nil,
				}
			}
		} else {
			if f.KB == 0 {
				p.S = &f
				p.T = nil
			} else if f.KB == 1 {
				p.S = nil
				p.T = &f
			}

			output <- p

			//clear
			p = &Pair{
				S: nil,
				T: nil,
			}
		}
	}

	return
}

//DiffOutput -
func (d *Cache) DiffOutput(sname string, tname string, output chan *File) (err error) {
	sql := `
	select kb,"path",filesize,isdir,md5 from (
		select 0 as kb, "path",filesize,isdir,md5 from $S m where not exists(select 1 from $T n where(m."path"=n."path" and m.filesize=n.filesize and m.isdir=n.isdir))
		union
		select 1 as kb,"path",filesize,isdir,md5 from $T n where not exists(select 1 from $S m where(m."path"=n."path" and m.filesize=n.filesize and m.isdir=n.isdir))
) order by "path"`
	sql = strings.ReplaceAll(sql, "$S", sname)
	sql = strings.ReplaceAll(sql, "$T", tname)
	rows, err := d.db.Query(sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		f := File{}
		err = rows.Scan(&f.KB, &f.Path, &f.Size, &f.IsDir, &f.Md5)
		if err != nil {
			return
		}

		output <- &f
	}

	return
}
