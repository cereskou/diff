package swap

//const -
const (
	CREATETBL       string = "CREATE TABLE %v(path text, filesize integer, isdir integer, md5 text);"
	CREATEDIFF      string = "CREATE TABLE DIFF(kb integer,path text, filesize integer, isdir integer, md5 text);"
	CREATEDIFFCOUNT string = "CREATE TABLE DIFF_COUNT(path text, filecount integer);"
)

//File -
type File struct {
	KB    int
	Path  string
	Size  int64
	IsDir bool
	Md5   string
}

//Path -
type Path struct {
	Name  string
	Table string
}

//Pair -
type Pair struct {
	S *File
	T *File
}
