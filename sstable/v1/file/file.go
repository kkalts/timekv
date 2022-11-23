package file

/*
	sst文件相关字段 结构体
*/
type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}
