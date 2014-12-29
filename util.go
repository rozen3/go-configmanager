package conf

import (
	"os"
	"io"
	"path/filepath"
)

func readFileAll (file *os.File) ([]byte, error) {
	file.Seek(0, 0)

	s := make([]byte, 0)
	for {
		buffSize := 512 * 1024 // 512KB
		buff := make([]byte, buffSize)
		n, err := file.Read(buff)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return s, err
			}
		} else if n < buffSize {
			s = append(s, buff[:n]...)
			break
		}else {
			s = append(s, buff...)
		}
	}

	return s, nil
}

func removeFiles(path string) error {
	pattern := filepath.Join(path, "*.data")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, file := range files {
		os.Remove(file)
	}

	return nil
}

func removeIndexs(path string) error {
	pattern := filepath.Join(path, "*.idx")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	for _, file := range files {
		os.Remove(file)
	}

	return nil
}

func removeAll(path string) {
	removeFiles(path)
	removeIndexs(path)
}


