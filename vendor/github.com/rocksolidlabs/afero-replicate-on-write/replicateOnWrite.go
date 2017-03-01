package aferorepwr

import (
	"os"
	"time"

	"github.com/rocksolidlabs/afero"
)

// The ReplicateOnWriteFs is a filesystem: this file replicates all
// write action to the layer
//
// Reading directories is currently only supported via Open(), not OpenFile().
type ReplicateOnWriteFs struct {
	base  afero.Fs
	layer afero.Fs
}

func NewReplicateOnWriteFs(base afero.Fs, layer afero.Fs) afero.Fs {
	return &ReplicateOnWriteFs{base: base, layer: layer}
}

func (u *ReplicateOnWriteFs) Chtimes(name string, atime, mtime time.Time) error {
	b, err := u.base.Stat(name)
	if err != nil {
		return err
	}
	if b != nil {
		u.base.Chtimes(name, atime, mtime)
		u.layer.Chtimes(name, atime, mtime)
	}
	return nil
}

func (u *ReplicateOnWriteFs) Chmod(name string, mode os.FileMode) error {
	b, err := u.base.Stat(name)
	if err != nil {
		return err
	}
	if b != nil {
		u.base.Chmod(name, mode)
		u.layer.Chmod(name, mode)
	}
	return nil
}

func (u *ReplicateOnWriteFs) Stat(name string) (os.FileInfo, error) {
	fi, err := u.base.Stat(name)
	if err != nil {
		return nil, err
	}
	return fi, nil
}

func (u *ReplicateOnWriteFs) Rename(oldname, newname string) error {
	b, err := u.base.Stat(oldname)
	if err != nil {
		return err
	}
	if b != nil {
		u.base.Rename(oldname, newname)
		u.layer.Rename(oldname, newname)
	}
	return nil
}

func (u *ReplicateOnWriteFs) Remove(name string) error {
	u.base.Remove(name)
	u.layer.Remove(name)
	return nil
}

func (u *ReplicateOnWriteFs) RemoveAll(name string) error {
	u.base.RemoveAll(name)
	u.layer.RemoveAll(name)
	return nil
}

func (u *ReplicateOnWriteFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	b, err := u.base.Stat(name)
	if err != nil {
		bf, err := u.Create(name)
		if err != nil {
			return nil, err
		}
		return bf, nil
	}

	if b != nil {
		bf, err := u.base.OpenFile(name, flag, perm)
		if err != nil {
			bf.Close()
			return nil, err
		}
		lf, err := u.base.OpenFile(name, flag, perm)
		if err != nil {
			lf.Close()
			return nil, err
		}
		return &UnionFile{base: bf, layer: lf}, nil
	}
	return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
}

func (u *ReplicateOnWriteFs) Open(name string) (afero.File, error) {
	b, err := u.base.Stat(name)
	if err != nil {
		return nil, err
	}

	if b != nil {
		bf, err := u.base.Open(name)
		if err != nil {
			bf.Close()
			return nil, err
		}
		lf, err := u.base.Open(name)
		if err != nil {
			lf.Close()
			return nil, err
		}
		return &UnionFile{base: bf, layer: lf}, nil
	}

	return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrNotExist}
}

func (u *ReplicateOnWriteFs) Mkdir(name string, perm os.FileMode) error {
	_, err := afero.IsDir(u.base, name)
	if err != nil {
		u.base.MkdirAll(name, perm)
		u.layer.MkdirAll(name, perm)
	}
	return nil
}

func (u *ReplicateOnWriteFs) Name() string {
	return "ReplicateOnWriteFs"
}

func (u *ReplicateOnWriteFs) MkdirAll(name string, perm os.FileMode) error {
	_, err := afero.IsDir(u.base, name)
	if err != nil {
		u.base.MkdirAll(name, perm)
		u.layer.MkdirAll(name, perm)
	}
	return nil
}

func (u *ReplicateOnWriteFs) Create(name string) (afero.File, error) {
	bf, err := u.base.Create(name)
	if err != nil {
		return nil, err
	}
	lf, err := u.layer.Create(name)
	if err != nil {
		bf.Close()
		return nil, err
	}
	return &UnionFile{base: bf, layer: lf}, nil
}
