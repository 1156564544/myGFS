package master

import (
	"testing"
)

func TestNamespace_manager_CreateFile(t *testing.T) {
	ns := namespace_manager{&dirTree{children: make(map[string]*dirTree), isDir: true}}
	err := ns.CreateFile("/hello")
	if err != nil {
		t.Errorf("Create file error: %v", err.Error())
	}
	files, err := ns.List("/")
	if err != nil {
		t.Errorf("List files error: %v", err.Error())
	}
	if len(files) != 1 || files[0] != "hello" {
		t.Errorf("New file is not in the namespace")
	}
}

func TestNamespace_manager_Mkdir(t *testing.T) {
	ns := namespace_manager{&dirTree{children: make(map[string]*dirTree), isDir: true}}
	err := ns.Mkdir("/dir")
	if err != nil {
		t.Errorf("Make directory error")
	}
	dirs, err := ns.List("/")
	if err != nil {
		t.Errorf("List dirs error: %v", err.Error())
	}
	if len(dirs) != 1 || dirs[0] != "dir" {
		t.Errorf("New file is not in the namespace")
	}
}

func Test_CreateFile_after_Mkdir(t *testing.T) {
	ns := namespace_manager{&dirTree{children: make(map[string]*dirTree), isDir: true}}
	err := ns.Mkdir("/dir")
	if err != nil {
		t.Errorf("Make directory error")
	}
	err = ns.CreateFile("/dir/hello")
	if err != nil {
		t.Errorf("Create file error: %v", err.Error())
	}
	files, err := ns.List("/dir")
	if err != nil {
		t.Errorf("List files error: %v", err.Error())
	}
	if len(files) != 1 || files[0] != "hello" {
		t.Errorf("New file is not in the namespace")
	}
}

func Test_MKdir_after_Mkdir(t *testing.T) {
	ns := namespace_manager{&dirTree{children: make(map[string]*dirTree), isDir: true}}
	err := ns.Mkdir("/dir")
	if err != nil {
		t.Errorf("Make directory error")
	}
	err = ns.Mkdir("/dir/dir2")
	if err != nil {
		t.Errorf("Make directory after make directory error")
	}
	dirs, err := ns.List("/dir")
	if err != nil {
		t.Errorf("List dirs error: %v", err.Error())
	}
	if len(dirs) != 1 || dirs[0] != "dir2" {
		t.Errorf("New directroy is not in the namespace")
	}
}

func Test_Create_and_MKdir(t *testing.T) {
	ns := namespace_manager{&dirTree{children: make(map[string]*dirTree), isDir: true}}
	err := ns.CreateFile("/hello")
	if err != nil {
		t.Errorf("Create file error: %v", err.Error())
	}
	err = ns.Mkdir("/dir")
	if err != nil {
		t.Errorf("Create directory error: %v", err.Error())
	}
	dirs, err := ns.List("/")
	if err != nil {
		t.Errorf("List dirs error: %v", err.Error())
	}
	if len(dirs) != 2 {
		t.Errorf("New directroy or file is not in the namespace")
	}
}

func TestNamespace_manager_DeletePath(t *testing.T) {
	ns := namespace_manager{&dirTree{children: make(map[string]*dirTree), isDir: true}}
	err := ns.Mkdir("/dir")
	if err != nil {
		t.Errorf("Create directory error: %v", err.Error())
	}
	err = ns.CreateFile("/dir/hello")
	if err != nil {
		t.Errorf("Create file error: %v", err.Error())
	}
	err = ns.DeletePath("/dir/hello")
	if err != nil {
		t.Errorf("Delete file error: %v", err.Error())
	}
	err = ns.DeletePath("/dir")
	if err != nil {
		t.Errorf("Delete directory error: %v", err.Error())
	}
	dirs, err := ns.List("/")
	if err != nil {
		t.Errorf("List fail!")
	}
	if len(dirs) != 0 {
		t.Errorf("Delete fail")
	}
}

func Test_Serial_and_Deserial(t *testing.T) {
	ns := namespace_manager{&dirTree{children: make(map[string]*dirTree), isDir: true}}
	err := ns.Mkdir("/dir")
	if err != nil {
		t.Errorf("Create directory error: %v", err.Error())
	}
	err = ns.CreateFile("/dir/hello")
	if err != nil {
		t.Errorf("Create file error: %v", err.Error())
	}
	ns.Serial()
	ns.Deserial()
	dirs, err := ns.List("/dir")
	if err != nil {
		t.Errorf("List dirs error: %v", err.Error())
	}
	if len(dirs) != 1 || dirs[0] != "hello" {
		t.Errorf("New file is not in the namespace")
	}
}
