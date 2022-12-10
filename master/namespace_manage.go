package master

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

type namespace_manager struct {
	root *dirTree
}

type dirTree struct {
	sync.RWMutex
	children map[string]*dirTree
	isDir    bool

	// If it is a file
	size   int64
	nChunk int
}

func (ns *namespace_manager) LockParent(path string, down bool) (*dirTree, error) {
	ps := strings.Split(path, "/")
	ps = ps[1:]
	cmd := ns.root
	if len(ps) > 0 {
		for i, node := range ps {
			cmd.RLock()
			if next, ok := cmd.children[node]; !ok {
				return nil, fmt.Errorf("%v not in namespace!", path)
			} else if i < len(ps)-1 || down {
				cmd = next
			}
		}
	}
	return cmd, nil
}

func (ns *namespace_manager) UnlockParent(path string) {
	ps := strings.Split(path, "/")
	ps = ps[1:]
	cmd := ns.root
	if len(ps) > 0 {
		for _, node := range ps {
			cmd.RUnlock()
			if next, ok := cmd.children[node]; !ok {
				log.Fatalf("%v not in namespace!", path)
				return
			} else {
				cmd = next
			}
		}
	}
}

func splitPath(path string) (string, string) {
	i := len(path) - 1
	for ; i >= 0 && path[i] != '/'; i-- {

	}
	return path[:i], path[i+1:]
}

func (ns *namespace_manager) CreateFile(path string) error {
	dirPath, fileName := splitPath(path)
	dirNode, err := ns.LockParent(dirPath, true)
	defer ns.UnlockParent(dirPath)
	if err != nil {
		return err
	}
	dirNode.Lock()
	defer dirNode.Unlock()
	if _, ok := dirNode.children[fileName]; ok {
		log.Printf("File %v is exist!\n", path)
		return fmt.Errorf("File %v is exist!", path)
	}
	dirNode.children[fileName] = &dirTree{
		isDir: false,
	}
	return nil
}

func (ns *namespace_manager) DeletePath(path string) error {
	dirPath, fileName := splitPath(path)
	dirNode, err := ns.LockParent(dirPath, true)
	defer ns.UnlockParent(dirPath)
	if err != nil {
		return err
	}
	dirNode.Lock()
	defer dirNode.Unlock()
	if _, ok := dirNode.children[fileName]; !ok {
		return fmt.Errorf("File %v is not exist!", path)
	}
	delete(dirNode.children, fileName)
	return nil
}

func (ns *namespace_manager) Mkdir(path string) error {
	dirName, fileName := splitPath(path)
	dirNode, err := ns.LockParent(dirName, true)
	defer ns.UnlockParent(dirName)
	if err != nil {
		return err
	}
	dirNode.Lock()
	defer dirNode.Unlock()
	if _, ok := dirNode.children[fileName]; ok {
		log.Printf("Directory %v is exist!\n", path)
		return fmt.Errorf("Directory %v is exist!", path)
	}
	dirNode.children[fileName] = &dirTree{
		children: make(map[string]*dirTree),
		isDir:    true,
	}
	return nil
}

func (ns *namespace_manager) List(path string) (files []string, e error) {
	cmd := ns.root
	if path == "/" {
		cmd = ns.root
	} else {
		cmd, e = ns.LockParent(path, true)
		if e != nil {
			return
		}
		defer ns.UnlockParent(path)
	}
	cmd.RLock()
	defer cmd.RUnlock()
	for file, _ := range cmd.children {
		files = append(files, file)
	}
	return
}

type serialTreeNode struct {
	IsDir    bool
	Children map[string]int
	NChunk   int
	Size     int64
}

type serialTree struct {
	Nodes     []serialTreeNode
	SerialCnt int
}

func tree2node(dTree *dirTree, sTree *serialTree) int {
	node := serialTreeNode{dTree.isDir, map[string]int{}, dTree.nChunk, dTree.size}
	sTree.Nodes = append(sTree.Nodes, node)
	rootCnt := sTree.SerialCnt
	sTree.SerialCnt += 1
	for k, v := range dTree.children {
		sTree.Nodes[rootCnt].Children[k] = tree2node(v, sTree)
	}
	return rootCnt
}

func node2tree(sTree *serialTree, cnt int) *dirTree {
	root := dirTree{}
	node := sTree.Nodes[cnt]
	root.isDir = node.IsDir
	root.size = node.Size
	root.nChunk = node.NChunk
	root.children = make(map[string]*dirTree)
	for k, v := range node.Children {
		root.children[k] = node2tree(sTree, v)
	}
	return &root
}

func (ns *namespace_manager) Serial() {
	sTree := serialTree{make([]serialTreeNode, 0), 0}
	tree2node(ns.root, &sTree)
	//log.Println(sTree)
	b, e := json.Marshal(sTree)
	if e != nil {
		log.Fatalf("Encoding error: %v", e.Error())
		return
	}
	//log.Println(string(b))
	f, e := os.Create("namespace_manage.txt")
	if e != nil {
		log.Fatalf("Create fail error: %v", e.Error())
	}
	f.Write(b)
}

func (ns *namespace_manager) Deserial() {
	f, e := os.Open("namespace_manage.txt")
	if e != nil {
		log.Fatalf("Open file error: %v", e.Error())
	}
	b, _ := ioutil.ReadAll(f)
	//log.Println(string(b))
	sTree := serialTree{}
	json.Unmarshal(b, &sTree)
	//log.Println(sTree)
	root := node2tree(&sTree, 0)
	ns.root = root
}
