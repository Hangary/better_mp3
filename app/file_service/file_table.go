package file_service

import (
	"fmt"
	"github.com/emirpasic/gods/maps/treemap"
	"k8s.io/apimachinery/pkg/util/sets"
	"log"
	"net/rpc"
	"strings"
	"time"
)

type FileTable struct {
	Storage    treemap.Map //virtual ring
	fileServer *FileServer
	latest     map[string]int64
}

type FileTableEntry struct {
	ServerIP string
	files    []string
}

func NewFileTable(fs *FileServer) FileTable {
	var tb FileTable
	tb.fileServer = fs
	tb.Storage = *treemap.NewWith(compare)
	tb.AddEmptyEntry(fs.ms.SelfIP)
	tb.latest = map[string]int64{}
	MyHash = hash(fs.ms.SelfIP)
	return tb
}

/*
	Monitor member list
 */
func (fs *FileServer) RunDaemon() {
	for {
		select {
			case joinedNode := <- fs.ms.JoinedNodeChan:
				fmt.Println("FileTable: node joined", joinedNode)
				fs.FileTable.AddEmptyEntry(joinedNode)
			case failedNode := <- fs.ms.FailedNodeChan:
				fmt.Println("FileTable: node left", failedNode)
				fs.FileTable.RemoveFromTable(fs.ms.GetFailedMemberIPList())
		}
	}
}

func (t *FileTable) AddEmptyEntry(ip string) {
	pos := hash(ip)
	t.Storage.Put(pos, FileTableEntry{ServerIP: ip, files: []string{}})
}

// remove failed nodes from fileTable
func (t *FileTable) RemoveFromTable(failed []string) {
	for _, ip := range failed {
		curHash := hash(ip)

		nextAlive := t.findNextAlive(failed, curHash)
		secAlive := t.findNextAlive(failed, nextAlive)
		thrAlive := t.findNextAlive(failed, secAlive)
		forAlive := t.findNextAlive(failed, thrAlive)

		// only nextAlive handles the re-replication
		if MyHash == nextAlive {
			//fmt.Println(nextAlive, secAlive, thrAlive, forAlive)
			n0s, _ := t.Storage.Get(curHash) // failed node
			n1s, _ := t.Storage.Get(nextAlive)
			n2s, _ := t.Storage.Get(secAlive)
			n3s, _ := t.Storage.Get(thrAlive)
			n4s, _ := t.Storage.Get(forAlive)

			i1 := n1s.(FileTableEntry).ServerIP
			i2 := n2s.(FileTableEntry).ServerIP
			i3 := n3s.(FileTableEntry).ServerIP
			i4 := n4s.(FileTableEntry).ServerIP
			//fmt.Println(i1, i2, i3, i4)

			f0 := sets.NewString()
			for _, f := range n0s.(FileTableEntry).files {
				f0.Insert(f)
			}
			f1 := sets.NewString()
			for _, f := range n1s.(FileTableEntry).files {
				f1.Insert(f)
			}
			f2 := sets.NewString()
			for _, f := range n2s.(FileTableEntry).files {
				f2.Insert(f)
			}
			f3 := sets.NewString()
			for _, f := range n3s.(FileTableEntry).files {
				f3.Insert(f)
			}

			tmp1 := f0.Intersection(f1)
			tmp2 := tmp1.Intersection(f2)
			to1 := f0.Difference(f1)
			to2 := tmp1.Difference(f2)
			to3 := tmp2.Difference(f3)
			to4 := tmp2.Intersection(f3)
			//fmt.Println(to1, to2, to3, to4)

			// nextAlive
			var success bool
			for filename := range to1 {
				err := t.fileServer.LocalReplicate(filename, &success)
				if err != nil {
					log.Println(err)
					continue
				}
			}

			// secAlive
			port := t.fileServer.config.Port
			client, err := rpc.Dial("tcp", i2+":"+port)
			if err != nil {
				log.Println(err)
			} else {
				for filename := range to2 {
					err = client.Call("FileRPCServer.LocalReplicate", filename, &success)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}

			// thrAlive
			client, err = rpc.Dial("tcp", i3+":"+port)
			if err != nil {
				log.Println(err)
			} else {
				for filename := range to3 {
					err = client.Call("FileRPCServer.LocalReplicate", filename, &success)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}

			// forAlive
			client, err = rpc.Dial("tcp", i4+":"+port)
			if err != nil {
				log.Println(err)
			} else {
				for filename := range to4 {
					err = client.Call("FileRPCServer.LocalReplicate", filename, &success)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}

			t.Storage.Remove(curHash)

			//fmt.Println(failed)
			// inform each member to update fileTable
			for _, v := range t.Storage.Values() {
				p := v.(FileTableEntry).ServerIP
				if !contains(failed, p) {
					if p == i1 {
						_ = t.PutRepEntry(map[uint32][]string{
							nextAlive: to1.List(),
							secAlive:  to2.List(),
							thrAlive:  to3.List(),
							forAlive:  to4.List(),
						}, &success)
					} else {
						client, err := rpc.Dial("tcp", p+":"+port)
						if err != nil {
							log.Println(err)
							continue
						}
						err = client.Call("FileRPCServer.PutRepEntry", map[uint32][]string{
							nextAlive: to1.List(),
							secAlive:  to2.List(),
							thrAlive:  to3.List(),
							forAlive:  to4.List(),
						}, &success)
					}
				}
			}
		} else {
			t.Storage.Remove(curHash)
		}
	}
}


func (t *FileTable) PutEntry(sdfs string, success *bool) error {
	t.latest[sdfs] = time.Now().UnixNano()
	floorKey, _ := t.Storage.Floor(hash(sdfs))

	if floorKey == nil {
		f, _ := t.Storage.Max()
		floorKey = f
	}
	//fmt.Println(hash(sdfs))
	//fmt.Println(floorKey)
	next := floorKey
	for i := 0; i < 4; i++ {
		next, _ = t.Storage.Ceiling(next)
		if next == nil {
			f, _ := t.Storage.Min()
			next = f
		}
		v, found := t.Storage.Get(next)
		if found {
			tmp := v.(FileTableEntry)
			if !contains(tmp.files, sdfs) {
				tmp.files = append(tmp.files, sdfs)
			}
			t.Storage.Put(next, tmp)
		}

		next = next.(uint32) + 1
	}
	return nil
}

func (t *FileTable) DeleteEntry(sdfs string, success *bool) error {
	for _, k := range t.Storage.Keys() {
		s, found := t.Storage.Get(k)
		if found {
			tmp := s.(FileTableEntry)
			for i, file := range tmp.files {
				if file == sdfs {
					tmp.files = append(tmp.files[:i], tmp.files[i+1:]...)
					fmt.Println("File entry for", sdfs, "deleted from", tmp.ServerIP)
				}
			}
			t.Storage.Put(k, tmp)
		}
	}
	return nil
}

// search for ips that has file
func (t *FileTable) search(sdfs string) []string {
	hashVal := hash(sdfs)
	floorKey, _ := t.Storage.Floor(hashVal)
	if floorKey == nil {
		f, _ := t.Storage.Max()
		floorKey = f
	}
	next := floorKey
	var ips []string
	for i := 0; i < 4; i++ {
		next, _ = t.Storage.Ceiling(next)
		if next == nil {
			f, _ := t.Storage.Min()
			next = f
		}
		tmp, found := t.Storage.Get(next)
		if found {
			ips = append(ips, tmp.(FileTableEntry).ServerIP)
		}
		next = next.(uint32) + 1
	}
	return ips
}

func (t *FileTable) PutRepEntry(args map[uint32][]string, success *bool) error {
	for k, extend := range args {
		v, found := t.Storage.Get(k)
		if found {
			tmp := v.(FileTableEntry)
			tmp.files = append(tmp.files, extend...)
			t.Storage.Put(k, tmp)
		}
	}
	return nil
}

func (t *FileTable) findNextAlive(failed []string, curHash uint32) uint32 {
	var hashed []uint32 // hashed list of failed nodes
	for _, k := range failed {
		hashed = append(hashed, hash(k))
	}
	floorKey, _ := t.Storage.Floor(curHash)
	if floorKey == nil {
		f, _ := t.Storage.Max()
		floorKey = f
	}
	next := floorKey
	var alive uint32 // first alive node after failed node
	for {
		next = next.(uint32) + 1
		next, _ = t.Storage.Ceiling(next)
		if next == nil {
			f, _ := t.Storage.Min()
			next = f
		}

		if !containsInt(hashed, next.(uint32)) {
			alive = next.(uint32)
			break
		}
	}
	return alive
}

func (t *FileTable) ListFilesByPrefix(prefix string) []string {
	fileset := sets.NewString()
	for _, s := range t.Storage.Values() {
		for _, f := range s.(FileTableEntry).files {
			if strings.HasPrefix(f, prefix) {
				fileset.Insert(f)
			}
		}
	}
	return fileset.UnsortedList()
}

func (t *FileTable) ListAllFiles() {
	for i, s := range t.Storage.Values() {
		for _, f := range s.(FileTableEntry).files {
			fmt.Println(i, s.(FileTableEntry).ServerIP, f)
		}
	}
}

func (t *FileTable) ListMyFiles() {
	v, found := t.Storage.Get(MyHash)
	if found {
		for _, rec := range v.(FileTableEntry).files {
			fmt.Println(rec)
		}
	} else {
		log.Fatal("Failed to list my files")
	}
}

func (t *FileTable) ListLocations(filename string) []string {
	var locations []string
	for _, s := range t.Storage.Values() {
		for _, file := range s.(FileTableEntry).files {
			if file == filename {
				locations = append(locations, s.(FileTableEntry).ServerIP)
			}
		}
	}
	return locations
}