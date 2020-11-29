package file_service

import "hash/fnv"

func compare(a, b interface{}) int {
	if a.(uint32) < b.(uint32) {
		return -1
	} else if a.(uint32) > b.(uint32) {
		return 1
	} else {
		return 0
	}
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func containsInt(s []uint32, e uint32) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32() % 1000000007
}
