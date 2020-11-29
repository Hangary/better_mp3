#!/bin/bash
git pull
go get k8s.io/apimachinery/pkg/util/sets
go get gopkg.in/yaml.v2
go get github.com/emirpasic/gods/maps/treemap
bash clean.sh
go run ./app/*.go