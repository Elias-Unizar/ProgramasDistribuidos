#!/usr/bin/env sh
ssh rasp1 "cd sd/practica1/cmd/master-worker/ && go run master/main.go :29150 worker.txt barrera.txt 1"
ssh rasp2 "cd sd/practica1/cmd/master-worker/ && go run worker/main.go worker.txt barrera.txt 2"
ssh rasp3 "cd sd/practica1/cmd/master-worker/ && go run worker/main.go worker.txt barrera.txt 3"
ssh rasp4 "cd sd/practica1/cmd/master-worker/ && go run worker/main.go worker.txt barrera.txt 4"