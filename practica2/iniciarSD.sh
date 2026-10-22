#!/usr/bin/env sh
ssh -n rasp1 "cd sd/practica2/ && bin/escritor 1 users.txt fileConc1" 2> log1.txt &
ssh -n rasp2 "cd sd/practica2/ && bin/lector 2 users.txt fileConc2" 2> log2.txt &
ssh -n rasp3 "cd sd/practica2/ && bin/lector 3 users.txt fileConc" 2> log3.txt &
ssh -n rasp4 "cd sd/practica2/ && bin/escritor 4 users.txt fileConcAunqueseLlameDiferente" 2> log4.txt &
wait
echo "Finalizado"