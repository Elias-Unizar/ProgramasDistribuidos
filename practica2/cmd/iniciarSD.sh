go run escritor/escritor.go 1 users.txt file1 &
go run escritor/escritor.go 2 users.txt file2 &
go run escritor/escritor.go 3 users.txt file3 &
go run lector/lector.go 4 users.txt file4 2> log4.txt &