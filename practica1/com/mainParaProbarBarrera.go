package main

import (
	"fmt"
	"net"
	"os"
	"practica1/com"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: go run main.go <endpoints_file> <line_number>")
	} else {
		fileName := os.Args[1]
		lineNumber, err := strconv.Atoi(os.Args[2])
		com.CheckError(err)
		connChan := make(chan net.Conn)

		barrierListener, ipsBarrera, err := com.InicializarVariablesBarrera(fileName, lineNumber)
		com.CheckError(err)
		go com.EscucharPuertoBarrera(barrierListener, connChan)
		com.ActivarBarrera(barrierListener, ipsBarrera, lineNumber, connChan)

		fmt.Println("YA HE PASADO LA PRIMERA BARRERAAAAAA")
		time.Sleep(3 * time.Second)
		com.ActivarBarrera(barrierListener, ipsBarrera, lineNumber, connChan)
		fmt.Println("YA HE PASADO LA SEGUNDA BARRERAAAAAA")
		time.Sleep(3 * time.Second)
		com.ActivarBarrera(barrierListener, ipsBarrera, lineNumber, connChan)
		fmt.Println("YA HE PASADO LA ULTIMA BARRERAAAAAA")

		barrierListener.Close()
	}
}
