/*
Paquete creado por Elias Agustin Mihailov, para facilitar la implantacion del algortimo de barrera en cualquier codigo
para práctica futuras. He donado este código a otros compañeros, ya que funciona similar a lo que sería usar otros elementos
de internet para lograr el mismo proposito, y no interfiere con la compleción del código barrera entregable
*/
package com

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

func readEndpoints(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var endpoints []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			endpoints = append(endpoints, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return endpoints, nil
}

func handleConnection(conn net.Conn, barrierChan chan<- bool,
	received *map[string]bool, mu *sync.Mutex, n int) {
	defer conn.Close()
	buf := make([]byte, 1024)
	_, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading from connection:", err)
		return
	}
	msg := string(buf)
	mu.Lock()
	(*received)[msg] = true
	fmt.Println("Received ", len(*received), " elements")
	if len(*received) == n-1 {
		barrierChan <- true
	}
	mu.Unlock()
}

// Get enpoints (IP adresse:port for each distributed process)
func GetEndpoints(endpointsFile string, lineNumber int) ([]string, error) {
	var err error
	var endpoints []string // Por qué esta declaración ?, porque err ya se declara con := antes de usarlo junto con endpoints?
	if lineNumber < 1 {
		err = errors.New("Invalid line number")
		fmt.Println("Invalid line number")
	} else if endpoints, err = readEndpoints(endpointsFile); err != nil {
		fmt.Println("Error reading endpoints:", err)
	} else if lineNumber > len(endpoints) {
		fmt.Printf("Line number %d out of range\n", lineNumber)
		err = errors.New("Line number out of range")
	}

	return endpoints, err
}

func acceptAndHandleConnections(quitChannel chan bool, connChan chan net.Conn,
	barrierChan chan bool, receivedMap *map[string]bool, mu *sync.Mutex, n int) {
	salirCanal := false
	for salirCanal != true {
		select {
		case <-quitChannel:
			fmt.Println("Stopping the listener...")
			salirCanal = true
		case conn := <-connChan:
			go handleConnection(conn, barrierChan, receivedMap, mu, n)
		}
	}
}

func notifyOtherDistributedProcesses(endPoints []string, lineNumber int, monitorEnvio *sync.WaitGroup) {
	for i, ep := range endPoints {
		if i+1 != lineNumber { //Para no enviar un mensaje a uno mismo
			monitorEnvio.Add(1)
			go func(ep string) {
				defer monitorEnvio.Done()
				for {
					conn, err := net.Dial("tcp", ep)
					if err != nil {
						fmt.Println("Error connecting to", ep, ":", err)
						time.Sleep(5 * time.Second)
						continue
					}
					_, err = conn.Write([]byte(strconv.Itoa(lineNumber)))
					if err != nil {
						fmt.Println("Error sending message:", err)
						conn.Close()
						continue
					}
					conn.Close()
					break
				}
			}(ep)
		}
	}
}

func EscucharPuertoBarrera(listener net.Listener, connChan chan net.Conn) {
	var err error
	var conn net.Conn
	for err == nil {
		conn, err = listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		connChan <- conn
	}
}

func InicializarVariablesBarrera(file string, lineNumber int) (net.Listener, []string, error) {
	var listener net.Listener
	var endPoints []string
	var err error
	if endPoints, err = GetEndpoints(file, lineNumber); err == nil {
		localEndpoint := endPoints[lineNumber-1]
		if listener, err = net.Listen("tcp", localEndpoint); err != nil {
			fmt.Println("Error creating listener:", err)
		} else {
			fmt.Println("Listening on", localEndpoint)
		}
	} else {
		fmt.Println("Error al inicializar la barrera")
	}
	return listener, endPoints, err
}

func ActivarBarrera(listener net.Listener, endPoints []string, lineNumber int, connChan chan net.Conn) {
	// Barrier synchronization
	var mu sync.Mutex
	receivedMap := make(map[string]bool)
	barrierChan := make(chan bool)
	quitChannel := make(chan bool)
	var n int = len(endPoints)
	var monitorEnvio sync.WaitGroup //Sirve para que el programa no se cierre hasta que se envien todos los mensajes

	go acceptAndHandleConnections(quitChannel, connChan, barrierChan,
		&receivedMap, &mu, n)

	notifyOtherDistributedProcesses(endPoints, lineNumber, &monitorEnvio)

	fmt.Println("Waiting for all the processes to reach the barrier")
	<-barrierChan //Este canal solo envia true cuando el proceso ha recibido un mensaje
	//(por el puerto indicado de escucha de mensajes de barrera) de todos los demas procesos
	quitChannel <- true
	monitorEnvio.Wait()
	fmt.Println("Todos los mensajes enviados")
}

// Ejemplo de uso:
// func main() {
// 	if len(os.Args) != 3 {
// 		fmt.Println("Usage: go run main.go <endpoints_file> <line_number>")
// 	} else {
// 		fileName := os.Args[1]
// 		lineNumber, err := strconv.Atoi(os.Args[2])
// 		quitChannel := make(chan bool)
// 		CheckError(err)
// 		barrierListener, ipsBarrera, err := InicializarVariablesBarrera(fileName, lineNumber)
// 		CheckError(err)
// 		ActivarBarrera(barrierListener, ipsBarrera, lineNumber, quitChannel)
// 		barrierListener.Close()
// 		quitChannel <- true
// 	}
// }
