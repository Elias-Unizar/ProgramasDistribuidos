/*
* AUTOR: Rafael Tolosana Calasanz y Unai Arronategui
* ASIGNATURA: 30221/39521 - Sistemas Distribuidos
*	       Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2022
* FICHERO: server-draft.go
* DESCRIPCIÓN: contiene la funcionalidad esencial para realizar los servidores
*				correspondientes a la práctica 1
 */
package main

import (
	"encoding/gob"
	"log"
	"net"
	"os"
	"practica1/com"
	"strconv"
)

// PRE: verdad = !foundDivisor
// POST: IsPrime devuelve verdad si n es primo y falso en caso contrario
func isPrime(n int) (foundDivisor bool) {
	foundDivisor = false
	for i := 2; (i < n) && !foundDivisor; i++ {
		foundDivisor = (n%i == 0)
	}
	return !foundDivisor
}

// PRE: interval.A < interval.B
// POST: FindPrimes devuelve todos los números primos comprendidos en el
//
//	intervalo [interval.A, interval.B]
func findPrimes(interval com.TPInterval) (primes []int) {
	for i := interval.Min; i <= interval.Max; i++ {
		if isPrime(i) {
			primes = append(primes, i)
		}
	}
	return primes
}

func processRequest(conn net.Conn) {
	var request com.Request
	decoder := gob.NewDecoder(conn)
	err := decoder.Decode(&request)
	com.CheckError(err)
	primes := findPrimes(request.Interval)
	reply := com.Reply{Id: request.Id, Primes: primes}
	encoder := gob.NewEncoder(conn)
	encoder.Encode(&reply)
}

func goroutine(peticionPendiente chan net.Conn, canalQuit <-chan bool) {
	var miCliente net.Conn
	for {
		select {
		case miCliente = <-peticionPendiente: //Oportunista, las goRoutines estan bloqueadas hasta que hay una conexion en el canal
			processRequest(miCliente)
			miCliente.Close()
		case <-canalQuit:
			break
			// default:
			// 	time.Sleep(1 * time.Second)
			// 	continue
		}
	}
}

func main() {
	args := os.Args
	if len(args) != 3 {
		log.Println("Error: endpoint missing: go run server.go ip:port goroutinePool")
		os.Exit(1)
	}
	endpoint := args[1]
	numberOfGoroutines, err := strconv.Atoi(args[2])
	com.CheckError(err)
	listener, err := net.Listen("tcp", endpoint)
	com.CheckError(err)

	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	canalClientes := make(chan net.Conn, numberOfGoroutines)
	canalQuit := make(chan bool)
	for i := 0; i < numberOfGoroutines; i++ {
		go goroutine(canalClientes, canalQuit)
	}

	log.Println("***** Listening for new connection in endpoint ", endpoint)
	serverActivo := true
	for serverActivo == true {
		conn, err := listener.Accept()
		com.CheckError(err)
		canalClientes <- (conn)
	}
	canalQuit <- true //No se usa, pero si se quiere una manera de parar el servidor con un tecla, es un break en el for infinito y salta a este canal
}
