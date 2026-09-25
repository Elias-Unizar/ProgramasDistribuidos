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

func escucharTareas(conexionMaster net.Conn, finPrograma chan bool) {
	defer conexionMaster.Close()
	seguirTrabajando := true
	for seguirTrabajando {
		decoder := gob.NewDecoder(conexionMaster)
		var intervaloTrabajo com.TPInterval
		err := decoder.Decode(&intervaloTrabajo)
		com.CheckError(err)
		if intervaloTrabajo.Max == -1 {
			seguirTrabajando = false
			continue
		}
		resultado := findPrimes(intervaloTrabajo)
		encoder := gob.NewEncoder(conexionMaster)
		err = encoder.Encode(resultado)
		com.CheckError(err)
	}
	finPrograma <- true
}

func conectarConMaster(ipsWorkers []string, miIp int, finPrograma chan bool) {
	endpoint := ipsWorkers[miIp-1]
	listener, err := net.Listen("tcp", endpoint)
	log.Println("Activo mi puerto para el master")
	com.CheckError(err)
	defer listener.Close()
	var conn net.Conn
	log.Println("Bloqueado el thread hasta que master me hable")
	conn, err = listener.Accept()
	com.CheckError(err)
	go escucharTareas(conn, finPrograma)
}

func main() {
	args := os.Args
	if len(args) != 5 {
		log.Println("Error: endpoint missing: go run server.go ficheroIpsInterno miLineaIp ficheroBarrera miLineaBarrera")
		os.Exit(1)
	}
	fileBarrier := os.Args[3]
	lineBarrier, err := strconv.Atoi(os.Args[4])
	connBarrierChan := make(chan net.Conn)
	barrierListener, ipsBarrera, err := com.InicializarVariablesBarrera(fileBarrier, lineBarrier)
	com.CheckError(err)
	go com.EscucharPuertoBarrera(barrierListener, connBarrierChan)
	log.Println("Activado puerto barrera")
	fileWorkers := os.Args[1]
	lineWorker, err := strconv.Atoi(os.Args[2])
	var workersIP []string
	workersIP, err = com.GetEndpoints(fileWorkers, lineWorker)
	log.Println("Tengo mi IP DE WORKER")

	finPrograma := make(chan bool)
	go conectarConMaster(workersIP, lineWorker, finPrograma)
	log.Println("Me paro en la barrera")
	com.ActivarBarrera(barrierListener, ipsBarrera, lineBarrier, connBarrierChan)
	<-finPrograma
	barrierListener.Close()
}
