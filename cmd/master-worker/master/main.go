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
	"sync"
	"time"
)

func dividirIntervalo(intervalo com.TPInterval, numeroSubdivision int) []com.TPInterval {
	var tamagnoParticion int
	var conjuntoIntervalos []com.TPInterval
	tamagnoParticion = (intervalo.Max - intervalo.Min) / numeroSubdivision
	inicioInt := intervalo.Min
	for i := 0; i < numeroSubdivision-1; i++ {
		conjuntoIntervalos = append(conjuntoIntervalos, com.TPInterval{Min: inicioInt,
			Max: inicioInt + tamagnoParticion - 1})
		inicioInt += tamagnoParticion
	}
	conjuntoIntervalos = append(conjuntoIntervalos, com.TPInterval{Min: inicioInt, Max: intervalo.Max})
	return conjuntoIntervalos
}

func iniciarConexionWorkers(listaIpWorkers []string, miIndiceIP int) []net.Conn {
	var conexionWorkers []net.Conn
	for i, ip := range listaIpWorkers {
		if i != miIndiceIP-1 {
			var err error
			var conn net.Conn
			for {
				conn, err = net.Dial("tcp", ip)
				if err == nil {
					break
				}
				log.Println("Conexion con el worker ", ip, " bloqueada, todavia no se ha levantado")
				time.Sleep(1 * time.Second)
			}
			conexionWorkers = append(conexionWorkers, conn)
		}
	}
	return conexionWorkers
}

func asignarTareaWorker(wg *sync.WaitGroup, mu *sync.Mutex, connWorker net.Conn, intervaloTrabajo com.TPInterval, resultadoCombinado *[]int) {
	defer wg.Done()
	encoder := gob.NewEncoder(connWorker)
	err := encoder.Encode(intervaloTrabajo)
	com.CheckError(err)

	var resutladoParcial []int
	decoder := gob.NewDecoder(connWorker)
	err = decoder.Decode(&resutladoParcial) //  receive reply
	com.CheckError(err)

	mu.Lock()
	*resultadoCombinado = append(*resultadoCombinado, resutladoParcial...)
	mu.Unlock()
}

func processRequestMaster(clientConn net.Conn, conexionWorkers []net.Conn) bool {
	var request com.Request
	decoder := gob.NewDecoder(clientConn)
	err := decoder.Decode(&request)
	com.CheckError(err)
	if request.Id == -1 {
		return false
	}
	conjuntoIntervalos := dividirIntervalo(request.Interval, len(conexionWorkers))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var resultadoCombinado []int
	for i, subintervalo := range conjuntoIntervalos {
		workeri := conexionWorkers[i]
		wg.Add(1)
		go asignarTareaWorker(&wg, &mu, workeri, subintervalo, &resultadoCombinado)
	}
	wg.Wait()

	reply := com.Reply{Id: request.Id, Primes: resultadoCombinado}
	encoder := gob.NewEncoder(clientConn)
	encoder.Encode(&reply)
	clientConn.Close()
	return true
}

func enviarFinWorkers(misWorkers []net.Conn) {
	var wg sync.WaitGroup
	for _, conn := range misWorkers {
		wg.Add(1)
		go func(connWorker net.Conn) {
			defer wg.Done()
			encoder := gob.NewEncoder(connWorker)
			err := encoder.Encode(com.TPInterval{Min: -1, Max: -1})
			com.CheckError(err)
		}(conn)
	}
	wg.Wait()
}

func main() {
	args := os.Args
	if len(args) != 6 {
		log.Println("Error: endpoint missing: go run server.go ip:port(Clientes) ficheroIpsInterno miLineaIp ficheroBarrera miLineaBarrera")
		os.Exit(1)
	}

	fileBarrier := os.Args[4]
	lineBarrier, err := strconv.Atoi(os.Args[5])
	connBarrierChan := make(chan net.Conn)
	barrierListener, ipsBarrera, err := com.InicializarVariablesBarrera(fileBarrier, lineBarrier)
	com.CheckError(err)
	go com.EscucharPuertoBarrera(barrierListener, connBarrierChan)
	log.Println("Activado puerto barrera")
	fileWorkers := os.Args[2]
	lineMaster, err := strconv.Atoi(os.Args[3])
	clientPort := os.Args[1]
	var workersIP []string
	workersIP, err = com.GetEndpoints(fileWorkers, lineMaster)
	var conexionesWorkers []net.Conn
	// go func() {
	// 	conexionesWorkers = iniciarConexionWorkers(workersIP, lineMaster)
	// }()
	conexionesWorkers = iniciarConexionWorkers(workersIP, lineMaster)
	com.ActivarBarrera(barrierListener, ipsBarrera, lineBarrier, connBarrierChan)

	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	clientListener, err := net.Listen("tcp", clientPort)
	com.CheckError(err)

	log.Println("***** Listening for new connection in endpoint ", clientPort)
	continueClient := true
	for continueClient {
		clientConn, err := clientListener.Accept()
		com.CheckError(err)
		continueClient = processRequestMaster(clientConn, conexionesWorkers)
	}
	barrierListener.Close()
	enviarFinWorkers(conexionesWorkers)
}
