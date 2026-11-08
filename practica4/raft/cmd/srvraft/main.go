package main

import (
	//"errors"

	//"log"

	"net"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raftLog"
	"strconv"
)

func funcionalidadServidor(nr *raftLog.NodoRaft) {
	memoria := make(map[string]string)
	for operacion := range nr.CanalAplicarOperacion {
		var valor string
		switch operacion.Operacion.Operacion {
		case "leer":
			valor = memoria[operacion.Operacion.Clave]
		case "escribir":
			memoria[operacion.Operacion.Clave] = operacion.Operacion.Valor
			valor = operacion.Operacion.Valor
		default:
		}
		nr.Logger.Printf("[SERVIDOR] N%d ejecuta: %v --- Resultado: %d:%s\n",
			nr.Yo, operacion, operacion.Indice, valor)
		nr.CanalRecibirResultado <- raftLog.OperacionAplicada{
			Indice: operacion.Indice, Valor: valor}
	}
}

func main() {
	// obtener entero de indice de este nodo
	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal numero entero de indice de nodo:")

	var nodos []rpctimeout.HostPort
	// Resto de argumento son los end points como strings
	// De todas la replicas-> pasarlos a HostPort
	for _, endPoint := range os.Args[2:] {
		nodos = append(nodos, rpctimeout.HostPort(endPoint))
	}

	// Parte Servidor
	nr := raftLog.NuevoNodo(nodos, me, make(chan raftLog.AplicaOperacion, 1000))
	rpc.Register(nr)

	l, err := net.Listen("tcp", os.Args[2:][me])
	check.CheckError(err, "Main listen error:")
	nr.Logger.Printf("[SERVIDOR] N%d: Voy a ejecutar ya el servidor RPC", nr.Yo)
	go rpc.Accept(l)
}
