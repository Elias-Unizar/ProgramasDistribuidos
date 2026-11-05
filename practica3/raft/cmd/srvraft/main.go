package main

import (
	//"errors"

	//"log"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"raft/internal/comun/check"
	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
	"strconv"
)

func main() {
	// obtener entero de indice de este nodo
	//dsads
	//fmt.Println("TEST: ", os.Args)
	me, err := strconv.Atoi(os.Args[1])
	check.CheckError(err, "Main, mal numero entero de indice de nodo:")

	var nodos []rpctimeout.HostPort
	// Resto de argumento son los end points como strings
	// De todas la replicas-> pasarlos a HostPort
	for _, endPoint := range os.Args[2:] {
		nodos = append(nodos, rpctimeout.HostPort(endPoint))
	}

	// Parte Servidor
	nr := raft.NuevoNodo(nodos, me, make(chan raft.AplicaOperacion, 1000))
	rpc.Register(nr)

	//	fmt.Println("Replica escucha en :", me, " de ") //, os.Args[2:])

	l, err := net.Listen("tcp", os.Args[2:][me])
	check.CheckError(err, "Main listen error:")

	fmt.Println("Replica :", me, " ha llegado al RPC")
	rpc.Accept(l)
}
