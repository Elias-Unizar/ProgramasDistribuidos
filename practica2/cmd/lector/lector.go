package main

import (
	"io"
	"log"
	"math/rand"
	"os"
	"practica2/ms"
	"practica2/ra"
	"strconv"
	"time"
)

const (
	Leer     ra.TipoSC = iota //0
	Escribir                  //1
)

type End struct{}

type FileSyncReq struct {
	Pid       int
	NuevoCont string
}
type FileSyncDone struct{}

func escribirEnFichero(fichero string, nuevoContenido string) bool {
	fdesc, err := os.OpenFile(fichero, os.O_APPEND|os.O_WRONLY, 0644) //Hacemos append directamente
	if err != nil {
		return false
	}
	defer fdesc.Close()

	_, err = fdesc.WriteString(nuevoContenido)
	return err == nil
}

func leerParteFichero(fichero string, offsetInicial int) string {
	fdesc, err := os.Open(fichero)
	if err != nil {
		return ""
	}
	defer fdesc.Close()
	_, err = fdesc.Seek(int64(offsetInicial), io.SeekStart)
	if err != nil {
		return ""
	}
	var datos []byte
	datos, err = io.ReadAll(fdesc)
	if err != nil {
		return ""
	}
	return string(datos)
}

func SincronizarCambioFichero(msSys *ms.MessageSystem, fichero string, msg FileSyncReq) {
	escribirEnFichero(fichero, msg.NuevoCont)
	msSys.Send(msg.Pid, FileSyncDone{})
}

func messageBoxListener(msSys *ms.MessageSystem, syncEnded chan bool, raRecieve chan ms.Message, fichero string) {
	processSynced := 0
	for {
		switch msg := msSys.Receive().(type) {
		case ra.Request, ra.Reply:
			raRecieve <- msg
		case FileSyncDone:
			processSynced += 1
			if processSynced == (msSys.GetTotalPeers() - 1) {
				processSynced = 0
				syncEnded <- true
			}
		case FileSyncReq:
			go SincronizarCambioFichero(msSys, fichero, msg)
		case End:
			return
		}
	}
}

func EscribirFicheroSC(ra *ra.RASharedDB, msSys *ms.MessageSystem, fichero string, nuevoContenido string, syncEnded chan bool) {
	ra.PreProtocol(Escribir)
	me := msSys.WhoAmI()
	for i := 1; i <= msSys.GetTotalPeers(); i++ {
		if i != me {
			msSys.Send(i, FileSyncReq{me, nuevoContenido})
		}
	}
	escribirEnFichero(fichero, nuevoContenido)
	<-syncEnded
	ra.PostProtocol()
}

func leerParteFicheroSC(ra *ra.RASharedDB, fichero string, offsetInicial int) string {
	ra.PreProtocol(Leer)
	datoLeido := leerParteFichero(fichero, offsetInicial)
	ra.PostProtocol()
	return datoLeido
}

func iniciarSistema(syncEnded chan bool) (string, *ms.MessageSystem, *ra.RASharedDB) {
	args := os.Args
	if len(args) != 4 {
		log.Println("Error: missing parameter: go run main.go ipLine endpointFile readWriteFile")
		os.Exit(1)
	}
	ipLine, err := strconv.Atoi(args[1])
	if err != nil {
		log.Println("No ha sido posible convertir el numero de linea de la IP")
	}
	messageTypes := []ms.Message{FileSyncReq{}, FileSyncDone{}, End{}}
	messageSystem := ms.New(ipLine, args[2], messageTypes)
	raRecieveChannel := make(chan ms.Message, 2)
	exludeSCAction := [][]bool{{false, true}, {true, true}}
	miRa := ra.New(&messageSystem, raRecieveChannel, exludeSCAction)
	go messageBoxListener(&messageSystem, syncEnded, raRecieveChannel, args[3])
	return args[3], &messageSystem, miRa
}

func finalizarSistema(msSys *ms.MessageSystem, raD *ra.RASharedDB) {
	msSys.Send(msSys.WhoAmI(), End{})
	raD.Stop()
	msSys.Send(msSys.WhoAmI(), ra.Reply{})
	msSys.Stop()
}

func bucleLector(miRa *ra.RASharedDB, fichero string, veces int) {
	offset := 0
	for i := 0; i < veces; i++ {
		nuevo := leerParteFicheroSC(miRa, fichero, offset)
		offset += len(nuevo)
		log.Printf("leido: %q", nuevo)
		time.Sleep(200 * time.Millisecond)
	}
}

func main() {
	syncEnded := make(chan bool)
	ficheroCompartido, msgSys, miRa := iniciarSistema(syncEnded)
	log.Printf("Sistema iniciado correctamente")
	s := time.Duration(rand.Int63n(500))
	time.Sleep(s * time.Millisecond)
	bucleLector(miRa, ficheroCompartido, 10)
	finalizarSistema(msgSys, miRa)
}
