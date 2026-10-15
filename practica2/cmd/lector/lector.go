package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"practica2/barrier"
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

func truncarFichero(fichero string) {
	if err := os.Truncate(fichero, 0); err != nil {
		log.Println("Error truncando", fichero, ":", err)
	}
}

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

func messageBoxListener(msSys *ms.MessageSystem, syncEnded chan bool, raRecieve chan ms.Message, barrierRecieve chan ms.Message, fichero string) {
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
			log.Printf("Cerramos el messageBoxListener")
			return
		case barrier.BarrierEnd, barrier.BarrierArrived:
			barrierRecieve <- msg
		}
	}
}

func EscribirFicheroSC(ra *ra.RASharedDB, msSys *ms.MessageSystem, fichero string, nuevoContenido string, syncEnded chan bool) {
	ra.PreProtocol(Escribir)
	log.Printf("Soy el proceso %d y he entrado a SC para escribir %s", msSys.WhoAmI(), nuevoContenido)
	me := msSys.WhoAmI()
	for i := 1; i <= msSys.GetTotalPeers(); i++ {
		if i != me {
			msSys.Send(i, FileSyncReq{me, nuevoContenido})
		}
	}
	escribirEnFichero(fichero, nuevoContenido)
	<-syncEnded
	log.Printf("Soy el proceso %d y estoy saliendo de SC con %d", msSys.WhoAmI(), Escribir)
	ra.PostProtocol()
}

func leerParteFicheroSC(ra *ra.RASharedDB, fichero string, offsetInicial int) string {
	ra.PreProtocol(Leer)
	datoLeido := leerParteFichero(fichero, offsetInicial)
	ra.PostProtocol()
	return datoLeido
}

func iniciarSistema(syncEnded chan bool) (string, *ms.MessageSystem, *ra.RASharedDB, *barrier.Barrier) {
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
	raRecieveChannel := make(chan ms.Message, 2*messageSystem.GetTotalPeers())
	exludeSCAction := [][]bool{{false, true}, {true, true}}
	miRa := ra.New(&messageSystem, raRecieveChannel, exludeSCAction)
	barrierInput := make(chan ms.Message, 2*messageSystem.GetTotalPeers())
	miBarrera := barrier.InicializarBarrera(&messageSystem, barrierInput)
	go messageBoxListener(&messageSystem, syncEnded, raRecieveChannel, barrierInput, args[3])
	truncarFichero(args[3])
	return args[3], &messageSystem, miRa, miBarrera
}

func finalizarSistema(msSys *ms.MessageSystem, raD *ra.RASharedDB) {
	raD.Stop()
	msSys.Send(msSys.WhoAmI(), End{})
	msSys.Stop()
}

func bucleEscritor(miRa *ra.RASharedDB, msSys *ms.MessageSystem, fichero string, syncEnded chan bool, veces int) {
	stringIni := "P" + strconv.Itoa(msSys.WhoAmI())
	stringIni = stringIni + " linea "
	log.Printf("Iniciando escritura, soy %s", stringIni)
	for i := 1; i <= veces; i++ {
		EscribirFicheroSC(miRa, msSys, fichero, (stringIni + strconv.Itoa(i) + "\n"), syncEnded)
		time.Sleep(time.Duration(rand.Int63n(1000)) * time.Millisecond)
	}
}

func bucleLector(miRa *ra.RASharedDB, fichero string, veces int, numP int) {
	offset := 0
	for i := 0; i < veces; i++ {
		nuevo := leerParteFicheroSC(miRa, fichero, offset)
		offset += len(nuevo)
		log.Printf("Soy P%d, y he leido: %q", numP, nuevo)
		time.Sleep(time.Duration(rand.Int63n(500)) * time.Millisecond)
	}
}

func main() {
	syncEnded := make(chan bool)
	s := time.Duration(rand.Int63n(1500))
	time.Sleep(s * time.Millisecond)
	ficheroCompartido, msgSys, miRa, miBarrera := iniciarSistema(syncEnded)
	miBarrera.ActivarBarrera_ProcesosSinEncender()
	s = time.Duration(rand.Int63n(500))
	time.Sleep(300 + s*time.Millisecond)
	bucleLector(miRa, ficheroCompartido, 20, msgSys.WhoAmI())
	miBarrera.ActivarBarrera()
	fmt.Printf("Hemos acabado el main, empezando a cerrar los canales, soy %d\n", msgSys.WhoAmI())
	finalizarSistema(msgSys, miRa)
	time.Sleep(1 * time.Second)
}
