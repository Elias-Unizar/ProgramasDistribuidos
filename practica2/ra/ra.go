/*
* AUTOR: Rafael Tolosana Calasanz
* ASIGNATURA: 30221 Sistemas Distribuidos del Grado en Ingeniería Informática
*			Escuela de Ingeniería y Arquitectura - Universidad de Zaragoza
* FECHA: septiembre de 2021
* FICHERO: ricart-agrawala.go
* DESCRIPCIÓN: Implementación del algoritmo de Ricart-Agrawala Generalizado en Go
 */
package ra

import (
	"log"
	"practica2/ms"
	"sync"
)

type TipoSC int //Lo creo para que la revision de prioridad de tipos de operacion en SC sea más cómoda(fila y columna son int)
// Se tendria que crear uno asi en cada problema, y la matriz en ese main

type Request struct {
	Clock     int
	Pid       int
	ReqAccion TipoSC
}

type Reply struct{}

type RASharedDB struct {
	Mutex       sync.Mutex // mutex para proteger concurrencia sobre las variables
	numberPeers int
	myPid       int

	OurSeqNum int //relojes escalares
	HigSeqNum int

	OutRepCnt int    //numero de mensajes que faltan por recibir
	RepDefd   []bool //(usado con la cola de mensajes diferidos, iteramos todos y comprobamos a quien hacemos esperar)

	ReqCS        bool
	CSAction     TipoSC
	exclSCMatrix [][]bool //Matriz de exclusión de SC. En el eje horizonal(columnas) se representa la accion que quiero realizar en SC
	//(si siquiera quiero entrar a SC). El eje vertical es la accion que quiere realizar el Request que me llega

	_ms        *ms.MessageSystem //para comunicarse con el resto de procesos(solo envios, el RA NO DEBE TENER control absoluto sobre la red)
	rcvMsgChan chan ms.Message   //Como queremos reutilizar el MS en la aplicación, si dejamos que el RA lea directamente del msgbox, colisiona TOTALMENTE con el resto de futuros mensajes potenciales diversos

	done  chan bool
	chrep chan bool
}

//Algo interesante, si hay varias secciones críticas, es mejor crear varias instancias del Ricart-Argwala, antes que hacer crecer
//a la matriz de exclusión de acciones. En un sistema con 1000 procesos, con una sección crítica de 20, y otra de 980 procesos
//los procesos que operan sobre la SC pequeña tienen una sobrecarga brutal, e inútil

//De igual manera, si se quiere optimizar el tamaño de la matriz de exclusión esta es simetrica y hay varias tecnicas para hacerlo

func (ra *RASharedDB) calcularPrio(rqst Request) bool {
	//Si no pido SC, da igual, no tengo prioridad
	miPrioridad := (ra.ReqCS) &&
		((ra.OurSeqNum < rqst.Clock) || ((ra.OurSeqNum == rqst.Clock) && (ra.myPid < rqst.Pid)))
	//Hasta aqui se que tengo prioridad a la hora de tomar la decision de si dejo o no pasar a SC al proceso que envia Request
	miPrioridad = miPrioridad && ra.exclSCMatrix[rqst.ReqAccion][ra.CSAction] //Si no tengo prioridad de igual mirar la matriz
	//SI tuviera prioridad, la matriz indica si es prioridad excluyente o no(true si no se ejecutan a la vez, tengo prioridad)
	return miPrioridad
}

func (ra *RASharedDB) recepcionRequest(msg Request) {
	if ra.HigSeqNum < msg.Clock {
		ra.HigSeqNum = msg.Clock
	}
	ra.Mutex.Lock()
	miPrioridad := ra.calcularPrio(msg)
	if miPrioridad {
		ra.RepDefd[msg.Pid-1] = true
	} else {
		ra._ms.Send(msg.Pid, Reply{})
	}
	ra.Mutex.Unlock()
}

func (ra *RASharedDB) recepcionReply() {
	ra.OutRepCnt -= 1 //No hace falta usar el mutex, Reply solo llega una vez se ha escrito la variable ya
	if ra.OutRepCnt == 0 {
		ra.chrep <- true
	} //desbloqueamos al proceso que ejecuta ricart-argwala
}

func (ra *RASharedDB) escucharMensajes() {
	for {
		select {
		case <-ra.done: //quieren cerrar al completo el ricartArgwala, hay que cerrar este hilo(funcion)
			log.Printf("Se cierra el listener del RicarAgrawala")
			return
		case m := <-ra.rcvMsgChan: //Se podria hacer msg := <- --- Ya que se inicializa la variable msg como unica para dentro del case, y luego del switch, pero queda feo
			switch msg := m.(type) {
			case Request:
				ra.recepcionRequest(msg)
			case Reply:
				ra.recepcionReply()
			}
		}
	}
}

func New(_ms *ms.MessageSystem, rcvMsg chan ms.Message, matrizAcciones [][]bool) (ra *RASharedDB) {
	messageTypes := []ms.Message{Request{}, Reply{}}
	ms.Register(messageTypes)

	ra = &RASharedDB{}
	ra.Mutex = sync.Mutex{}
	ra.OurSeqNum, ra.HigSeqNum, ra.OutRepCnt = 0, 0, 0
	ra.myPid = _ms.WhoAmI()
	ra.numberPeers = _ms.GetTotalPeers()
	ra.RepDefd = make([]bool, ra.numberPeers) //Funcion extra creada para no releer el usersFile del messageSystem
	ra.ReqCS = false

	ra.exclSCMatrix = matrizAcciones

	ra._ms = _ms
	ra.rcvMsgChan = rcvMsg

	ra.done = make(chan bool)
	ra.chrep = make(chan bool) //Se usa para bloquear el proceso sin hacer un while infinito, se llena cuando ya se han recibido todas las Reply
	ra.CSAction = 0

	go ra.escucharMensajes()

	return ra
}

// Pre: Verdad
// Post: Realiza  el  PreProtocol  para el  algoritmo de
//
//	Ricart-Agrawala Generalizado
func (ra *RASharedDB) PreProtocol(accion TipoSC) {
	ra.CSAction = accion
	ra.ReqCS = true
	ra.OurSeqNum = ra.HigSeqNum + 1
	ra.OutRepCnt = ra.numberPeers - 1
	for i := 1; i <= ra.numberPeers; i++ {
		if i != ra.myPid {
			ra._ms.Send(i, Request{ra.OurSeqNum, ra.myPid, accion})
		}
	}
	<-ra.chrep //Bloqueados hasta que todos los procesos nos respondan (el hilo que ejecute escucharMensajes lo llenara)
}

// Pre: Verdad
// Post: Realiza  el  PostProtocol  para el  algoritmo de
//
//	Ricart-Agrawala Generalizado
func (ra *RASharedDB) PostProtocol() {
	ra.Mutex.Lock()   //El unico problema de concurrencia ocurre al calcular la prioridad y decidir si permitir SC o no a otro proceso
	ra.ReqCS = false  //El hilo que se encarga de ello tiene el mutex para calcular la prioridad(y enviar el proceso a la cola de diferidos o no)
	ra.Mutex.Unlock() //Una vez que nos aseguramos que el hilo principal(este) es el unico ejecutando algo, y se va de SC, todos los mensajes dejaran de diferirse
	ra.CSAction = 0
	for i := 1; i <= ra.numberPeers; i++ {
		if ra.RepDefd[i-1] {
			ra.RepDefd[i-1] = false
			ra._ms.Send(i, Reply{})
		}
	}
}

func (ra *RASharedDB) Stop() {
	ra.done <- true
}
