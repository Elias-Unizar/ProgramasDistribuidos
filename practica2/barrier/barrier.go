/*
Paquete creado por Elias Agustin Mihailov, para facilitar la implantacion del algortimo de barrera en cualquier codigo
para práctica futuras. He donado este código a otros compañeros, ya que funciona similar a lo que sería usar otros elementos
de internet para lograr el mismo proposito, y no interfiere con la compleción del código barrera entregable
*/
package barrier

import (
	"encoding/gob"
	"fmt"
	"net"
	"practica2/ms"
	"strconv"
	"sync"
	"time"
)

type BarrierArrived struct {
	Pid int //En un mundo sin error no haria falta, siempre llegan los mensajes justos sin
}

type BarrierEnd struct{}

type Barrier struct {
	_ms          *ms.MessageSystem
	inputMsgChan chan ms.Message
	recievedMsg  map[string]bool
	mutex        sync.Mutex
	totalPeer    int
	waitChan     chan bool
}

func handleBarrierMS(miB *Barrier, msgArrived BarrierArrived) {
	miB.mutex.Lock()
	miB.recievedMsg[strconv.Itoa(msgArrived.Pid)] = true
	fmt.Println("Received ", len(miB.recievedMsg), " elements")
	if len(miB.recievedMsg) == miB.totalPeer-1 {
		miB.recievedMsg = map[string]bool{}
		miB.waitChan <- true
	}
	miB.mutex.Unlock()
}

func acceptAndHandleConnections(miB *Barrier) {
	for {
		switch msg := (<-miB.inputMsgChan).(type) {
		case BarrierArrived:
			handleBarrierMS(miB, msg)
		case BarrierEnd:
			fmt.Println("Stopping the barrier scheduler...")
			return
		}
	}
}

func notifyOtherDistributedProcesses(miB *Barrier) {
	myPid := miB._ms.WhoAmI()
	for i := 1; i <= miB.totalPeer; i++ {
		if i != myPid { //Para no enviar un mensaje a uno mismo
			miB._ms.Send(i, BarrierArrived{myPid})
		}
	}
}

func InicializarBarrera(_ms *ms.MessageSystem, inputChan chan ms.Message) (b *Barrier) {
	registerMessages := []ms.Message{BarrierArrived{}, BarrierEnd{}}
	ms.Register(registerMessages)
	b = &Barrier{}
	b._ms = _ms
	b.inputMsgChan = inputChan
	b.recievedMsg = map[string]bool{}
	b.mutex = sync.Mutex{}
	b.totalPeer = _ms.GetTotalPeers()
	b.waitChan = make(chan bool)
	return b
}

func (b *Barrier) ActivarBarrera_ProcesosSinEncender() {
	var waitEnvios sync.WaitGroup
	myPid := b._ms.WhoAmI()
	go acceptAndHandleConnections(b)
	for i, ip := range b._ms.GetPeers() {
		if i+1 != myPid { //No tengo que ser yo mismo
			waitEnvios.Add(1)
			go func(ep string, myPid int) {
				for {
					conn, err := net.Dial("tcp", ep)
					if err != nil {
						fmt.Println("Error connecting to", ep, ":", err)
						time.Sleep(5 * time.Second)
						continue
					}
					var m ms.Message = BarrierArrived{myPid}
					err = gob.NewEncoder(conn).Encode(&m)
					if err != nil {
						fmt.Println("Error sending message:", err)
						conn.Close()
						time.Sleep(1 * time.Second)
						continue
					}
					conn.Close()
					waitEnvios.Done()
					return
				}
			}(ip, myPid)
		}
	}
	fmt.Println("Waiting for all the processes to reach the barrier INSTANCIACION DE NUEVAS MAQUINAS")
	<-b.waitChan //Este canal solo envia true cuando el proceso ha recibido un mensaje
	//(por el proceso indicado de escucha de mensajes de barrera) de todos los demas procesos
	waitEnvios.Wait()
	fmt.Println("Todos los mensajes enviados")
	b._ms.Send(b._ms.WhoAmI(), BarrierEnd{})
}

func (b *Barrier) ActivarBarrera() {
	// Barrier synchronization
	go acceptAndHandleConnections(b)

	notifyOtherDistributedProcesses(b)

	fmt.Println("Waiting for all the processes to reach the barrier")
	<-b.waitChan //Este canal solo envia true cuando el proceso ha recibido un mensaje
	//(por el proceso indicado de escucha de mensajes de barrera) de todos los demas procesos
	fmt.Println("Todos los mensajes enviados")
	b._ms.Send(b._ms.WhoAmI(), BarrierEnd{})
}
