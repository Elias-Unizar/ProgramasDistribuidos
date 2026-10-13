package ra

import (
	"sync"
	"testing"
	"time"

	"practica2/ms"
)

// Necesita ./users.txt con n lineas IP:puerto (aqui n = 3).
// Para emular el hecho de que los procesos distribuidos existen, usando unicamente un unico proceso(el que realiza los test claro)
// Se usan vectores, cada componente
const n = 3 //numero de procesos concurrente a simular

var sistemas [n]ms.MessageSystem //cada proceso tiene que tener su propio actor, y probaremos diferentes RA para los mismos procesos
var recvChannels [n]chan ms.Message
var msIniciados bool = false //NECESARIO PARA NO REABRIR EL MS y que falle por ocupacion de puertos iguales
// Los puertos solo se pueden abrir una vez, asi que el ms se levanta una sola vez
// y cada test crea sus propios RA (con su matriz) sobre el.
func iniciarMSs() {
	for i := 0; i < n; i++ {
		sistemas[i] = ms.New(i+1, "./users.txt", []ms.Message{})
		recvChannels[i] = make(chan ms.Message, 2*n)
		go func(msy *ms.MessageSystem, rac chan ms.Message) {
			for {
				rac <- msy.Receive() // hace de messageBoxListener: aqui solo hay 2 mensajes, los dos son para el RA y su canal propio
			}
		}(&sistemas[i], recvChannels[i])
	}
	msIniciados = true
}

func finalizarMSs(sistemas []ms.MessageSystem) {
	for _, mss := range sistemas {
		mss.Stop()
	}
	msIniciados = false
}

func crearRA(matriz [][]bool) []*RASharedDB {
	ras := make([]*RASharedDB, n)
	for i := range ras {
		ras[i] = New(&sistemas[i], recvChannels[i], matriz)
	}
	return ras
}

func finalizarRA(ras []*RASharedDB) {
	for _, ra := range ras {
		ra.Stop()
	}
}

// Cada proceso i: PreProtocol(acciones[i]), 200ms dentro de la SC, PostProtocol.
// Devuelve el maximo de procesos a la vez dentro de la SC, y el maximo simultaneo por accion(revisar comportamiento muchos lectores, muchos escritores).
// Esto funciona unicamente gracias a que al crear los Sistemas de Mensajes, se crea OTRO HILO MAS POR PROCESO,
// que hace de *Dispatcher*, es decir, que decide a donde tiene que enviar cada mensaje y si necesita tomar alguna accion o no,
// en este caso simplemente reenvia al RA
func simularComportamiento(ras []*RASharedDB, acciones []TipoSC) (simultaneos int, porAccion [2]int) {
	var mu sync.Mutex     //mutex local, vamos a hacer varios procesos que leen y escriben la misma variable, como el x++ que se usa de ejemplo de error de concurrencia
	var dentro [2]int     //Basicamente 2 contadores, uno por cada tipo de accion, idealmente seria variable, pero no se me ocurre como reusar la funcion
	var wg sync.WaitGroup //no finalizar el main hasta que los procesos simulados acaben realmente
	for i := range ras {
		wg.Add(1)
		go func(r *RASharedDB, a TipoSC) {
			defer wg.Done()
			r.PreProtocol(a)
			mu.Lock()
			dentro[a]++
			if dentro[0]+dentro[1] > simultaneos {
				simultaneos = dentro[0] + dentro[1]
			}
			if dentro[a] > porAccion[a] {
				porAccion[a] = dentro[a]
			}
			mu.Unlock()
			time.Sleep(400 * time.Millisecond)
			mu.Lock()
			dentro[a]--
			mu.Unlock()
			r.PostProtocol()
		}(ras[i], acciones[i])
	}
	wg.Wait()
	return
}

// Todas las acciones se excluyen: nunca mas de uno dentro.
func TestTodoExcluyente(t *testing.T) {
	if !msIniciados {
		iniciarMSs()
	}
	ras := crearRA([][]bool{{true, true}, {true, true}})
	simultaneos, _ := simularComportamiento(ras, []TipoSC{0, 0, 1})
	if simultaneos != 1 {
		t.Errorf("hubo %d procesos a la vez en la SC; se esperaba 1", simultaneos)
	}
	finalizarRA(ras)
}

// Nada se excluye: deben coincidir varios dentro.
func TestNadaExcluyente(t *testing.T) {
	if !msIniciados {
		iniciarMSs()
	}
	ras := crearRA([][]bool{{false, false}, {false, false}})
	simultaneos, _ := simularComportamiento(ras, []TipoSC{0, 0, 1})
	if simultaneos < 2 {
		t.Errorf("hubo %d procesos a la vez en la SC; se esperaba al menos 2", simultaneos)
	}
	finalizarRA(ras)
}

// La accion 0 se excluye consigo misma, pero no con la accion 1.
func TestMismaAccionExcluyeDistintaNo(t *testing.T) {
	if !msIniciados {
		iniciarMSs()
	}
	ras := crearRA([][]bool{{true, false}, {false, false}})
	simultaneos, porAccion := simularComportamiento(ras, []TipoSC{0, 0, 1})
	if porAccion[0] != 1 {
		t.Errorf("%d procesos de la accion 0 a la vez; se esperaba 1", porAccion[0])
	}
	if simultaneos < 2 {
		t.Errorf("la accion 1 deberia solaparse con la 0, pero simultaneos = %d", simultaneos)
	}
	finalizarRA(ras)
}

// Ejecutar test main, usar siempre este, que crea los MS
func TestEjecutarTodos(t *testing.T) {
	if !msIniciados {
		iniciarMSs()
	}
	TestMismaAccionExcluyeDistintaNo(t)
	TestNadaExcluyente(t)
	TestTodoExcluyente(t)
	finalizarMSs(sistemas[:])
}
