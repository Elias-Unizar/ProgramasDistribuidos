package barrier

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"practica2/ms"
)

const nNodos = 4

// Crea el MessageSystem de un nodo y su Barrier.
func nuevaBarrera(pid int, fichero string) *Barrier {
	m := ms.New(pid, fichero, []ms.Message{}) // AJUSTA a la firma real de tu ms.New
	in := make(chan ms.Message)
	go func(ms *ms.MessageSystem) {
		for {
			in <- ms.Receive() // AJUSTA al nombre real del método de recepción
		}
	}(&m)
	return InicializarBarrera(&m, in)
}

func esperar(t *testing.T, todos *sync.WaitGroup, limite time.Duration) {
	fin := make(chan struct{})
	go func() { todos.Wait(); close(fin) }()
	select {
	case <-fin:
	case <-time.After(limite):
		t.Fatal("timeout: la barrera no se liberó")
	}
}

// Todos los MS arrancados antes de entrar (Send cierra el programa si un peer está apagado).
// El último nodo llega tarde: nadie debe salir antes de que haya entrado.
func TestActivarBarrera(t *testing.T) {
	barreras := make([]*Barrier, nNodos)
	for i := range barreras {
		barreras[i] = nuevaBarrera(i+1, "./users.txt")
	}
	time.Sleep(800 * time.Millisecond) // MS escuchando antes de enviar

	var entraron atomic.Int32
	var todos sync.WaitGroup
	for i, b := range barreras {
		todos.Add(1)
		go func(i int, b *Barrier) {
			defer todos.Done()
			if i == nNodos-1 {
				time.Sleep(time.Second) // llega tarde a la barrera
			}
			entraron.Add(1)
			b.ActivarBarrera()
			if n := entraron.Load(); n != nNodos {
				t.Errorf("nodo %d salió con solo %d/%d dentro", i+1, n, nNodos)
			}
		}(i, b)
	}
	esperar(t, &todos, 10*time.Second)
}

// Los nodos se encienden escalonados; ProcesosSinEncender no puede volver
// hasta que el último esté encendido.
func TestProcesosSinEncender(t *testing.T) {
	retraso := 600 * time.Millisecond
	inicio := time.Now()

	var todos sync.WaitGroup
	for pid := 1; pid <= nNodos; pid++ {
		todos.Add(1)
		go func(pid int) {
			defer todos.Done()
			time.Sleep(time.Duration(pid-1) * retraso) // encendido escalonado
			b := nuevaBarrera(pid, "./users.txt")
			b.ActivarBarrera_ProcesosSinEncender()
			if time.Since(inicio) < time.Duration(nNodos-1)*retraso {
				t.Errorf("nodo %d terminó antes de que el último estuviera encendido", pid)
			}
		}(pid)
	}
}
