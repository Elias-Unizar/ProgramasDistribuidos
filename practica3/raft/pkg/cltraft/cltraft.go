// Paquete cltraft: cliente sencillo para enviar operaciones a un grupo de
// nodos Raft. Localiza al lider, sigue sus redirecciones y reintenta.
package cltraft

import (
	"fmt"
	"os"
	"time"

	"raft/internal/comun/rpctimeout"
	"raft/internal/raft"
)

const (
	timeoutRPC     = 500 * time.Millisecond
	pausaReintento = 100 * time.Millisecond
)

// Cliente guarda los nodos Raft y el ultimo lider conocido
type Cliente struct {
	nodos []rpctimeout.HostPort
	lider int
}

func NuevoCliente(nodos []rpctimeout.HostPort) *Cliente {
	return &Cliente{nodos: nodos, lider: 1} //Lider 1 por ejemplo, no el 0
}

// Anota el lider segun la respuesta recibida del nodo consultado
func (c *Cliente) actualizarLider(r *raft.ResultadoRemoto) {
	switch {
	case r.EsLider:
		return
	case r.IdLider >= 0 && r.IdLider < len(c.nodos):
		c.lider = r.IdLider
	default:
		fmt.Printf("Error, el cliente no tiene acceso a ese nodo")
		os.Exit(1)
	}
}

// Un solo intento. La respuesta es nueva en cada llamada porque gob no
// transmite los campos con valor cero y reutilizarla dejaria restos.
func (c *Cliente) intentar(nodo int, op raft.TipoOperacion) (
	raft.ResultadoRemoto, error) {
	var r raft.ResultadoRemoto
	err := c.nodos[nodo].CallTimeout("NodoRaft.SometerOperacionRaft",
		op, &r, timeoutRPC)
	return r, err
}

// SometerOperacion envia la operacion al lider y devuelve su respuesta
// (indice de registro, mandato, lider). Reintenta si no hay lider o si el
// nodo consultado no responde.
func (c *Cliente) SometerOperacion(op raft.TipoOperacion, intentos int) (
	raft.ResultadoRemoto, error) {
	var r raft.ResultadoRemoto
	for intento := 0; intento < intentos; intento++ {
		var err error
		if r, err = c.intentar(c.lider, op); err != nil {
			c.lider = (c.lider + 1) % (len(c.nodos))
		} else if c.actualizarLider(&r); r.EsLider {
			return r, nil
		}
		time.Sleep(pausaReintento)
	}
	return r, fmt.Errorf("sin lider tras %d intentos", intentos)
}
