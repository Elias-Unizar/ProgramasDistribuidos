// Escribir vuestro código de funcionalidad Raft en este fichero
//

package raft

//
// API
// ===
// Este es el API que vuestra implementación debe exportar
//
// nodoRaft = NuevoNodo(...)
//   Crear un nuevo servidor del grupo de elección.
//
// nodoRaft.Para()
//   Solicitar la parado de un servidor
//
// nodo.ObtenerEstado() (yo, mandato, esLider)
//   Solicitar a un nodo de elección por "yo", su mandato en curso,
//   y si piensa que es el msmo el lider
//
// nodoRaft.SometerOperacion(operacion interface()) (indice, mandato, esLider)
// type AplicaOperacion

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	//"crypto/rand"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/rpctimeout"
)

const (
	//milisegundos base sobre los que luego se aplicara un plus random para que
	//los timeouts puedan salir de livelocks?
	TIMEOUTBASE_MS = 20

	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = true

	// Poner a true para logear a stdout en lugar de a fichero
	kLogToStdout = false

	// Cambiar esto para salida de logs en un directorio diferente
	kLogOutputDir = "./logs_raft/"
)

type TipoOperacion struct {
	Operacion string // La operaciones posibles son "leer" y "escribir"
	Clave     string
	Valor     string // en el caso de la lectura Valor = ""
}

type EntradaRegistro struct {
	Operacion TipoOperacion
	Mandato   int
}

// A medida que el nodo Raft conoce las operaciones de las  entradas de registro
// comprometidas, envía un AplicaOperacion, con cada una de ellas, al canal
// "canalAplicar" (funcion NuevoNodo) de la maquina de estados
type AplicaOperacion struct {
	Indice    int // en la entrada de registro
	Operacion TipoOperacion
}

const (
	SEGUIDOR  = "Seguidor"
	CANDIDATO = "Candidato"
	LIDER     = "Lider"
)

type datosEleccion struct {
	miEstadoEleccion  string
	tmrOut            <-chan time.Time
	candidatoHayLider chan bool
	candidatoSoyLider chan bool
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos   []rpctimeout.HostPort
	Yo      int // indice de este nodos en campo array "nodos"
	IdLider int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger                *log.Logger
	CanalAplicarOperacion chan AplicaOperacion

	MandatoActual   int
	CandidatoVotado int //Si se ha votado a alguien en el mandato actual tiene
	//su id, si no tendra el valor de IntNOINICIALIZADO
	//El registro con entradas que contienen mandato y operacion
	RegistroOperaciones  []EntradaRegistro
	UltimoIndiceRegistro int
	UltimoComprometido   int
	UltimoAplicado       int

	//Datos que usará el nodo en caso de que se vuelva lider
	//Contiene información de todos los nodos de la red(de los que pueda tener)
	SiguienteIndiceEnviar []int
	IndiceReplicado       []int

	misDatosEleccion datosEleccion
}

// Creacion de un nuevo nodo de eleccion
//
// Tabla de <Direccion IP:puerto> de cada nodo incluido a si mismo.
//
// <Direccion IP:puerto> de este nodo esta en nodos[yo]
//
// Todos los arrays nodos[] de los nodos tienen el mismo orden

// canalAplicar es un canal donde, en la practica 5, se recogerán las
// operaciones a aplicar a la máquina de estados. Se puede asumir que
// este canal se consumira de forma continúa.
//
// NuevoNodo() debe devolver resultado rápido, por lo que se deberían
// poner en marcha Gorutinas para trabajos de larga duracion

func crearFicheroConRutaGlobal(nombre string) *os.File {
	err := os.MkdirAll(kLogOutputDir, os.ModePerm)
	if err != nil {
		panic(err.Error())
	}
	logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt",
		kLogOutputDir, nombre), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		panic(err.Error())
	}
	return logOutputFile
}

func crearLoggerNodo(nr *NodoRaft, nodos []rpctimeout.HostPort, yo int) {
	if kEnableDebugLogs {
		nombreNodo := nodos[yo].Host() + "_" + nodos[yo].Port()
		logPrefix := fmt.Sprintf("%s", nombreNodo)

		fmt.Println("LogPrefix: ", logPrefix)

		if kLogToStdout {
			nr.Logger = log.New(os.Stdout, nombreNodo+" -->> ",
				log.Lmicroseconds|log.Lshortfile)
		} else {
			logOutputFile := crearFicheroConRutaGlobal(logPrefix)
			nr.Logger = log.New(logOutputFile,
				logPrefix+" -> ", log.Lmicroseconds|log.Lshortfile)
		}
		nr.Logger.Println("logger initialized")
	} else {
		nr.Logger = log.New(ioutil.Discard, "", 0)
	}
}

// En RCP de append, ver si enviar por el canal que hayLider
// En RCP de votar, avisa por un canal si gana
// yy ya estaria
func (nr *NodoRaft) accionesSeguidor() {
	nr.misDatosEleccion.tmrOut = time.After(TIMEOUTBASE_MS * time.Millisecond)
	<-nr.misDatosEleccion.tmrOut //Siempre estará aqui bloqueado, total
	nr.misDatosEleccion.miEstadoEleccion = CANDIDATO
	//RCP de pedir votos¿
}

func (nr *NodoRaft) accionesCandidato() {
	nr.misDatosEleccion.tmrOut = time.After(TIMEOUTBASE_MS * time.Millisecond)
	select {
	case <-nr.misDatosEleccion.tmrOut:
		//Volver RPC de pedir votos
	case <-nr.misDatosEleccion.candidatoHayLider:
		//Cambiar a seguidor, el canal se activa desde AppendEntries
		nr.misDatosEleccion.miEstadoEleccion = SEGUIDOR
	case <-nr.misDatosEleccion.candidatoSoyLider:
		nr.misDatosEleccion.miEstadoEleccion = LIDER
	}
}

func (nr *NodoRaft) accionesLider() {
	tckrChan := time.Tick(TIMEOUTBASE_MS * time.Millisecond)

	select {
	case <-tckrChan:
		//RCP de appendEntries
	case <-nr.misDatosEleccion.candidatoHayLider:
		nr.misDatosEleccion.miEstadoEleccion = SEGUIDOR
	}
}

func (nr *NodoRaft) monitorizadorEstadosNodo() {
	for {
		switch nr.misDatosEleccion.miEstadoEleccion {
		case SEGUIDOR:
			nr.accionesSeguidor()
		case LIDER:
			nr.accionesLider()
		case CANDIDATO:
			nr.accionesCandidato()
		}
	}
}

func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = IntNOINICIALIZADO
	nr.Mux = sync.Mutex{}
	nr.CanalAplicarOperacion = canalAplicarOperacion

	crearLoggerNodo(nr, nodos, yo)
	go nr.monitorizadorEstadosNodo()

	// Añadir codigo de inicialización
	nr.MandatoActual = 0
	nr.CandidatoVotado = IntNOINICIALIZADO
	nr.UltimoIndiceRegistro = 0
	nr.UltimoComprometido = 0
	nr.UltimoAplicado = 0
	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
func (nr *NodoRaft) para() {
	go func() { time.Sleep(5 * time.Millisecond); os.Exit(0) }()
}

// Devuelve "yo", mandato en curso y si este nodo cree ser lider
//
// Primer valor devuelto es el indice de este  nodo Raft el el conjunto de nodos
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int) {
	var yo int = nr.Yo
	var mandato int = nr.MandatoActual
	var idLider int = nr.IdLider
	var esLider bool = yo == idLider

	return yo, mandato, esLider, idLider
}

// El servicio que utilice Raft (base de datos clave/valor, por ejemplo)
// Quiere buscar un acuerdo de posicion en registro para siguiente operacion
// solicitada por cliente.

// Si el nodo no es el lider, devolver falso
// Sino, comenzar la operacion de consenso sobre la operacion y devolver en
// cuanto se consiga
//
// No hay garantia que esta operacion consiga comprometerse en una entrada de
// de registro, dado que el lider puede fallar y la entrada ser reemplazada
// en el futuro.
// Primer valor devuelto es el indice del registro donde se va a colocar
// la operacion si consigue comprometerse.
// El segundo valor es el mandato en curso
// El tercer valor es true si el nodo cree ser el lider
// Cuarto valor es el lider, es el indice del líder si no es él
func (nr *NodoRaft) someterOperacion(operacion TipoOperacion) (int, int,
	bool, int, string) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	indice := nr.UltimoIndiceRegistro + 1
	mandato := nr.MandatoActual
	idLider := nr.IdLider
	valorADevolver := ""
	EsLider := nr.Yo == nr.IdLider

	if EsLider {
		nr.UltimoIndiceRegistro += 1
		nr.RegistroOperaciones[nr.UltimoIndiceRegistro] =
			EntradaRegistro{Operacion: operacion, Mandato: nr.MandatoActual}
		//appendEntries
		//posible segundo canal de comunicacion para obtener el resultado de
		//la operacion, en principio el servidor atende clientes de manera
		//secuencial, en cualquier caso se podría realizar un mapa de canales
		//que se accedan desde un idCliente autoasignado¿ para atender con
		//currentemente? No lo voy a pensar ahora
	}

	return indice, mandato, EsLider, idLider, valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	defer nr.para()
	return nil
}

type EstadoParcial struct {
	Mandato int
	EsLider bool
	IdLider int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider = nr.obtenerEstado()
	return nil
}

type ResultadoRemoto struct {
	ValorADevolver string
	IndiceRegistro int
	EstadoParcial
}

func (nr *NodoRaft) SometerOperacionRaft(operacion TipoOperacion,
	reply *ResultadoRemoto) error {
	reply.IndiceRegistro, reply.Mandato, reply.EsLider,
		reply.IdLider, reply.ValorADevolver = nr.someterOperacion(operacion)
	return nil
}

// -----------------------------------------------------------------------
// LLAMADAS RPC protocolo RAFT
//
// Structura de ejemplo de argumentos de RPC PedirVoto.
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type ArgsPeticionVoto struct {
	Mandato              int
	IdCandidato          int
	UltimoIndiceRegistro int
	UltimoIndiceMandato  int
}

// Structura de ejemplo de respuesta de RPC PedirVoto,
//
// Recordar
// -----------
// Nombres de campos deben comenzar con letra mayuscula !
type RespuestaPeticionVoto struct {
	MandatoActual int
	DecisionVoto  bool
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	if nr.MandatoActual > peticion.Mandato {
		reply.DecisionVoto = false
		reply.MandatoActual = nr.MandatoActual
	} else if nr.MandatoActual == peticion.Mandato {

		if nr.CandidatoVotado == IntNOINICIALIZADO ||
			nr.CandidatoVotado == peticion.IdCandidato {

			nr.CandidatoVotado = peticion.IdCandidato
			reply.DecisionVoto = true
			reply.MandatoActual = nr.MandatoActual

		} else {
			reply.DecisionVoto = false
			reply.MandatoActual = nr.MandatoActual
		}
	} else {
		nr.MandatoActual = peticion.Mandato
		//nr.CandidatoVotado = IntNOINICIALIZADO
		nr.CandidatoVotado = peticion.IdCandidato
		reply.DecisionVoto = true
		reply.MandatoActual = nr.MandatoActual
	}

	return nil
}

type ArgAppendEntries struct {
	// Vuestros datos aqui
}

type Results struct {
	// Vuestros datos aqui
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	// Completar....

	return nil
}

// ----- Metodos/Funciones a utilizar como clientes
//
//

// Ejemplo de código enviarPeticionVoto
//
// nodo int -- indice del servidor destino en nr.nodos[]
//
// args *RequestVoteArgs -- argumentos par la llamada RPC
//
// reply *RequestVoteReply -- respuesta RPC
//
// Los tipos de argumentos y respuesta pasados a CallTimeout deben ser
// los mismos que los argumentos declarados en el metodo de tratamiento
// de la llamada (incluido si son punteros
//
// Si en la llamada RPC, la respuesta llega en un intervalo de tiempo,
// la funcion devuelve true, sino devuelve false
//
// la llamada RPC deberia tener un timout adecuado.
//
// Un resultado falso podria ser causado por una replica caida,
// un servidor vivo que no es alcanzable (por problemas de red ?),
// una petición perdida, o una respuesta perdida
//
// Para problemas con funcionamiento de RPC, comprobar que la primera letra
// del nombre  todo los campos de la estructura (y sus subestructuras)
// pasadas como parametros en las llamadas RPC es una mayuscula,
// Y que la estructura de recuperacion de resultado sea un puntero a estructura
// y no la estructura misma.
func (nr *NodoRaft) enviarPeticionVoto(nodo int, args *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) bool {

	// Completar....

	return true
}
