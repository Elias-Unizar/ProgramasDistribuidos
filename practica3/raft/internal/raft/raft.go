// Escribir vuestro código de funcionalidad Raft en este fichero
//
// FUTURO REENVIO CONSTANTE A NODOS PARA COMPROMETER
// SOMETER OPERACION DEVUELVE RESULTADO
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
	"math/big"
	"os"

	"crypto/rand"
	"sync"
	"time"

	//"net/rpc"

	"raft/internal/comun/rpctimeout"
)

// Tomando la practica 1 como ejemplo, y que la media del RTT estaba en 1.5-2ms
// He decidido que el tiempo que tarda el lider en hacer un "broadcast" y recibir
// la respuesta de los latidos está en torno a los 5ms, y el tiempo de expiracion
// del latido en un seguidor debe ser un orden de magnitud por encima, por tanto
// situo su minimo en unos 60ms, subiendolo a 80ms, para tener un margen respecto
// a la frecuencia de latidos del lider(3 veces un RTT)
const (
	//milisegundos base sobre los que luego se aplicara un plus random para que
	//los timeouts puedan salir de livelocks?
	TIMEOUT_RTT_RPC         = 20
	FRECUENCIA_LATIDO_MS    = 65  // 15 latidos por segundo
	TIMEOUTBASE_LATIDO_MS   = 130 //Dar de margen 2 latidos al lider +
	RANGO_RAND_LATIDO       = 20  // + RTT con valor random
	TIMEOUTBASE_ELECCION_MS = 150 //Junto con 80ms de maximo de random
	RANGO_RAND_ELECCION     = 70
	TIMEOUT_GLOBAL_MS       = 2500
	COOLDOWN_APPEND_PESADO  = 10 //Todavia sin usar, es para
	//fallos con AppendEntries
	//Un candidato tendra tiempo en principio de votar a otros procesos en
	//elecciones si no sale elegido, antes de que esos procesos reinicien eleccion
	//IMPORTANTE: RTT << TIMEOUT_LATIDO/TIMEOUT_ELECCION << FALLOS SISTEMA

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

type datosMaquinaEstados struct {
	miEstado string
	//El timer para poder esperar un timeout
	tmrOut *time.Timer
	//Canales para salir de la espera en la maquina de estados
	candidatoHayLider chan bool
	candidatoSoyLider chan bool
}
type infoSeguidores struct {
	SiguienteIdceEnviar_seguidor []int
	IdceReplicado_seguidor       []int
}

// Tipo de dato Go que representa un solo nodo (réplica) de raft
type NodoRaft struct {
	Mux sync.Mutex // Mutex para proteger acceso a estado compartido

	// Host:Port de todos los nodos (réplicas) Raft, en mismo orden
	Nodos []rpctimeout.HostPort
	Yo    int // indice de este nodos en campo array "nodos"

	IdLider       int
	MandatoActual int
	// Utilización opcional de este logger para depuración
	// Cada nodo Raft tiene su propio registro de trazas (logs)
	Logger *log.Logger

	CanalAplicarOperacion chan AplicaOperacion
	CanalRecibirResultado chan string
	//Si se ha votado a alguien en el mandato actual tiene
	//su id, si no tendra el valor de IntNOINICIALIZADO
	CandidatoVotado int
	misDatosEstado  datosMaquinaEstados

	//El registro con entradas que contienen mandato y operacion
	RegistroOperaciones  []EntradaRegistro
	UltimoIndiceRegistro int
	UltimoComprometido   int
	UltimoAplicado       int

	// Datos que usará el nodo en caso de que se vuelva lider
	// Contiene información de todos los nodos de la red(de los que pueda tener)
	infoSeg infoSeguidores
}

func timerRand(tiempoBase int64, rangoRand int64) time.Duration {
	randInc, _ := rand.Int(rand.Reader, big.NewInt(rangoRand))
	return time.Duration(tiempoBase+randInc.Int64()) * time.Millisecond
}

func reiniciarCanales(nr *NodoRaft) {
	nr.Mux.Lock()
	nr.misDatosEstado.candidatoHayLider = make(chan bool, 1)
	nr.misDatosEstado.candidatoSoyLider = make(chan bool, 1)
	nr.Mux.Unlock()
}

func reiniciarTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

func enviarLatidosVacios(nr *NodoRaft) {
	totalNodos := len(nr.Nodos)
	for nodoAEnviar := 0; nodoAEnviar < totalNodos; nodoAEnviar++ {
		if nr.Yo != nodoAEnviar {
			nr.Mux.Lock()
			args := constrArgApndEntrNoNuevo(nr, nodoAEnviar)
			nr.Mux.Unlock()
			go enviar_tratarAppendEntries(nr, nodoAEnviar, &args)
		}
	}
}

// Envia el primer latido vacio,
func inicializarLider(nr *NodoRaft) {
	reiniciarCanales(nr)
	nr.Mux.Lock()
	nr.IdLider = nr.Yo
	nr.misDatosEstado.miEstado = LIDER
	nr.infoSeg.IdceReplicado_seguidor = make([]int, len(nr.Nodos))
	nr.infoSeg.SiguienteIdceEnviar_seguidor = make([]int, len(nr.Nodos))
	for i := range nr.infoSeg.SiguienteIdceEnviar_seguidor {
		nr.infoSeg.SiguienteIdceEnviar_seguidor[i] = nr.UltimoIndiceRegistro + 1
	}
	nr.Mux.Unlock()
	enviarLatidosVacios(nr)
}

func contarEntradaCompSeguidores(nr *NodoRaft, N int) bool {
	var totalNodosComp int = 1 //Yo mismo, el lider
	var totalNodos int = len(nr.Nodos)
	for i := 0; i < totalNodos; i++ {
		if nr.infoSeg.IdceReplicado_seguidor[i] >= N {
			totalNodosComp += 1
		}
	}
	return totalNodosComp >= (totalNodos/2)+1
}

func comprobarNuevoCompr(nr *NodoRaft) {
	nr.Mux.Lock()
	defer nr.Mux.Unlock()
	for N := nr.UltimoIndiceRegistro; N > nr.UltimoComprometido; N-- {
		if nr.RegistroOperaciones[N].Mandato == nr.MandatoActual {
			if contarEntradaCompSeguidores(nr, N) {
				nr.UltimoComprometido = N
				return
			}
		} else {
			return
		}
	}
}

func tareasLider(finLider chan bool, nr *NodoRaft) {
	for {
		select {
		case <-finLider:
			return
		default:
			//appendEntriesToAll(nr)
			comprobarNuevoCompr(nr)
			time.Sleep(COOLDOWN_APPEND_PESADO * time.Millisecond)
		}
	}
}

// En RCP de append, ver si enviar por el canal que hayLider
// En RCP de votar, avisa por un canal si gana
// yy ya estaria
func (nr *NodoRaft) accionesSeguidor() {
	//Reiniciamos el timer si acabamos de volvernos seguidor, este timer
	//se resetea al mismo valor con cada appendEntries
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
	//Siempre estará aqui bloqueado, hasta que no exista un lider
	nr.Logger.Printf("Empiezo como SEGUIDOR: %d", nr.Yo)
	<-nr.misDatosEstado.tmrOut.C
	nr.Logger.Printf("Oh ooooohhhhh TIMEOUT!!!! Quiero ser presidente: %d", nr.Yo)
	nr.Mux.Lock()
	nr.misDatosEstado.miEstado = CANDIDATO
	nr.IdLider = IntNOINICIALIZADO
	nr.Mux.Unlock()
	nr.Logger.Printf("Oficialmente soy CANDIDATO: %d", nr.Yo)
}

func (nr *NodoRaft) accionesCandidato() {
	tiempoLimite := time.NewTimer(TIMEOUT_GLOBAL_MS * time.Millisecond)
	reiniciarCanales(nr)
	nr.Logger.Printf("Y si adelanto las elecciones generales?: %d", nr.Yo)
	go iniciarProcesoEleccion(nr)
	for {
		//Inicializa el timeout aleatorio
		t := timerRand(TIMEOUTBASE_ELECCION_MS, RANGO_RAND_ELECCION)
		reiniciarTimer(nr.misDatosEstado.tmrOut, t)

		//Quien sabe si justo al empezar la eleccion, ya se habia elegido un
		//nuevo lider
		select {
		case <-nr.misDatosEstado.tmrOut.C:
			nr.Logger.Printf("Mi eleccion ha caducado: %d, iniciare otra", nr.Yo)
			go iniciarProcesoEleccion(nr)
		case <-nr.misDatosEstado.candidatoHayLider:
			nr.Logger.Printf("No gane la eleccion, hay otro lider: %d", nr.Yo)
			//Cambiar a seguidor, el canal se activa desde AppendEntries
			nr.Mux.Lock()
			nr.misDatosEstado.miEstado = SEGUIDOR
			nr.Mux.Unlock()
			return
		case <-nr.misDatosEstado.candidatoSoyLider:
			nr.Logger.Printf("HE GANADOOOO LA ELECCION: %d", nr.Yo)
			//Podria ser mas robusto comprobando si es un falso o un true
			inicializarLider(nr)
			return
		case <-tiempoLimite.C:
			nr.Logger.Printf("Sin lideres la vida no tiene sentido....: %d", nr.Yo)
			os.Exit(1)
		}
	}
}

func (nr *NodoRaft) accionesLider() {
	finLider := make(chan bool, 1)
	go tareasLider(finLider, nr)
	tckrChan := time.Tick(FRECUENCIA_LATIDO_MS * time.Millisecond)
	for {
		select {
		case <-tckrChan:
			enviarLatidosVacios(nr)
		case <-nr.misDatosEstado.candidatoHayLider:
			finLider <- true
			nr.Mux.Lock()
			nr.misDatosEstado.miEstado = SEGUIDOR
			nr.Mux.Unlock()
			return
		}
	}
}

func (nr *NodoRaft) monitorizadorEstadosNodo() {
	time.Sleep(4100 * time.Millisecond) // SOlo inicio ejecucion para TEST.........
	for {
		//Necesito leer el estado, y con tanta concurrencia mejor un mutex
		switch nr.misDatosEstado.miEstado {
		case SEGUIDOR:
			nr.accionesSeguidor()
		case LIDER:
			nr.accionesLider()
		case CANDIDATO:
			nr.accionesCandidato()
		}
	}
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

func iniciarEstadoNodo(nr *NodoRaft) {
	nr.misDatosEstado.miEstado = SEGUIDOR
	nr.misDatosEstado.candidatoHayLider = make(chan bool, 1)
	nr.misDatosEstado.candidatoSoyLider = make(chan bool, 1)
}

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

func NuevoNodo(nodos []rpctimeout.HostPort, yo int,
	canalAplicarOperacion chan AplicaOperacion) *NodoRaft {
	nr := &NodoRaft{}
	nr.Nodos = nodos
	nr.Yo = yo
	nr.IdLider = IntNOINICIALIZADO
	nr.Mux = sync.Mutex{}
	nr.CanalAplicarOperacion = canalAplicarOperacion
	iniciarEstadoNodo(nr)
	crearLoggerNodo(nr, nodos, yo)

	// Añadir codigo de inicialización
	nr.MandatoActual = 0
	nr.CandidatoVotado = IntNOINICIALIZADO

	nr.RegistroOperaciones = []EntradaRegistro{{Mandato: 0}}
	nr.UltimoIndiceRegistro = 0
	nr.UltimoComprometido = 0
	nr.UltimoAplicado = 0

	nr.misDatosEstado.tmrOut = time.NewTimer(0)
	go nr.monitorizadorEstadosNodo()
	nr.Logger.Printf("He acabado de ejecutar mi codigo de inicio: %d", nr.Yo)
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
	nr.Mux.Lock()
	var yo int = nr.Yo
	var mandato int = nr.MandatoActual
	var esLider bool = yo == nr.IdLider
	var idLider int = nr.IdLider
	nr.Mux.Unlock()

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
	indice := nr.UltimoIndiceRegistro + 1
	mandato := nr.MandatoActual
	idLider := nr.IdLider
	EsLider := nr.Yo == nr.IdLider
	valorADevolver := ""

	if EsLider {
		nr.UltimoIndiceRegistro += 1
		nr.RegistroOperaciones = append(nr.RegistroOperaciones,
			EntradaRegistro{Operacion: operacion, Mandato: nr.MandatoActual})
		nr.Mux.Unlock()
		appendEntriesToAll(nr)
		//posible segundo canal de comunicacion para obtener el resultado de
		//la operacion, en principio el servidor atende clientes de manera
		//secuencial, en cualquier caso se podría realizar un mapa de canales
		//que se accedan desde un idCliente autoasignado¿ para atender con
		//currentemente? No lo voy a pensar ahora
	} else {
		nr.Mux.Unlock()
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
	nr.Logger.Printf("Intentando obtener el estado de %d", nr.Yo)
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
	nr.Logger.Printf("Intentando someter una nueva opracion en %d", nr.Yo)
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

func (nr *NodoRaft) otorgarVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) {
	nr.CandidatoVotado = peticion.IdCandidato
	reply.DecisionVoto = true
	if nr.misDatosEstado.miEstado != SEGUIDOR {
		nr.misDatosEstado.candidatoHayLider <- true
		nr.IdLider = IntNOINICIALIZADO
	}
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	nr.Logger.Printf("Soy %d, y %d me pide votarle, mandato: %d", nr.Yo, peticion.IdCandidato, peticion.Mandato)
	nr.Mux.Lock()
	if nr.MandatoActual > peticion.Mandato {
		reply.DecisionVoto = false
	} else if nr.MandatoActual < peticion.Mandato {
		nr.MandatoActual = peticion.Mandato
		//nr.CandidatoVotado = IntNOINICIALIZADO
		nr.otorgarVoto(peticion, reply)
	} else if nr.CandidatoVotado == IntNOINICIALIZADO ||
		nr.CandidatoVotado == peticion.IdCandidato {
		nr.otorgarVoto(peticion, reply)
	} else {
		reply.DecisionVoto = false
	}
	reply.MandatoActual = nr.MandatoActual
	nr.Logger.Printf("Soy %d, y me parece %v que %d sea buen lider", nr.Yo, reply.DecisionVoto, peticion.IdCandidato)
	nr.Mux.Unlock()
	return nil
}

type ArgAppendEntries struct {
	MandatoLider      int
	IdLider           int
	UltIndxAnt        int
	UltIndxAntMandato int
	UltIndiceComprom  int
	NuevaEntradasReg  []EntradaRegistro
}

type Results struct {
	MandatoActual int
	Exito         bool
}

func mismoMandatoEnUltimoIndice(nr *NodoRaft, args *ArgAppendEntries) bool {
	return nr.RegistroOperaciones[args.UltIndxAnt].Mandato ==
		args.UltIndxAntMandato
}

func actualizarIndiceComprometido(nr *NodoRaft, args *ArgAppendEntries) {
	if args.UltIndiceComprom > nr.UltimoComprometido {
		ultimoNuevo := args.UltIndxAnt + len(args.NuevaEntradasReg)
		minComp := args.UltIndiceComprom
		if ultimoNuevo < args.UltIndiceComprom { //No ha enviado todos los comp.
			minComp = ultimoNuevo
		}
		nr.UltimoComprometido = minComp
	}
}

func actualizarInfoLider(nr *NodoRaft, args *ArgAppendEntries) {
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
	if nr.misDatosEstado.miEstado != SEGUIDOR {
		nr.misDatosEstado.candidatoHayLider <- true
	}
	nr.IdLider = args.IdLider
	if nr.MandatoActual != args.MandatoLider {
		nr.CandidatoVotado = IntNOINICIALIZADO
	}
	nr.MandatoActual = args.MandatoLider
}

// Hecho asi para que en caso de que el lider me envie un slice mas corto
// del que deberia, pero en el que tengo todas las entradas iguales
// no perder datos posteriores (por si fue error de red que llegara tarde un)
// mensaje
func agnadirEntradas(nr *NodoRaft, args *ArgAppendEntries) {
	for i, nueva := range args.NuevaEntradasReg {
		indice := args.UltIndxAnt + 1 + i
		if indice < len(nr.RegistroOperaciones) {
			if nr.RegistroOperaciones[indice].Mandato == nueva.Mandato {
				continue // ya la tengo, no hay nada que hacer
			}
			// Conflicto: borro esta entrada y todas las siguientes
			nr.RegistroOperaciones = nr.RegistroOperaciones[:indice]
		}
		nr.RegistroOperaciones = append(nr.RegistroOperaciones, nueva)
	}
	nr.UltimoIndiceRegistro = len(nr.RegistroOperaciones) - 1
	actualizarIndiceComprometido(nr, args)
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	nr.Logger.Printf("Soy %d, y %d me ha pedido ejecutar AppendEntries", nr.Yo, args.IdLider)
	nr.Mux.Lock()
	if args.MandatoLider < nr.MandatoActual {
		results.Exito = false
	} else {
		actualizarInfoLider(nr, args)
		if nr.UltimoIndiceRegistro < args.UltIndxAnt {
			results.Exito = false
		} else if !mismoMandatoEnUltimoIndice(nr, args) {
			results.Exito = false
		} else {
			results.Exito = true
			agnadirEntradas(nr, args)
		}
	}
	nr.Logger.Printf("Soy %d, y el appendEntries de %d ha tenido exito: %v", nr.Yo, args.IdLider, results.Exito)
	results.MandatoActual = nr.MandatoActual
	nr.Mux.Unlock()
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

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto",
		args, reply, TIMEOUT_RTT_RPC*time.Millisecond)

	return err == nil
}

func (nr *NodoRaft) enviarEntradasReg(nodo int, args *ArgAppendEntries,
	results *Results) bool {

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries",
		args, results, TIMEOUT_RTT_RPC*time.Millisecond)

	return err == nil
}

// Ademas de construir y devolver el struct ArgsPeticionVoto, modifica el
// propio nodoRaft con los datos modificados por empezar la votacion
func construirArgumentosVotos(nr *NodoRaft) (args *ArgsPeticionVoto, totalN int) {
	nr.Mux.Lock()
	nr.MandatoActual += 1
	nr.CandidatoVotado = nr.Yo
	ulInd := nr.UltimoIndiceRegistro
	args = &ArgsPeticionVoto{Mandato: nr.MandatoActual, IdCandidato: nr.Yo,
		UltimoIndiceRegistro: ulInd,
		UltimoIndiceMandato:  nr.RegistroOperaciones[ulInd].Mandato}
	totalN = len(nr.Nodos)
	nr.Mux.Unlock()
	return
}

func tratarReplyVoto(nr *NodoRaft, reply *RespuestaPeticionVoto) int {
	nr.Mux.Lock()
	if nr.MandatoActual < reply.MandatoActual {
		nr.MandatoActual = reply.MandatoActual
		nr.CandidatoVotado = IntNOINICIALIZADO
		if nr.misDatosEstado.miEstado != SEGUIDOR {
			nr.misDatosEstado.candidatoHayLider <- true
		}
	}
	nr.Mux.Unlock()
	if reply.DecisionVoto {
		return 1
	} else {
		return 0
	}
}

// Existe un mutex para incrementar los votos recibidos, es uno local de la
// funcion de votacion para no molestar al nodo entero de raft para incrementar
// un solo numero
func envioYTratamientoVoto(nr *NodoRaft, nodoAEnviar int,
	args *ArgsPeticionVoto, chVotoRecibido chan int) {

	var reply RespuestaPeticionVoto
	if nr.enviarPeticionVoto(nodoAEnviar, args, &reply) {
		//Sea cual sea el reply, fue valido
		chVotoRecibido <- tratarReplyVoto(nr, &reply)
	} else {
		chVotoRecibido <- 0
	} //Aqui fallo la llamada RPC, no podriamos revisar el reply
}

func comprobarResultadoEleccion(nr *NodoRaft, args *ArgsPeticionVoto) {
	nr.Mux.Lock()
	if nr.MandatoActual == args.Mandato &&
		nr.misDatosEstado.miEstado == CANDIDATO {
		nr.misDatosEstado.candidatoSoyLider <- true
	}
	nr.Mux.Unlock()
}

// Se podria mejorar para que parara la eleccion si ya sabe que va a perder
func iniciarProcesoEleccion(nr *NodoRaft) {
	args, totalNodos := construirArgumentosVotos(nr)
	var totalVotos int = 1 //Me autovoto
	chVotoRecbdo := make(chan int, totalNodos)
	for nodoAEnviar := 0; nodoAEnviar < totalNodos; nodoAEnviar++ {
		if nr.Yo != nodoAEnviar {
			go envioYTratamientoVoto(nr, nodoAEnviar, args, chVotoRecbdo)
		}
	}
	var yaComprobado bool = false
	for i := 0; i < totalNodos-1; i++ {
		totalVotos += <-chVotoRecbdo
		if !yaComprobado && (totalVotos >= (totalNodos/2 + 1)) {
			yaComprobado = true
			comprobarResultadoEleccion(nr, args)
		}
	}
}

func constrArgApndEntrNoNuevo(nr *NodoRaft, nodo int) (args ArgAppendEntries) {
	prevLogIndex := nr.infoSeg.SiguienteIdceEnviar_seguidor[nodo] - 1
	args = ArgAppendEntries{nr.MandatoActual, nr.IdLider, prevLogIndex,
		nr.RegistroOperaciones[prevLogIndex].Mandato, nr.UltimoComprometido,
		[]EntradaRegistro{}}
	return
}

func constrArgsApndEntr(nr *NodoRaft, nodo int) (args ArgAppendEntries) {
	nr.Mux.Lock()
	args = constrArgApndEntrNoNuevo(nr, nodo)
	//Empieza vacio el registro de nuevas entradas cada llamada
	args.NuevaEntradasReg = append(args.NuevaEntradasReg,
		nr.RegistroOperaciones[nr.infoSeg.SiguienteIdceEnviar_seguidor[nodo]:]...)
	nr.Mux.Unlock()
	return
}

func comprobarResultadoAppendEntries(nr *NodoRaft, nodo int,
	argumentos *ArgAppendEntries, resultado *Results) {
	nr.Mux.Lock()
	if nr.misDatosEstado.miEstado == LIDER {
		if !resultado.Exito {
			if resultado.MandatoActual > nr.MandatoActual {
				nr.MandatoActual = resultado.MandatoActual
				nr.CandidatoVotado = IntNOINICIALIZADO
				nr.misDatosEstado.miEstado = SEGUIDOR
				nr.misDatosEstado.candidatoHayLider <- true
			} else {
				nr.infoSeg.SiguienteIdceEnviar_seguidor[nodo] -= 1
			}
		} else {
			entAñad := argumentos.UltIndxAnt + len(argumentos.NuevaEntradasReg)
			nr.infoSeg.SiguienteIdceEnviar_seguidor[nodo] = entAñad + 1
			nr.infoSeg.IdceReplicado_seguidor[nodo] = entAñad
		}
	}
	nr.Mux.Unlock()
}

func enviar_tratarAppendEntries(nr *NodoRaft, nodo int, args *ArgAppendEntries) {
	var resultado Results
	if nr.enviarEntradasReg(nodo, args, &resultado) {
		comprobarResultadoAppendEntries(nr, nodo, args, &resultado)
	}
}

func appendEntriesToAll(nr *NodoRaft) {
	totalNodos := len(nr.Nodos)
	for nodoAEnviar := 0; nodoAEnviar < totalNodos; nodoAEnviar++ {
		if nr.Yo != nodoAEnviar {
			args := constrArgsApndEntr(nr, nodoAEnviar)
			go enviar_tratarAppendEntries(nr, nodoAEnviar, &args)
		}
	}
}
