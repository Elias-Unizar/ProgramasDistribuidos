// Escribir vuestro código de funcionalidad Raft en este fichero
//
// COMENTAR IDEMPOTENCIA EN OPERACIONES, LAS OPERACIONES EN SI SON IDEM
// PERO EN DISTRIBUIDOS LOS CLIENTES QUIEREN QUE GUARDE SU VALOR CORRECTAMENTE
// CANAL POR CLIENTE
// DUDAS SOBRE: ENVIAR CONSTANTEMENTE RPC A SEGUIDORES, O CADA X TIEMPO ESTA BIEN
// EL TICKER NO SE PUEDE REINICIAR....
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
	WAIT_INICIO_MS = 3000
	//milisegundos base sobre los que luego se aplicara un plus random para que
	//los timeouts puedan salir de livelocks?
	TIMEOUT_RTT_RPC         = 20
	FRECUENCIA_LATIDO_MS    = 65  // 15 latidos por segundo
	TIMEOUTBASE_LATIDO_MS   = 130 //Dar de margen 2 latidos al lider +
	RANGO_RAND_LATIDO       = 20  // + RTT con valor random
	TIMEOUTBASE_ELECCION_MS = 150 //Junto con 80ms de maximo de random
	RANGO_RAND_ELECCION     = 70
	TIMEOUT_GLOBAL_MS       = 2500

	FRECUENCIA_APLICAR_NUEVA_OP_MS = 100
	FRECUENCIA_COMPROMETER_MS      = 10 //Todavia sin usar, es para
	//fallos con AppendEntries
	//Un candidato tendra tiempo en principio de votar a otros procesos en
	//elecciones si no sale elegido, antes de que esos procesos reinicien eleccion
	//IMPORTANTE: RTT << TIMEOUT_LATIDO/TIMEOUT_ELECCION << FALLOS SISTEMA

	// Constante para fijar valor entero no inicializado
	IntNOINICIALIZADO = -1

	//  false deshabilita por completo los logs de depuracion
	// Aseguraros de poner kEnableDebugLogs a false antes de la entrega
	kEnableDebugLogs = false

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

type OperacionAplicada struct {
	Indice int
	Valor  string
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
	//El HayLider sirve, para enviar al idLider, si sabemos quien es con seguridad
	candidatoHayLider chan bool
	candidatoSoyLider chan bool
}
type infoLider struct {
	SiguienteIdceEnviar_seguidor []int
	IdceReplicado_seguidor       []int
	CanalesClientesActivos       map[int](chan OperacionAplicada)
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
	CanalRecibirResultado chan OperacionAplicada
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
	infoLid infoLider

	mapaClaveValor map[string]string
}

func timerRand(tiempoBase int64, rangoRand int64) time.Duration {
	randInc, _ := rand.Int(rand.Reader, big.NewInt(rangoRand))
	return time.Duration(tiempoBase+randInc.Int64()) * time.Millisecond
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
	reiniciarTimer(nr.misDatosEstado.tmrOut, timerRand(FRECUENCIA_LATIDO_MS, 0))
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
	nr.Mux.Lock()
	nr.IdLider = nr.Yo
	nr.misDatosEstado.miEstado = LIDER
	nr.infoLid.IdceReplicado_seguidor = make([]int, len(nr.Nodos))
	nr.infoLid.SiguienteIdceEnviar_seguidor = make([]int, len(nr.Nodos))
	for i := range nr.infoLid.SiguienteIdceEnviar_seguidor {
		nr.infoLid.SiguienteIdceEnviar_seguidor[i] = nr.UltimoIndiceRegistro + 1
	}
	nr.infoLid.CanalesClientesActivos = make(map[int]chan OperacionAplicada)
	nr.Mux.Unlock()
	enviarLatidosVacios(nr)
}

func contarEntradaCompSeguidores(nr *NodoRaft, N int) bool {
	var totalNodosComp int = 1 //Yo mismo, el lider
	var totalNodos int = len(nr.Nodos)
	for i := 0; i < totalNodos; i++ {
		if nr.infoLid.IdceReplicado_seguidor[i] >= N {
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

func obtenerOperacionesAplicar(nr *NodoRaft) []EntradaRegistro {
	var operacionesParaAplicar []EntradaRegistro
	//Asi no abusamos del mutex, de todas formas las operaciones deben de
	//ser secuenciales
	nr.Mux.Lock()
	uApl := nr.UltimoAplicado
	uComp := nr.UltimoComprometido
	nr.Mux.Unlock()
	if uApl < uComp {
		operacionesParaAplicar =
			(nr.RegistroOperaciones[uApl+1 : uComp+1])
	}
	return operacionesParaAplicar
}

func obtenerResultadosServidor(nr *NodoRaft) []OperacionAplicada {
	hayMasEntradas := true
	resultados := make([]OperacionAplicada, 0)
	for hayMasEntradas {
		select {
		case resultado := <-nr.CanalRecibirResultado:
			resultados = append(resultados, resultado)
		default:
			hayMasEntradas = false
		}
	}
	return resultados
}

func comprobarYEnviarACliente(nr *NodoRaft, op OperacionAplicada) {
	canalCliente, existe := nr.infoLid.CanalesClientesActivos[op.Indice]
	if existe {
		canalCliente <- op
	}
}

func (nr *NodoRaft) aplicarEntradasComprometidas() {
	for {
		operacionesParaAplicar := obtenerOperacionesAplicar(nr)
		for _, entrada := range operacionesParaAplicar {
			nr.UltimoAplicado += 1
			nr.CanalAplicarOperacion <- AplicaOperacion{
				Indice: nr.UltimoAplicado, Operacion: entrada.Operacion}
		}
		resultadosOp := obtenerResultadosServidor(nr)
		if nr.misDatosEstado.miEstado == LIDER {
			for _, resultadoCliente := range resultadosOp {
				comprobarYEnviarACliente(nr, resultadoCliente)
			}
		}
		time.Sleep(FRECUENCIA_APLICAR_NUEVA_OP_MS * time.Millisecond)
	}
}

func tareasLider(finLider chan bool, nr *NodoRaft) {
	for {
		select {
		case <-finLider:
			return
		default:
			comprobarNuevoCompr(nr)
			appendEntriesToAll(nr)
			time.Sleep(FRECUENCIA_COMPROMETER_MS * time.Millisecond)
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
	<-nr.misDatosEstado.tmrOut.C
	nr.Mux.Lock()
	nr.misDatosEstado.miEstado = CANDIDATO
	nr.IdLider = IntNOINICIALIZADO
	nr.Mux.Unlock()
}

func (nr *NodoRaft) accionesCandidato() {
	tiempoLimite := time.NewTimer(TIMEOUT_GLOBAL_MS * time.Millisecond)
	go iniciarProcesoEleccion(nr)
	for {
		//Inicializa el timeout aleatorio
		t := timerRand(TIMEOUTBASE_ELECCION_MS, RANGO_RAND_ELECCION)
		reiniciarTimer(nr.misDatosEstado.tmrOut, t)

		//Quien sabe si justo al empezar la eleccion, ya se habia elegido un
		//nuevo lider
		select {
		case <-nr.misDatosEstado.tmrOut.C:
			go iniciarProcesoEleccion(nr)
		case <-nr.misDatosEstado.candidatoHayLider:
			//Cambiar a seguidor, el proceso se hizo cuando se envio al canal
			return
		case <-nr.misDatosEstado.candidatoSoyLider:
			//Podria ser mas robusto comprobando si es un falso o un true
			inicializarLider(nr)
			return
		case <-tiempoLimite.C:
			os.Exit(1)
		}
	}
}

func (nr *NodoRaft) accionesLider() {
	finLider := make(chan bool, 1)
	go tareasLider(finLider, nr)
	for {
		select {
		case <-nr.misDatosEstado.tmrOut.C:
			enviarLatidosVacios(nr)
		case <-nr.misDatosEstado.candidatoHayLider:
			finLider <- true
			return
		}
	}
}

func (nr *NodoRaft) monitorizadorEstadosNodo() {
	//Retrasar ejecucion para el primer test
	time.Sleep(WAIT_INICIO_MS * time.Millisecond)
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
	nr.CanalRecibirResultado = make(chan OperacionAplicada)
	nr.misDatosEstado.tmrOut = time.NewTimer(0)
	go nr.monitorizadorEstadosNodo()
	go nr.aplicarEntradasComprometidas()
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
func (nr *NodoRaft) obtenerEstado() (int, int, bool, int, int, int) {
	nr.Mux.Lock()
	var yo int = nr.Yo
	var mandato int = nr.MandatoActual
	var esLider bool = yo == nr.IdLider
	var idLider int = nr.IdLider
	var indCompr int = nr.UltimoComprometido
	var indAplc int = nr.UltimoAplicado
	nr.Mux.Unlock()

	return yo, mandato, esLider, idLider, indCompr, indAplc
}

type someterRespuesta struct {
	indice         int
	mandato        int
	EsLider        bool
	idLider        int
	valorADevolver string
}

func esperarResultadoOp(nr *NodoRaft, canal int) string {
	appendEntriesToAll(nr)
	result := <-nr.infoLid.CanalesClientesActivos[canal]
	return result.Valor
}

// VITAAAAAL:::: SE LLAMAN SIEMPRE CON MUTEX ACTIVO
func someterOpLider(nr *NodoRaft, op TipoOperacion) (resp someterRespuesta) {
	nr.UltimoIndiceRegistro += 1
	nr.RegistroOperaciones = append(nr.RegistroOperaciones,
		EntradaRegistro{Operacion: op, Mandato: nr.MandatoActual})
	nr.infoLid.CanalesClientesActivos[resp.indice] =
		make(chan OperacionAplicada, 1)
	nr.Mux.Unlock()
	resp.valorADevolver = esperarResultadoOp(nr, resp.indice)
	return
}

// VITAAAAAL:::: SE LLAMAN SIEMPRE CON MUTEX ACTIVO
func someterOpSeguidor(nr *NodoRaft) (resp someterRespuesta) {
	resp.valorADevolver = ""
	nr.Mux.Unlock()
	return
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
	var resp someterRespuesta
	nr.Mux.Lock() //el mutex se suelta adecuadamente en las funciones de abajo
	resp.indice = nr.UltimoIndiceRegistro + 1
	resp.mandato = nr.MandatoActual
	resp.idLider = nr.IdLider
	resp.EsLider = nr.Yo == nr.IdLider
	if resp.EsLider {
		resp = someterOpLider(nr, operacion)
	} else {
		resp = someterOpSeguidor(nr)
	}

	return resp.indice, resp.mandato, resp.EsLider,
		resp.idLider, resp.valorADevolver
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
	Mandato   int
	EsLider   bool
	IdLider   int
	IndcCompr int
	IndcAplc  int
}

type EstadoRemoto struct {
	IdNodo int
	EstadoParcial
}

func (nr *NodoRaft) ObtenerEstadoNodo(args Vacio, reply *EstadoRemoto) error {
	reply.IdNodo, reply.Mandato, reply.EsLider, reply.IdLider,
		reply.IndcCompr, reply.IndcAplc = nr.obtenerEstado()
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

func (nr *NodoRaft) otorgarVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) {
	nr.CandidatoVotado = peticion.IdCandidato
	reply.DecisionVoto = true
	if nr.misDatosEstado.miEstado != SEGUIDOR {
		//No enviamos el id de la peticion, porque todavia no es lider
		nr.misDatosEstado.candidatoHayLider <- true
		nr.misDatosEstado.miEstado = SEGUIDOR
		nr.IdLider = IntNOINICIALIZADO
	}
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
}

func comprobarCondicionesVoto(nr *NodoRaft, peticion *ArgsPeticionVoto) bool {
	if nr.CandidatoVotado != IntNOINICIALIZADO &&
		nr.CandidatoVotado != peticion.IdCandidato {
		return false
	}
	if peticion.UltimoIndiceMandato < nr.MandatoActual {
		return false
	}
	return peticion.UltimoIndiceMandato > nr.MandatoActual ||
		peticion.UltimoIndiceRegistro >= nr.UltimoIndiceRegistro
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	nr.Mux.Lock()
	if nr.MandatoActual > peticion.Mandato {
		reply.DecisionVoto = false
	} else if nr.MandatoActual == peticion.Mandato {
		if !comprobarCondicionesVoto(nr, peticion) {
			reply.DecisionVoto = false
		} else {
			nr.otorgarVoto(peticion, reply)
		}
	} else { //nr.MandatoActual < peticion.Mandato
		nr.MandatoActual = peticion.Mandato
		//nr.CandidatoVotado = IntNOINICIALIZADO
		nr.otorgarVoto(peticion, reply)
	}
	reply.MandatoActual = nr.MandatoActual
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
		nuevo := args.UltIndiceComprom
		if ultimoNuevo < nuevo { //No ha enviado todos los comp.
			nuevo = ultimoNuevo
		}
		if nuevo > nr.UltimoComprometido {
			nr.UltimoComprometido = nuevo
		}
	}
}

func actualizarInfoLider(nr *NodoRaft, args *ArgAppendEntries) {
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
	if nr.misDatosEstado.miEstado != SEGUIDOR {
		nr.misDatosEstado.candidatoHayLider <- true
		nr.misDatosEstado.miEstado = SEGUIDOR
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
func construirArgumentosVotos(nr *NodoRaft) (args *ArgsPeticionVoto,
	totalN int) {
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
			//Si me responden que no puedo ser lider por el mandato, dejo de intentarlo
			nr.misDatosEstado.candidatoHayLider <- true
			nr.misDatosEstado.miEstado = SEGUIDOR
			nr.IdLider = IntNOINICIALIZADO
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
	prevLogIndex := nr.infoLid.SiguienteIdceEnviar_seguidor[nodo] - 1
	args = ArgAppendEntries{nr.MandatoActual, nr.IdLider, prevLogIndex,
		nr.RegistroOperaciones[prevLogIndex].Mandato, nr.UltimoComprometido,
		[]EntradaRegistro{}}
	return
}

func constrArgsApndEntr(nr *NodoRaft, nodo int) (args ArgAppendEntries) {
	nr.Mux.Lock()
	args = constrArgApndEntrNoNuevo(nr, nodo)
	//Empieza vacio el registro de nuevas entradas cada llamada
	desde := nr.infoLid.SiguienteIdceEnviar_seguidor[nodo]
	args.NuevaEntradasReg = append(args.NuevaEntradasReg,
		nr.RegistroOperaciones[desde:]...)
	nr.Mux.Unlock()
	return
}

func comprobarResultadoAppendEntries_fallo(nr *NodoRaft, nodo int,
	resultado *Results) {
	if resultado.MandatoActual > nr.MandatoActual {
		nr.MandatoActual = resultado.MandatoActual
		nr.CandidatoVotado = IntNOINICIALIZADO
		//Un seguidor tiene mayor mandato que yo, no se si es el lider
		nr.misDatosEstado.candidatoHayLider <- true
		nr.misDatosEstado.miEstado = SEGUIDOR
		nr.IdLider = IntNOINICIALIZADO
	} else {
		nr.infoLid.SiguienteIdceEnviar_seguidor[nodo] -= 1
	}
}

func comprobarResultadoAppendEntries_exito(nr *NodoRaft, nodo int,
	argumentos *ArgAppendEntries) {
	indEntAñad := argumentos.UltIndxAnt + len(argumentos.NuevaEntradasReg)
	if indEntAñad > nr.infoLid.IdceReplicado_seguidor[nodo] {
		nr.infoLid.SiguienteIdceEnviar_seguidor[nodo] = indEntAñad + 1
		nr.infoLid.IdceReplicado_seguidor[nodo] = indEntAñad
	}
}

func comprobarResultadoAppendEntries(nr *NodoRaft, nodo int,
	argumentos *ArgAppendEntries, resultado *Results) {
	nr.Mux.Lock()
	if nr.misDatosEstado.miEstado == LIDER &&
		nr.MandatoActual == argumentos.MandatoLider {
		if !resultado.Exito {
			comprobarResultadoAppendEntries_fallo(nr, nodo, resultado)
		} else {
			comprobarResultadoAppendEntries_exito(nr, nodo, argumentos)
		}
	}
	nr.Mux.Unlock()
}

func enviar_tratarAppendEntries(nr *NodoRaft, nodo int,
	args *ArgAppendEntries) {
	var resultado Results
	if nr.enviarEntradasReg(nodo, args, &resultado) {
		comprobarResultadoAppendEntries(nr, nodo, args, &resultado)
	}
}

func appendEntriesToAll(nr *NodoRaft) {
	reiniciarTimer(nr.misDatosEstado.tmrOut, timerRand(FRECUENCIA_LATIDO_MS, 0))
	totalNodos := len(nr.Nodos)
	for nodoAEnviar := 0; nodoAEnviar < totalNodos; nodoAEnviar++ {
		if nr.Yo != nodoAEnviar {
			args := constrArgsApndEntr(nr, nodoAEnviar)
			go enviar_tratarAppendEntries(nr, nodoAEnviar, &args)
		}
	}
}
