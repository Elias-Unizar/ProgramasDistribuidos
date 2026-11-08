// Escribir vuestro código de funcionalidad Raft en este fichero
//
// COMENTAR IDEMPOTENCIA EN OPERACIONES, LAS OPERACIONES EN SI SON IDEM
// PERO EN DISTRIBUIDOS LOS CLIENTES QUIEREN QUE GUARDE SU VALOR CORRECTAMENTE
// CANAL POR CLIENTE
// DUDAS SOBRE: ENVIAR CONSTANTEMENTE RPC A SEGUIDORES, O CADA X TIEMPO ESTA BIEN
// EL TICKER NO SE PUEDE REINICIAR....
package raftLog

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
	nr.Logger.Printf("[LATIDO] N%d envia ronda de latidos vacios", nr.Yo)
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
	nr.Logger.Printf("[ESTADO] N%d -> LIDER: mandato=%d ultimoIdx=%d "+
		"comprometido=%d nextIdx inicial=%d", nr.Yo, nr.MandatoActual,
		nr.UltimoIndiceRegistro, nr.UltimoComprometido,
		nr.UltimoIndiceRegistro+1)
	nr.Mux.Unlock()
	nr.Logger.Printf("[LATIDO] N%d envia el primer latido como lider", nr.Yo)
	enviarLatidosVacios(nr)
}

func contarEntradaCompSeguidores(nr *NodoRaft, N int) bool {
	var totalNodosComp int = 1 //Yo mismo, el lider
	var totalNodos int = len(nr.Nodos)
	for i := 0; i < totalNodos; i++ {
		if nr.infoLid.IdceReplicado_seguidor[i] >= N {
			totalNodosComp += 1
			nr.Logger.Printf("[COMPROMISO] entrada %d replicada en N%d "+
				"(matchIdx=%d)", N, i, nr.infoLid.IdceReplicado_seguidor[i])
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
				nr.Logger.Printf("[COMPROMISO] N%d lider compromete la "+
					"entrada %d (comprometido antes=%d, mandato=%d)",
					nr.Yo, N, nr.UltimoComprometido, nr.MandatoActual)
				nr.UltimoComprometido = N
				return
			}
		} else {
			nr.Logger.Printf("[COMPROMISO] N%d no compromete la entrada %d: "+
				"mandato entrada=%d, mandato actual=%d", nr.Yo, N,
				nr.RegistroOperaciones[N].Mandato, nr.MandatoActual)
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
		nr.Logger.Printf("[APLICAR] N%d entradas pendientes de aplicar: "+
			"%d..%d", nr.Yo, nr.UltimoAplicado+1, nr.UltimoComprometido)
	}
	return operacionesParaAplicar
}

func obtenerResultadosServidor(nr *NodoRaft) []OperacionAplicada {
	hayMasEntradas := true
	resultados := make([]OperacionAplicada, 0)
	for hayMasEntradas {
		select {
		case resultado := <-nr.CanalRecibirResultado:
			nr.Logger.Printf("[APLICAR] N%d resultado del servidor: "+
				"indice=%d valor=%q", nr.Yo, resultado.Indice, resultado.Valor)
			resultados = append(resultados, resultado)
		default:
			hayMasEntradas = false
		}
	}
	return resultados
}

func comprobarYEnviarACliente(nr *NodoRaft, op OperacionAplicada) {
	canalCliente, existe := nr.infoLid.CanalesClientesActivos[op.Indice]
	nr.Logger.Printf("[CLIENTE] N%d resultado indice=%d valor=%q, cliente "+
		"esperando=%v", nr.Yo, op.Indice, op.Valor, existe)
	if existe {
		canalCliente <- op
	}
}

func (nr *NodoRaft) aplicarEntradasComprometidas() {
	nr.Logger.Printf("[APLICAR] N%d hilo de aplicacion iniciado", nr.Yo)
	for {
		operacionesParaAplicar := obtenerOperacionesAplicar(nr)
		for _, entrada := range operacionesParaAplicar {
			nr.UltimoAplicado += 1
			nr.Logger.Printf("[APLICAR] N%d aplica la entrada %d: %+v",
				nr.Yo, nr.UltimoAplicado, entrada.Operacion)
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
	nr.Logger.Printf("[ESTADO] N%d hilo de tareas de lider iniciado", nr.Yo)
	for {
		select {
		case <-finLider:
			nr.Logger.Printf("[ESTADO] N%d hilo de tareas de lider "+
				"detenido", nr.Yo)
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
	nr.Logger.Printf("[ESTADO] N%d SEGUIDOR: espero latido (timeout "+
		"%d-%dms)", nr.Yo, TIMEOUTBASE_LATIDO_MS,
		TIMEOUTBASE_LATIDO_MS+RANGO_RAND_LATIDO)
	//Reiniciamos el timer si acabamos de volvernos seguidor, este timer
	//se resetea al mismo valor con cada appendEntries
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
	//Siempre estará aqui bloqueado, hasta que no exista un lider
	<-nr.misDatosEstado.tmrOut.C
	nr.Logger.Printf("[ESTADO] N%d SEGUIDOR: timeout de latido sin "+
		"recibir lider", nr.Yo)
	nr.Mux.Lock()
	nr.misDatosEstado.miEstado = CANDIDATO
	nr.IdLider = IntNOINICIALIZADO
	nr.Logger.Printf("[ESTADO] N%d SEGUIDOR -> CANDIDATO (mandato=%d, "+
		"lider borrado)", nr.Yo, nr.MandatoActual)
	nr.Mux.Unlock()
}

func (nr *NodoRaft) accionesCandidato() {
	tiempoLimite := time.NewTimer(TIMEOUT_GLOBAL_MS * time.Millisecond)
	nr.Logger.Printf("[ESTADO] N%d CANDIDATO: empiezo (limite global "+
		"%dms)", nr.Yo, TIMEOUT_GLOBAL_MS)
	go iniciarProcesoEleccion(nr)
	for {
		//Inicializa el timeout aleatorio
		t := timerRand(TIMEOUTBASE_ELECCION_MS, RANGO_RAND_ELECCION)
		reiniciarTimer(nr.misDatosEstado.tmrOut, t)
		nr.Logger.Printf("[ELECCION] N%d espero resultado, timeout de "+
			"eleccion=%v", nr.Yo, t)

		//Quien sabe si justo al empezar la eleccion, ya se habia elegido un
		//nuevo lider
		select {
		case <-nr.misDatosEstado.tmrOut.C:
			nr.Logger.Printf("[ELECCION] N%d eleccion caducada, lanzo "+
				"otra", nr.Yo)
			go iniciarProcesoEleccion(nr)
		case <-nr.misDatosEstado.candidatoHayLider:
			nr.Logger.Printf("[ESTADO] N%d CANDIDATO -> SEGUIDOR (hay "+
				"lider o mandato mayor)", nr.Yo)
			//Cambiar a seguidor, el proceso se hizo cuando se envio al canal
			return
		case <-nr.misDatosEstado.candidatoSoyLider:
			nr.Logger.Printf("[ESTADO] N%d CANDIDATO -> LIDER (senal de "+
				"victoria recibida)", nr.Yo)
			//Podria ser mas robusto comprobando si es un falso o un true
			inicializarLider(nr)
			return
		case <-tiempoLimite.C:
			nr.Logger.Printf("[ESTADO] N%d sin lider tras %dms: finaliza "+
				"el nodo", nr.Yo, TIMEOUT_GLOBAL_MS)
			os.Exit(1)
		}
	}
}

func (nr *NodoRaft) accionesLider() {
	finLider := make(chan bool, 1)
	nr.Logger.Printf("[ESTADO] N%d LIDER: arranco hilo de tareas y latidos",
		nr.Yo)
	go tareasLider(finLider, nr)
	for {
		select {
		case <-nr.misDatosEstado.tmrOut.C:
			enviarLatidosVacios(nr)
		case <-nr.misDatosEstado.candidatoHayLider:
			nr.Logger.Printf("[ESTADO] N%d LIDER -> SEGUIDOR (mandato "+
				"mayor o lider nuevo)", nr.Yo)
			finLider <- true
			return
		}
	}
}

func (nr *NodoRaft) monitorizadorEstadosNodo() {
	//Retrasar ejecucion para el primer test
	time.Sleep(WAIT_INICIO_MS * time.Millisecond)
	nr.Logger.Printf("[ESTADO] N%d monitor de estados en marcha", nr.Yo)
	for {
		//Necesito leer el estado, y con tanta concurrencia mejor un mutex
		switch nr.misDatosEstado.miEstado {
		case SEGUIDOR:
			nr.Logger.Printf("[ESTADO] N%d monitor: ejecuto SEGUIDOR", nr.Yo)
			nr.accionesSeguidor()
		case LIDER:
			nr.Logger.Printf("[ESTADO] N%d monitor: ejecuto LIDER", nr.Yo)
			nr.accionesLider()
		case CANDIDATO:
			nr.Logger.Printf("[ESTADO] N%d monitor: ejecuto CANDIDATO", nr.Yo)
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
	nr.Logger.Printf("[ESTADO] N%d arrancado como SEGUIDOR: mandato=%d "+
		"nodos=%d", nr.Yo, nr.MandatoActual, len(nr.Nodos))
	go nr.monitorizadorEstadosNodo()
	go nr.aplicarEntradasComprometidas()
	return nr
}

// Metodo Para() utilizado cuando no se necesita mas al nodo
//
// Quizas interesante desactivar la salida de depuracion
// de este nodo
func (nr *NodoRaft) para() {
	nr.Logger.Printf("[API] N%d parando: os.Exit en 5ms", nr.Yo)
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
	nr.Logger.Printf("[API] N%d ObtenerEstado: mandato=%d esLider=%v "+
		"lider=%d comprometido=%d aplicado=%d", yo, mandato, esLider,
		idLider, indCompr, indAplc)
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
	nr.Logger.Printf("[CLIENTE] N%d lider espera resultado en el canal %d",
		nr.Yo, canal)
	appendEntriesToAll(nr)
	result := <-nr.infoLid.CanalesClientesActivos[canal]
	nr.Logger.Printf("[CLIENTE] N%d resultado recibido en el canal %d: %q",
		nr.Yo, canal, result.Valor)
	return result.Valor
}

// VITAAAAAL:::: SE LLAMAN SIEMPRE CON MUTEX ACTIVO
func someterOpLider(nr *NodoRaft, op TipoOperacion) (resp someterRespuesta) {
	nr.UltimoIndiceRegistro += 1
	nr.RegistroOperaciones = append(nr.RegistroOperaciones,
		EntradaRegistro{Operacion: op, Mandato: nr.MandatoActual})
	nr.infoLid.CanalesClientesActivos[resp.indice] =
		make(chan OperacionAplicada, 1)
	nr.Logger.Printf("[CLIENTE] N%d lider anade la entrada %d (mandato=%d): "+
		"%+v; canal registrado con clave %d", nr.Yo, nr.UltimoIndiceRegistro,
		nr.MandatoActual, op, resp.indice)
	nr.Mux.Unlock()
	resp.valorADevolver = esperarResultadoOp(nr, resp.indice)
	return
}

// VITAAAAAL:::: SE LLAMAN SIEMPRE CON MUTEX ACTIVO
func someterOpSeguidor(nr *NodoRaft) (resp someterRespuesta) {
	resp.valorADevolver = ""
	nr.Logger.Printf("[CLIENTE] N%d no soy lider (lider conocido=%d): "+
		"rechazo la operacion", nr.Yo, nr.IdLider)
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
	nr.Logger.Printf("[CLIENTE] N%d someterOperacion %+v: indice "+
		"siguiente=%d esLider=%v mandato=%d", nr.Yo, operacion, resp.indice,
		resp.EsLider, resp.mandato)
	if resp.EsLider {
		resp = someterOpLider(nr, operacion)
	} else {
		resp = someterOpSeguidor(nr)
	}

	nr.Logger.Printf("[CLIENTE] N%d devuelve al cliente: indice=%d "+
		"mandato=%d esLider=%v lider=%d valor=%q", nr.Yo, resp.indice,
		resp.mandato, resp.EsLider, resp.idLider, resp.valorADevolver)
	return resp.indice, resp.mandato, resp.EsLider,
		resp.idLider, resp.valorADevolver
}

// -----------------------------------------------------------------------
// LLAMADAS RPC al API
//
// Si no tenemos argumentos o respuesta estructura vacia (tamaño cero)
type Vacio struct{}

func (nr *NodoRaft) ParaNodo(args Vacio, reply *Vacio) error {
	nr.Logger.Printf("[API] N%d RPC ParaNodo recibida", nr.Yo)
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
	nr.Logger.Printf("[API] N%d RPC SometerOperacionRaft: %+v", nr.Yo,
		operacion)
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
	nr.Logger.Printf("[VOTO] N%d concedo el voto a N%d en el mandato %d",
		nr.Yo, peticion.IdCandidato, nr.MandatoActual)
	if nr.misDatosEstado.miEstado != SEGUIDOR {
		//No enviamos el id de la peticion, porque todavia no es lider
		nr.Logger.Printf("[ESTADO] N%d %s -> SEGUIDOR por voto concedido "+
			"(envio senal)", nr.Yo, nr.misDatosEstado.miEstado)
		nr.misDatosEstado.candidatoHayLider <- true
		nr.misDatosEstado.miEstado = SEGUIDOR
		nr.IdLider = IntNOINICIALIZADO
	}
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
	nr.Logger.Printf("[VOTO] N%d temporizador de latido reiniciado tras "+
		"votar", nr.Yo)
}

func comprobarCondicionesVoto(nr *NodoRaft, peticion *ArgsPeticionVoto) bool {
	if nr.CandidatoVotado != IntNOINICIALIZADO &&
		nr.CandidatoVotado != peticion.IdCandidato {
		nr.Logger.Printf("[VOTO] N%d condicion: ya voto a N%d en este "+
			"mandato", nr.Yo, nr.CandidatoVotado)
		return false
	}
	if peticion.UltimoIndiceMandato < nr.MandatoActual {
		nr.Logger.Printf("[VOTO] N%d condicion: ultimo mandato del log del "+
			"candidato (%d) < mi mandato (%d)", nr.Yo,
			peticion.UltimoIndiceMandato, nr.MandatoActual)
		return false
	}
	nr.Logger.Printf("[VOTO] N%d condicion: log candidato (mandato=%d "+
		"idx=%d) frente a mio (mandato=%d idx=%d)", nr.Yo,
		peticion.UltimoIndiceMandato, peticion.UltimoIndiceRegistro,
		nr.MandatoActual, nr.UltimoIndiceRegistro)
	return peticion.UltimoIndiceMandato > nr.MandatoActual ||
		peticion.UltimoIndiceRegistro >= nr.UltimoIndiceRegistro
}

// Metodo para RPC PedirVoto
func (nr *NodoRaft) PedirVoto(peticion *ArgsPeticionVoto,
	reply *RespuestaPeticionVoto) error {
	nr.Mux.Lock()
	nr.Logger.Printf("[VOTO] N%d recibe PedirVoto de N%d: mandato cand=%d "+
		"mio=%d votado=%d", nr.Yo, peticion.IdCandidato, peticion.Mandato,
		nr.MandatoActual, nr.CandidatoVotado)
	if nr.MandatoActual > peticion.Mandato {
		nr.Logger.Printf("[VOTO] N%d niego el voto a N%d: su mandato es "+
			"obsoleto", nr.Yo, peticion.IdCandidato)
		reply.DecisionVoto = false
	} else if nr.MandatoActual == peticion.Mandato {
		if !comprobarCondicionesVoto(nr, peticion) {
			reply.DecisionVoto = false
			nr.Logger.Printf("[VOTO] N%d niego el voto a N%d: condiciones "+
				"no cumplidas", nr.Yo, peticion.IdCandidato)
		} else {
			nr.otorgarVoto(peticion, reply)
		}
	} else { //nr.MandatoActual < peticion.Mandato
		nr.Logger.Printf("[VOTO] N%d adopto el mandato mayor %d (antes %d)",
			nr.Yo, peticion.Mandato, nr.MandatoActual)
		nr.MandatoActual = peticion.Mandato
		//nr.CandidatoVotado = IntNOINICIALIZADO
		nr.otorgarVoto(peticion, reply)
	}
	reply.MandatoActual = nr.MandatoActual
	nr.Logger.Printf("[VOTO] N%d responde a N%d: voto=%v mi mandato=%d",
		nr.Yo, peticion.IdCandidato, reply.DecisionVoto, reply.MandatoActual)
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
			nr.Logger.Printf("[COMPROMISO] N%d seguidor: comprometido %d "+
				"-> %d (lider=%d, ultima entrada nueva=%d)", nr.Yo,
				nr.UltimoComprometido, nuevo, args.UltIndiceComprom,
				ultimoNuevo)
			nr.UltimoComprometido = nuevo
		}
	}
}

func actualizarInfoLider(nr *NodoRaft, args *ArgAppendEntries) {
	nr.Logger.Printf("[APPEND] N%d reconozco al lider N%d: mandato lider=%d "+
		"mio=%d estado=%s", nr.Yo, args.IdLider, args.MandatoLider,
		nr.MandatoActual, nr.misDatosEstado.miEstado)
	reiniciarTimer(nr.misDatosEstado.tmrOut,
		timerRand(TIMEOUTBASE_LATIDO_MS, RANGO_RAND_LATIDO))
	if nr.misDatosEstado.miEstado != SEGUIDOR {
		nr.Logger.Printf("[ESTADO] N%d %s -> SEGUIDOR por AppendEntries "+
			"de N%d (envio senal)", nr.Yo, nr.misDatosEstado.miEstado,
			args.IdLider)
		nr.misDatosEstado.candidatoHayLider <- true
		nr.misDatosEstado.miEstado = SEGUIDOR
	}
	nr.IdLider = args.IdLider

	if nr.MandatoActual != args.MandatoLider {
		nr.Logger.Printf("[APPEND] N%d cambio de mandato %d -> %d: olvido "+
			"el voto (votado=%d)", nr.Yo, nr.MandatoActual, args.MandatoLider,
			nr.CandidatoVotado)
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
				nr.Logger.Printf("[APPEND] N%d entrada %d ya presente "+
					"(mandato=%d)", nr.Yo, indice, nueva.Mandato)
				continue // ya la tengo, no hay nada que hacer
			}
			nr.Logger.Printf("[APPEND] N%d conflicto en la entrada %d: "+
				"mandato local=%d mandato lider=%d, trunco el registro",
				nr.Yo, indice, nr.RegistroOperaciones[indice].Mandato,
				nueva.Mandato)
			// Conflicto: borro esta entrada y todas las siguientes
			nr.RegistroOperaciones = nr.RegistroOperaciones[:indice]
		}
		nr.Logger.Printf("[APPEND] N%d anado la entrada %d (mandato=%d): %+v",
			nr.Yo, indice, nueva.Mandato, nueva.Operacion)
		nr.RegistroOperaciones = append(nr.RegistroOperaciones, nueva)
	}
	nr.UltimoIndiceRegistro = len(nr.RegistroOperaciones) - 1
	nr.Logger.Printf("[APPEND] N%d registro actualizado: ultimo indice=%d",
		nr.Yo, nr.UltimoIndiceRegistro)
	actualizarIndiceComprometido(nr, args)
}

// Metodo de tratamiento de llamadas RPC AppendEntries
func (nr *NodoRaft) AppendEntries(args *ArgAppendEntries,
	results *Results) error {
	nr.Mux.Lock()
	nr.Logger.Printf("[APPEND] N%d recibe AppendEntries de N%d: mandato=%d "+
		"prevIdx=%d prevMandato=%d entradas=%d commitLider=%d", nr.Yo,
		args.IdLider, args.MandatoLider, args.UltIndxAnt,
		args.UltIndxAntMandato, len(args.NuevaEntradasReg),
		args.UltIndiceComprom)
	if args.MandatoLider < nr.MandatoActual {
		nr.Logger.Printf("[APPEND] N%d rechazo: mandato del lider (%d) "+
			"obsoleto frente al mio (%d)", nr.Yo, args.MandatoLider,
			nr.MandatoActual)
		results.Exito = false
	} else {
		actualizarInfoLider(nr, args)
		if nr.UltimoIndiceRegistro < args.UltIndxAnt {
			nr.Logger.Printf("[APPEND] N%d rechazo: me falta el indice %d "+
				"(mi ultimo=%d)", nr.Yo, args.UltIndxAnt,
				nr.UltimoIndiceRegistro)
			results.Exito = false
		} else if !mismoMandatoEnUltimoIndice(nr, args) {
			nr.Logger.Printf("[APPEND] N%d rechazo: el mandato del indice "+
				"%d no coincide con el del lider (%d)", nr.Yo,
				args.UltIndxAnt, args.UltIndxAntMandato)
			results.Exito = false
		} else {
			results.Exito = true
			agnadirEntradas(nr, args)
		}
	}
	results.MandatoActual = nr.MandatoActual
	nr.Logger.Printf("[APPEND] N%d responde a N%d: exito=%v mi mandato=%d",
		nr.Yo, args.IdLider, results.Exito, results.MandatoActual)
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

	nr.Logger.Printf("[ELECCION] N%d -> N%d envia PedirVoto (mandato=%d)",
		nr.Yo, nodo, args.Mandato)
	err := nr.Nodos[nodo].CallTimeout("NodoRaft.PedirVoto",
		args, reply, TIMEOUT_RTT_RPC*time.Millisecond)
	nr.Logger.Printf("[ELECCION] N%d <- N%d resultado RPC PedirVoto: "+
		"err=%v", nr.Yo, nodo, err)

	return err == nil
}

func (nr *NodoRaft) enviarEntradasReg(nodo int, args *ArgAppendEntries,
	results *Results) bool {

	err := nr.Nodos[nodo].CallTimeout("NodoRaft.AppendEntries",
		args, results, TIMEOUT_RTT_RPC*time.Millisecond)
	nr.Logger.Printf("[REPL] N%d -> N%d AppendEntries (mandato=%d "+
		"prevIdx=%d entradas=%d commit=%d): err=%v", nr.Yo, nodo,
		args.MandatoLider, args.UltIndxAnt, len(args.NuevaEntradasReg),
		args.UltIndiceComprom, err)

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
	nr.Logger.Printf("[ELECCION] N%d inicia eleccion: mandato=%d, me voto "+
		"a mi mismo, ultimoIdx=%d ultimoMandato=%d nodos=%d", nr.Yo,
		nr.MandatoActual, ulInd, args.UltimoIndiceMandato, totalN)
	nr.Mux.Unlock()
	return
}

func tratarReplyVoto(nr *NodoRaft, reply *RespuestaPeticionVoto) int {
	nr.Mux.Lock()
	nr.Logger.Printf("[ELECCION] N%d respuesta de voto: voto=%v mandato "+
		"respuesta=%d (mio=%d)", nr.Yo, reply.DecisionVoto,
		reply.MandatoActual, nr.MandatoActual)
	if nr.MandatoActual < reply.MandatoActual {
		nr.Logger.Printf("[ELECCION] N%d detecta mandato mayor %d (mio=%d): "+
			"olvido el voto", nr.Yo, reply.MandatoActual, nr.MandatoActual)
		nr.MandatoActual = reply.MandatoActual
		nr.CandidatoVotado = IntNOINICIALIZADO
		if nr.misDatosEstado.miEstado != SEGUIDOR {
			//Si me responden que no puedo ser lider por el mandato, dejo de intentarlo
			nr.Logger.Printf("[ESTADO] N%d %s -> SEGUIDOR por mandato mayor "+
				"(envio senal)", nr.Yo, nr.misDatosEstado.miEstado)
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
		nr.Logger.Printf("[ELECCION] N%d sin respuesta de N%d en el mandato "+
			"%d (fallo RPC o timeout)", nr.Yo, nodoAEnviar, args.Mandato)
		chVotoRecibido <- 0
	} //Aqui fallo la llamada RPC, no podriamos revisar el reply
}

func comprobarResultadoEleccion(nr *NodoRaft, args *ArgsPeticionVoto) {
	nr.Mux.Lock()
	nr.Logger.Printf("[ELECCION] N%d mayoria en el mandato %d: compruebo "+
		"(mandato actual=%d estado=%s)", nr.Yo, args.Mandato,
		nr.MandatoActual, nr.misDatosEstado.miEstado)
	if nr.MandatoActual == args.Mandato &&
		nr.misDatosEstado.miEstado == CANDIDATO {
		nr.Logger.Printf("[ELECCION] N%d victoria valida en el mandato %d: "+
			"envio senal", nr.Yo, args.Mandato)
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
	nr.Logger.Printf("[ELECCION] N%d mandato %d: peticiones enviadas a %d "+
		"nodos", nr.Yo, args.Mandato, totalNodos-1)
	var yaComprobado bool = false
	for i := 0; i < totalNodos-1; i++ {
		totalVotos += <-chVotoRecbdo
		nr.Logger.Printf("[ELECCION] N%d mandato %d: recuento %d votos de "+
			"%d nodos (mayoria=%d)", nr.Yo, args.Mandato, totalVotos,
			totalNodos, totalNodos/2+1)
		if !yaComprobado && (totalVotos >= (totalNodos/2 + 1)) {
			yaComprobado = true
			comprobarResultadoEleccion(nr, args)
		}
	}
	nr.Logger.Printf("[ELECCION] N%d mandato %d: fin del recuento con %d "+
		"votos", nr.Yo, args.Mandato, totalVotos)
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
	nr.Logger.Printf("[APPEND] N%d fallo de AppendEntries en N%d: mandato "+
		"respuesta=%d mio=%d", nr.Yo, nodo, resultado.MandatoActual,
		nr.MandatoActual)
	if resultado.MandatoActual > nr.MandatoActual {
		nr.Logger.Printf("[ESTADO] N%d LIDER -> SEGUIDOR: N%d tiene mandato "+
			"mayor (%d)", nr.Yo, nodo, resultado.MandatoActual)
		nr.MandatoActual = resultado.MandatoActual
		nr.CandidatoVotado = IntNOINICIALIZADO
		//Un seguidor tiene mayor mandato que yo, no se si es el lider
		nr.misDatosEstado.candidatoHayLider <- true
		nr.misDatosEstado.miEstado = SEGUIDOR
		nr.IdLider = IntNOINICIALIZADO
	} else {
		nr.Logger.Printf("[APPEND] N%d inconsistencia con N%d: retrocedo "+
			"nextIdx %d -> %d", nr.Yo, nodo,
			nr.infoLid.SiguienteIdceEnviar_seguidor[nodo],
			nr.infoLid.SiguienteIdceEnviar_seguidor[nodo]-1)
		nr.infoLid.SiguienteIdceEnviar_seguidor[nodo] -= 1
	}
}

func comprobarResultadoAppendEntries_exito(nr *NodoRaft, nodo int,
	argumentos *ArgAppendEntries) {
	indEntAñad := argumentos.UltIndxAnt + len(argumentos.NuevaEntradasReg)
	if indEntAñad > nr.infoLid.IdceReplicado_seguidor[nodo] {
		nr.infoLid.SiguienteIdceEnviar_seguidor[nodo] = indEntAñad + 1
		nr.infoLid.IdceReplicado_seguidor[nodo] = indEntAñad
		nr.Logger.Printf("[REPL] N%d exito en N%d: matchIdx=%d nextIdx=%d",
			nr.Yo, nodo, nr.infoLid.IdceReplicado_seguidor[nodo],
			nr.infoLid.SiguienteIdceEnviar_seguidor[nodo])
	}
}

func comprobarResultadoAppendEntries(nr *NodoRaft, nodo int,
	argumentos *ArgAppendEntries, resultado *Results) {
	nr.Mux.Lock()
	nr.Logger.Printf("[REPL] N%d respuesta de N%d: exito=%v mandato "+
		"respuesta=%d (estado=%s mandato=%d mandato del envio=%d)", nr.Yo,
		nodo, resultado.Exito, resultado.MandatoActual,
		nr.misDatosEstado.miEstado, nr.MandatoActual, argumentos.MandatoLider)
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
