package conf

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type ServerConfiguration struct {
	Host string // ip:port
	ZooKeeperClientConfiguration
}

type ProxyConfiguration struct {
	SocketPort int
	HttpPort   int
	Addr       string
	ZooKeeperClientConfiguration
	ConsistencyConfiguration
}

type ServersConfiguration struct {
	ServersHost     []string
	PoolMaxCapacity int
	PoolCapacity    int
	IdleTimeout     time.Duration
	ConnectTimeout  time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

type ServerSendQueueConfiguration struct {
	queueLimits int
	batchNum    int
}

type ClientConfiguration struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

type ZooKeeperClientConfiguration struct {
	ZKAddress string
	RingPath  string
}

type ZooKeeperServerConfiguration struct {
	ClientPort int
	RunDir     string
	ZkPath     string
}

type BufferPoolConfiguration struct {
	IdleTimeout time.Duration
	ChunkSize   int
	Capacity    int
	MaxCapacity int
}

const (
	MasterSlave       = 0
	EventConsistency  = 1
	StrongConsistency = 2
)

type RingConfiguration struct {
	Replicas   int
	Partitions int
}

type ConsistencyConfiguration struct {
	Mode int
	// for consistency configuration:
	// if data wants to maintain eventual consistency, it need to make sure Replicas > WriteCount + ReadCount
	WriteLowWater int
	ReadLowWater  int
}

type BalancersConfiguration struct {
	BalancersName          []string
	balancersConfiguration map[string]*BalancerConfiguration
}

func (self BalancersConfiguration) GetBalancerConfiguration(name string) *BalancerConfiguration {
	if configuration, ok := self.balancersConfiguration[name]; ok {
		return configuration
	} else {
		panic(fmt.Sprintf("init balancer configuration error! %s not existed", name))
	}
}

type BalancerConfiguration struct {
	BalancerType string
	Identifiers  []string
	*RoutinePoolConfiguration
	AggreagatorName string //aggregator type name
	*WorkAggregatorConfiguration
	QueueConfiguration
}

type RoutinePoolConfiguration struct {
	InitRoutineNum     int // default routine num in the pool
	MinRoutineNum      int
	RoutineIdleTime    time.Duration
	MoniterInterval    time.Duration
	SmoothConst        float64
	RequestsThreshould int  // if requests in the waitting list exceed this, it will add 1 routine to the routine pool
	ManagerAutoDetect  bool // balance the routine num according to the throughput
}

type WorkAggregatorConfiguration struct {
	AggregateLimits int
	MinAggreagation int
	MaxAggreagation int
	BlockTime       time.Duration
	RecalcWindow    int64 // time interval between each calculation
	SmoothConst     float64
}

type QueueConfiguration struct {
	QueueBuffer int // if requests in the waitting list exceed this, request will block
	QueueLimits int // if requests in the waitting list exceeds this, it will reject the request
}

type EngineConfiguration struct {
	EngineName string
	LeveldbEngineConfiguration
}

type LeveldbEngineConfiguration struct {
	FileName        string
	FilePath        string
	LRUCacheSize    int
	CreateIfMissing bool
}

type LogConfiguration struct {
	Logs []string
	*FileLogConfiguration
	*ConsoleLogConfiguration
	*RemoteConnLogConfiguration
	*SmtpLogConfiguration
}

type FileLogConfiguration struct {
	Level    int
	Filepath string
	// Rotate or not
	Rotate bool
	// Rotate at lines
	Maxlines int
	// Rotate at size
	Maxsize int
	// Rotate daily
	RotateDaily bool
	// longgest preserved time
	Maxdays int64
}

type ConsoleLogConfiguration struct {
	Level int
}

type RemoteConnLogConfiguration struct {
	Level      int
	Host       string
	Port       int
	Protocol   string // udp or tcp
	MaxRetries int    // max retry time per message
}

type SmtpLogConfiguration struct {
	Level              int
	Username           string
	Passwd             string
	Host               string
	Port               int
	Subject            string // mail subject
	RecipientAddresses []string
}

const (
	// ---------------SECTION NAME-----------------
	defaultConfigureName = "/home/ted/goWorkspace/src/github.com/senarukana/dldb/confFile/dldb.confFile"
	coreSectionName      = "dbCore"
	serverSectionName    = "server"
	clientSectionName    = "client"

	balancerSectionName      = "balancer"
	engineSectionName        = "engine"
	leveldbEngineSectionName = "leveldb"
	logSectionName           = "log"
	fileLogSectionName       = "log-file"
	consoleLogSectionName    = "log-console"
	remoteConnLogSectionName = "log-conn"
	smtpLogSectioNname       = "log-smtp"

	//------------dbCore configuration--------------
	//Proxy configuration
	defaultProxyAddress    = "localhost"
	defaultProxySocketPort = 9093
	defaultProxyHttpPort   = 8093

	//server configuration
	defaultServerAddress = "localhost"
	defaultServerPort    = 9093
	// client configuration
	defaultClientReadTimeout  = time.Duration(200) * time.Minute // 200 ms
	defaultClientWriteTimeout = time.Duration(500) * time.Minute // 500 ms

	//buffer pool configuration
	defaultBufferChunkSize   = 4096
	defaultBufferCapacity    = 4096     // 16MB
	defaultBufferMaxCapacity = 4096 * 4 // 64MB

	//balancer configuration
	defaultBalancers    = "engine send"
	defaultBalancerType = "priority"
	// routine configuration
	defaultInitRoutineNum         = 5
	defaultMinRoutineNum          = 3
	defaultRoutineIdleTime        = time.Duration(50) * time.Millisecond //50 ms
	defaultMoniterInterval        = time.Duration(10) * time.Millisecond
	defaultRequestThreshould      = 20
	defaultRoutinePoolSmoothConst = 0.7
	defaultManagerAutoDetect      = true

	//Aggreagator configuration
	defaultAggregatorName           = "none"
	defaultSimpleMinAggregation     = 2
	defaultSimpleMaxAggregation     = 1000
	defaultSimpleAggregateBlockTime = time.Duration(5) * time.Second // 5ms
	defaultSimpleRecalcWindow       = 1000                           // time interval(ms) between each calculation
	defaultSimpleSmoothConst        = 0.7

	// Queue Configuration
	defaultQueueBuffer = 200
	defaultQueueLimits = 1000 // if the msg exceeds this, it will reject the request

	//------------dbCore end------------------------

	//----------log configuration----------------------
	// file log configuration
	defaultFileLogLevel       = 2 //Info
	defaultFileLogRotate      = true
	defaultFileLogRotateDaily = true

	// console log configuration
	defaultConsoleLogLevel = 0 //Trace
	// smtp log configuration
	defaultSmtpLogLevel = 4 //Error
	// remote log configuration
	defaultRemoteConnLogLevel = 3 // Warn
	//----------log configuration----------------------

	//----------engine configuration----------------------
	defaultEngine = "leveldb"
	// leveldb engine configuration
	defaultLeveldbFileName      = "level.db"
	defaultLevelLRUCacheSize    = 2 >> 28 //256 MB
	defaultLevelCreateIfMissing = true
	//---------engine configuration-----------------------
)

func initProxyConfiguration(configuration *ProxyConfiguration, confFile *ConfigFile) {
	if serverAddr, err := confFile.GetString(serverSectionName, "address"); err == nil {
		configuration.Addr = serverAddr
	} else {
		configuration.Addr = defaultServerAddress
	}
	if socketPort, err := confFile.GetInt(serverSectionName, "socketPort"); err == nil {
		configuration.SocketPort = socketPort
	} else {
		configuration.SocketPort = defaultProxySocketPort
	}
	if httpPort, err := confFile.GetInt(serverSectionName, "httpPort"); err == nil {
		configuration.HttpPort = httpPort
	} else {
		configuration.HttpPort = defaultProxyHttpPort
	}
}

func initServersConfiguration(configuration *ServersConfiguration, confFile *ConfigFile) {
}

func initServerConfiguration(configuration *ServerConfiguration, confFile *ConfigFile) {
	var (
		address string
		port    int
	)
	if serverAddr, err := confFile.GetString(serverSectionName, "address"); err == nil {
		address = serverAddr
	} else {
		address = defaultServerAddress
	}
	if socketPort, err := confFile.GetInt(serverSectionName, "port"); err == nil {
		port = socketPort
	} else {
		port = defaultServerPort
	}
	configuration.Host = fmt.Sprintf("%s:%d", address, port)
}

func initClientConfiguration(configuration *ClientConfiguration, confFile *ConfigFile) {
	if readTimeout, err := confFile.GetInt(clientSectionName, "readTimeout"); err == nil {
		configuration.ReadTimeout = time.Duration(readTimeout) * time.Millisecond
	} else {
		configuration.ReadTimeout = defaultClientReadTimeout
	}
	if writeTimeout, err := confFile.GetInt(clientSectionName, "writeTimeout"); err == nil {
		configuration.WriteTimeout = time.Duration(writeTimeout) * time.Millisecond
	} else {
		configuration.WriteTimeout = defaultClientWriteTimeout
	}
}

func initBufferPoolConfiguration(configuration *BufferPoolConfiguration, confFile *ConfigFile) {
	configuration.Capacity = defaultBufferCapacity
	configuration.MaxCapacity = defaultBufferMaxCapacity
	configuration.ChunkSize = defaultBufferChunkSize
	configuration.IdleTimeout = time.Minute
}

func initBalancersConfiguration(configuration *BalancersConfiguration, confFile *ConfigFile) {
	balancers, err := confFile.GetString(balancerSectionName, "balancers")
	if err != nil {
		balancers = defaultBalancers
	}
	configuration.balancersConfiguration = make(map[string]*BalancerConfiguration)
	list := strings.Split(balancers, " ")
	configuration.BalancersName = list
	for _, balancerName := range list {
		configuration.balancersConfiguration[balancerName] = new(BalancerConfiguration)
		initBalancerConfiguration(configuration.GetBalancerConfiguration(balancerName), balancerName, confFile)
	}
	// initSimpleAggreagatorConfiguration(confFile)
}

func initBalancerConfiguration(configuration *BalancerConfiguration, name string, confFile *ConfigFile) {
	sectionName := balancerSectionName + "-" + name // balancer-send balancer-receive
	if balancerType, err := confFile.GetString(sectionName, "balancerType"); err == nil {
		configuration.BalancerType = balancerType
	} else {
		configuration.BalancerType = defaultBalancerType
	}
	if configuration.BalancerType != "identifier" {
		configuration.RoutinePoolConfiguration = new(RoutinePoolConfiguration)
		initRoutinePoolConfiguration(configuration.RoutinePoolConfiguration, name, confFile)
	}
	initAggregatorConfiguration(configuration, name, confFile)
	initQueuerConfiguration(&configuration.QueueConfiguration, name, configuration.BalancerType, confFile)
}

func initRoutinePoolConfiguration(routineConfiguration *RoutinePoolConfiguration, balancerName string, confFile *ConfigFile) {
	/*
		InitRoutineNum     int // default routine num in the pool
		MinRoutineNum      int
		RoutineIdleTime    time.Duration
		MoniterInterval    time.Duration
		SmoothConst        float64
		RequestsBuffer     int  // if requests in the waitting list exceed this, request will block
		RequestsLimit      int  // if requests in the waitting list exceeds this, it will reject the request
		RequestsThreshould int  // if requests in the waitting list exceed this, it will add 1 routine to the routine pool
		ManagerAutoDetect  bool // balance the routine num according to the throughput
	*/

	sectionName := balancerSectionName + "-" + balancerName // balancer-send balancer-receive
	if initRoutineNum, err := confFile.GetInt(sectionName, "initRoutineNum"); err == nil {
		routineConfiguration.InitRoutineNum = initRoutineNum
	} else {
		routineConfiguration.InitRoutineNum = defaultInitRoutineNum
	}
	if minRoutineNum, err := confFile.GetInt(sectionName, "minRoutineNum"); err == nil {
		routineConfiguration.MinRoutineNum = minRoutineNum
	} else {
		routineConfiguration.MinRoutineNum = defaultMinRoutineNum
	}
	if routineIdleTime, err := confFile.GetInt(sectionName, "routineidletime"); err == nil {
		routineConfiguration.RoutineIdleTime = time.Duration(routineIdleTime) * time.Millisecond
	} else {
		routineConfiguration.RoutineIdleTime = defaultRoutineIdleTime
	}
	if moniterInterval, err := confFile.GetInt(sectionName, "moniterinterval"); err == nil {
		routineConfiguration.MoniterInterval = time.Duration(moniterInterval) * time.Millisecond
	} else {
		routineConfiguration.MoniterInterval = defaultMoniterInterval
	}

	if smoothConst, err := confFile.GetFloat64(sectionName, "smoothconst"); err == nil {
		routineConfiguration.SmoothConst = smoothConst
	} else {
		routineConfiguration.SmoothConst = defaultRoutinePoolSmoothConst
	}
	if requestsThreshould, err := confFile.GetInt(sectionName, "requeststhreshould"); err == nil {
		routineConfiguration.RequestsThreshould = requestsThreshould
	} else {
		routineConfiguration.RequestsThreshould = defaultRequestThreshould
	}
	if managerAutodetect, err := confFile.GetBool(sectionName, "managerautodetect"); err == nil {
		routineConfiguration.ManagerAutoDetect = managerAutodetect
	} else {
		routineConfiguration.ManagerAutoDetect = defaultManagerAutoDetect
	}
}

func initAggregatorConfiguration(balancerConfiguration *BalancerConfiguration, balancerName string, confFile *ConfigFile) {
	sectionName := balancerSectionName + "-" + balancerName //i.e: balancer-send
	if aggregatorName, err := confFile.GetString(sectionName, "aggregatorName"); err == nil {
		balancerConfiguration.AggreagatorName = aggregatorName
	} else {
		balancerConfiguration.AggreagatorName = defaultAggregatorName // default aggregator is none
	}
	switch balancerConfiguration.AggreagatorName {
	case "simple":
		balancerConfiguration.WorkAggregatorConfiguration = new(WorkAggregatorConfiguration)
		initSimpleAggreagatorConfiguration(sectionName, confFile, balancerConfiguration.WorkAggregatorConfiguration)
	}
}

func initSimpleAggreagatorConfiguration(sectionName string, confFile *ConfigFile, aggregatorConfiguration *WorkAggregatorConfiguration) {
	if minAggregation, err := confFile.GetInt(sectionName, "minaggregation"); err == nil {
		aggregatorConfiguration.MinAggreagation = minAggregation
	} else {
		aggregatorConfiguration.MinAggreagation = defaultSimpleMinAggregation
	}
	if maxAggregation, err := confFile.GetInt(sectionName, "maxaggregation"); err == nil {
		aggregatorConfiguration.MaxAggreagation = maxAggregation
	} else {
		aggregatorConfiguration.MaxAggreagation = defaultSimpleMaxAggregation
	}
	if aggregateBlockTime, err := confFile.GetInt(sectionName, "blocktime"); err == nil {
		aggregatorConfiguration.BlockTime = time.Duration(aggregateBlockTime) * time.Millisecond
	} else {
		aggregatorConfiguration.BlockTime = defaultSimpleAggregateBlockTime
	}

	if recalcwindow, err := confFile.GetInt(sectionName, "recalcwindow"); err == nil {
		aggregatorConfiguration.RecalcWindow = int64(recalcwindow)
	} else {
		aggregatorConfiguration.RecalcWindow = int64(defaultSimpleRecalcWindow)
	}

	if smoothConst, err := confFile.GetFloat64(sectionName, "smoothconst"); err == nil {
		aggregatorConfiguration.SmoothConst = smoothConst
	} else {
		aggregatorConfiguration.SmoothConst = defaultSimpleSmoothConst
	}
}

func initQueuerConfiguration(configuration *QueueConfiguration, balancerName string, balancerType string, confFile *ConfigFile) {
	sectionName := balancerSectionName + "-" + balancerName //i.e: balancer-send
	if queueBuffer, err := confFile.GetInt(sectionName, "queuebuffer"); err == nil {
		configuration.QueueBuffer = queueBuffer
	} else {
		configuration.QueueBuffer = defaultQueueBuffer
	}
	if queueLimits, err := confFile.GetInt(sectionName, "queuelimits"); err == nil {
		configuration.QueueLimits = queueLimits
	} else {
		configuration.QueueLimits = defaultQueueLimits
	}

}

func initEngineConfiguration(configuration *EngineConfiguration, confFile *ConfigFile) {
	engine, err := confFile.GetString(engineSectionName, "engine")
	if err != nil {
		engine = defaultEngine
	}
	configuration.EngineName = engine
	switch engine {
	case "leveldb":
		initLeveldbEngineConfiguration(configuration, confFile)
	default:
		panic("unknown engine name")
	}
}

func initLeveldbEngineConfiguration(configuration *EngineConfiguration, confFile *ConfigFile) {
	// leveldb configuration
	if filePath, err := confFile.GetString(leveldbEngineSectionName, "filepath"); err == nil {
		configuration.LeveldbEngineConfiguration.FilePath = filePath
	} else {
		if filepath, err := os.Getwd(); err != nil {
			panic(fmt.Sprintf("can't get current file path ", filepath))
		} else {
			configuration.LeveldbEngineConfiguration.FilePath = filePath
		}
	}
	if fileName, err := confFile.GetString(leveldbEngineSectionName, "file"); err == nil {
		configuration.LeveldbEngineConfiguration.FileName = fileName
	} else {
		configuration.LeveldbEngineConfiguration.FileName = defaultLeveldbFileName
	}
	if lruCache, err := confFile.GetInt(leveldbEngineSectionName, "lrucache"); err == nil {
		configuration.LeveldbEngineConfiguration.LRUCacheSize = lruCache
	} else {
		configuration.LeveldbEngineConfiguration.LRUCacheSize = defaultLevelLRUCacheSize
	}
	if createIfMissing, err := confFile.GetBool(leveldbEngineSectionName, "createifmissing"); err == nil {
		configuration.LeveldbEngineConfiguration.CreateIfMissing = createIfMissing
	} else {
		configuration.LeveldbEngineConfiguration.CreateIfMissing = defaultLevelCreateIfMissing
	}
}

func initLogConfiguration(configuration *LogConfiguration, confFile *ConfigFile) {
	logs, err := confFile.GetString(logSectionName, "logs")
	if err != nil {
		logs = "file console"
	}

	list := strings.Split(logs, " ")
	for _, logName := range list {
		switch logName {
		case "file":
			configuration.FileLogConfiguration = new(FileLogConfiguration)
			initFileLogConfiguration(configuration.FileLogConfiguration, confFile)
		case "console":
			configuration.ConsoleLogConfiguration = new(ConsoleLogConfiguration)
			initConsoleLogConfiguration(configuration.ConsoleLogConfiguration, confFile)
		case "smtp":
			configuration.SmtpLogConfiguration = new(SmtpLogConfiguration)
			initSmtpLogConfiguration(configuration.SmtpLogConfiguration, confFile)
		case "conn":
			configuration.RemoteConnLogConfiguration = new(RemoteConnLogConfiguration)

		}
	}

}

func initFileLogConfiguration(configuration *FileLogConfiguration, confFile *ConfigFile) {
	if filepath, err := confFile.GetString(fileLogSectionName, "filepath"); err == nil {
		configuration.Filepath = filepath
	} else {
		if filepath, err := os.Getwd(); err != nil {
			panic(fmt.Sprintf("can't get current file path, %v", err))
		} else {
			configuration.Filepath = filepath
		}
	}

	if level, err := confFile.GetInt(fileLogSectionName, "level"); err == nil {
		configuration.Level = level
	} else {
		configuration.Level = defaultFileLogLevel
	}

	if rotate, err := confFile.GetBool(fileLogSectionName, "rotate"); err == nil {
		configuration.Rotate = rotate
	} else {
		configuration.Rotate = defaultFileLogRotate
	}

	if rotateDaily, err := confFile.GetBool(fileLogSectionName, "rotatedaily"); err == nil {
		configuration.RotateDaily = rotateDaily
	} else {
		configuration.RotateDaily = defaultFileLogRotateDaily
	}
}

func initConsoleLogConfiguration(configuration *ConsoleLogConfiguration, confFile *ConfigFile) {
	// console configuration
	if level, err := confFile.GetInt(consoleLogSectionName, "level"); err == nil {
		configuration.Level = level
	} else {
		configuration.Level = defaultConsoleLogLevel
	}
}

func initSmtpLogConfiguration(configuration *SmtpLogConfiguration, confFile *ConfigFile) {
	// console configuration
	if level, err := confFile.GetInt(smtpLogSectioNname, "level"); err == nil {
		configuration.Level = level
	} else {
		configuration.Level = defaultSmtpLogLevel
	}
}

func initRemoteConnLogConfiguration(configuration *RemoteConnLogConfiguration, confFile *ConfigFile) {
	// console configuration
	if level, err := confFile.GetInt(smtpLogSectioNname, "level"); err == nil {
		configuration.Level = level
	} else {
		configuration.Level = defaultRemoteConnLogLevel
	}
}
