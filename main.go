package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/mdaxf/iac/com"
	"github.com/mdaxf/iac/config"
	dbconn "github.com/mdaxf/iac/databases"
	"github.com/mdaxf/iac/documents"
	"github.com/mdaxf/iac/engine/trancode"
	"github.com/mdaxf/iac/framework/callback_mgr"
	"github.com/mdaxf/iac/health"
	"github.com/mdaxf/iac/integration/mqttclient"
	iacmb "github.com/mdaxf/iac/integration/signalr"
	"github.com/mdaxf/iac/logger"
	"github.com/mdaxf/signalrsrv/signalr"
)

var (
	nodedata    map[string]interface{}
	Mqttclients []*mqttclient.MqttClient
)

func main() {
	startTime := time.Now()

	var wg sync.WaitGroup

	gconfig, err := config.LoadGlobalConfig()

	if err != nil {
		log.Fatalf("Failed to load global configuration: %v", err)
		//ilog.Error(fmt.Sprintf("Failed to load global configuration: %v", err))
	}
	initializeloger(gconfig)

	nodedata = make(map[string]interface{})
	nodedata["Name"] = "iac-mqtt"
	nodedata["AppID"] = uuid.New().String()
	nodedata["Description"] = "IAC MQTT Client Service"
	nodedata["Type"] = "MQTTClient"
	nodedata["Version"] = "1.0.0"

	ilog := logger.Log{ModuleName: logger.Framework, User: "System", ControllerName: "iac-mqtt"}
	ilog.Debug(fmt.Sprintf("Start the iac-mqtt: %v", nodedata))

	DB := initializeDatabase(ilog, gconfig)
	if DB == nil {
		ilog.Error("Database connection error")
		return
	}

	docDB := initializedDocuments(ilog, gconfig)
	if docDB == nil {
		ilog.Error("MongoDB connection error!")
		return
	}

	IACMessageBusClient, err := iacmb.Connect(gconfig.SingalRConfig)
	if err != nil {
		ilog.Error(fmt.Sprintf("Failed to connect to IAC Message Bus: %v", err))
	} else {
		ilog.Debug(fmt.Sprintf("IAC Message Bus: %v", IACMessageBusClient))
	}

	if callback_mgr.CallBackMap["TranCode_Execute"] == nil {
		ilog.Debug("Register the trancode execution interface")
		tfr := trancode.TranFlowstr{}
		callback_mgr.RegisterCallBack("TranCode_Execute", tfr.Execute)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		initializeMqttClient(gconfig, ilog, DB, docDB, IACMessageBusClient)
	}()

	// Start the HeartBeat
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			HeartBeat(ilog, gconfig, DB, docDB, IACMessageBusClient)
			time.Sleep(5 * time.Minute)
		}
	}()

	elapsed := time.Since(startTime)
	ilog.PerformanceWithDuration("iac-mqtt.main", elapsed)

	wg.Wait()

	waitForTerminationSignal(ilog, gconfig)
}

func HeartBeat(ilog logger.Log, gconfig *config.GlobalConfig, DB *sql.DB, DocDB *documents.DocDB, IACMessageBusClient signalr.Client) {
	ilog.Debug("Start HeartBeat for iac-activemq application with appid: " + nodedata["AppID"].(string))
	appHeartBeatUrl := com.ConverttoString(gconfig.AppServer["url"]) + "/IACComponents/heartbeat"
	ilog.Debug("HeartBeat URL: " + appHeartBeatUrl)

	result, err := health.CheckNodeHealth(nodedata, DB, DocDB.MongoDBClient, IACMessageBusClient)

	ilog.Debug(fmt.Sprintf("HeartBeat Result: %v", result))

	activemqsresult, err := CheckServiceStatus(ilog)
	if err != nil {
		ilog.Error(fmt.Sprintf("HeartBeat error: %v", err))
	}

	data := make(map[string]interface{})
	data["Node"] = nodedata
	data["Result"] = result
	data["ServiceStatus"] = activemqsresult
	data["time"] = time.Now().UTC()
	// send the heartbeat to the server
	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = "apikey " + com.ConverttoString(gconfig.AppServer["apikey"])

	response, err := com.CallWebService(appHeartBeatUrl, "POST", data, headers)

	if err != nil {
		ilog.Error(fmt.Sprintf("HeartBeat error: %v", err))
		return
	}

	ilog.Debug(fmt.Sprintf("HeartBeat post response: %v", response))
}

func CheckServiceStatus(iLog logger.Log) (map[string]interface{}, error) {
	iLog.Debug("Check ActiveMQ Status")
	result := make(map[string]interface{})

	for _, client := range Mqttclients {
		if client.Client.IsConnected() {
			result[client.Config.Broker+":"+client.Config.Port] = true
		} else {
			result[client.Config.Broker+":"+client.Config.Port] = false
		}
	}
	return result, nil
}

func initializeloger(gconfig *config.GlobalConfig) error {
	if gconfig.LogConfig == nil {
		return fmt.Errorf("log configuration is missing")
	}
	fmt.Printf("log configuration: %v", gconfig.LogConfig)
	logger.Init(gconfig.LogConfig)
	return nil
}

func initializeMqttClient(gconfig *config.GlobalConfig, ilog logger.Log, DB *sql.DB, DocDB *documents.DocDB, IACMessageBusClient signalr.Client) {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		ilog.PerformanceWithDuration("main.initializeMqttClient", elapsed)
	}()

	config.MQTTClients = make(map[string]*mqttclient.MqttClient)

	//	go func() {

	ilog.Debug("initialize MQTT Client")

	data, err := ioutil.ReadFile("mqttconfig.json")
	if err != nil {
		ilog.Debug(fmt.Sprintf("failed to read configuration file: %v", err))

	}
	ilog.Debug(fmt.Sprintf("MQTT Clients configuration file: %s", string(data)))
	var mqttconfig mqttclient.MqttConfig
	err = json.Unmarshal(data, &mqttconfig)
	if err != nil {
		ilog.Debug(fmt.Sprintf("failed to unmarshal the configuration file: %v", err))

	}

	ilog.Debug(fmt.Sprintf("MQTT Clients configuration: %v", logger.ConvertJson(mqttconfig)))
	i := 1
	for _, mqttcfg := range mqttconfig.Mqtts {
		ilog.Debug(fmt.Sprintf("MQTT Client configuration: %s", logger.ConvertJson(mqttcfg)))
		mqtc := mqttclient.NewMqttClient(mqttcfg)

		mqtc.DB = DB
		mqtc.DocDBconn = DocDB
		mqtc.SignalRClient = IACMessageBusClient
		mqtc.AppServer = com.ConverttoString(gconfig.AppServer["url"])
		mqtc.ApiKey = mqttconfig.ApiKey
		//config.MQTTClients[fmt.Sprintf("mqttclient_%d", i)] = mqtc
		mqtc.Initialize_mqttClient()
		//	fmt.Sprintln("MQTT Client: %v", config.MQTTClients)
		//	ilog.Debug(fmt.Sprintf("MQTT Client: %v", config.MQTTClients))
		i++
	}

	fmt.Println("MQTT Clients: %v, %d", config.MQTTClients, i)
	ilog.Debug(fmt.Sprintf("MQTT Clients: %v, %d", config.MQTTClients, i))
	//}()

}

func waitForTerminationSignal(ilog logger.Log, gconfig *config.GlobalConfig) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("\nShutting down...")
	ilog.Debug("Start HeartBeat for iac-activemq application with appid: " + nodedata["AppID"].(string))
	appHeartBeatUrl := com.ConverttoString(gconfig.AppServer["url"]) + "/IACComponents/close"
	ilog.Debug("HeartBeat URL: " + appHeartBeatUrl)

	data := make(map[string]interface{})
	data["Node"] = nodedata
	data["time"] = time.Now().UTC()

	headers := make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Authorization"] = "apikey " + com.ConverttoString(gconfig.AppServer["apikey"])

	_, err := com.CallWebService(appHeartBeatUrl, "POST", data, headers)

	if err != nil {
		ilog.Error(fmt.Sprintf("HeartBeat error: %v", err))
	}

	time.Sleep(2 * time.Second) // Add any cleanup or graceful shutdown logic here
	os.Exit(0)
}

func initializeDatabase(ilog logger.Log, gconfig *config.GlobalConfig) *sql.DB {
	// function execution start time
	startTime := time.Now()

	// defer function to log the performance duration of initializeDatabase
	defer func() {
		elapsed := time.Since(startTime)
		ilog.PerformanceWithDuration("main.initializeDatabase", elapsed)
	}()

	ilog.Debug("initialize Database")
	databaseconfig := gconfig.DatabaseConfig

	// check if database type is missing
	if databaseconfig["type"] == nil {
		ilog.Error(fmt.Sprintf("initialize Database error: %s", "DatabaseType is missing"))
		return nil
	}

	// check if database connection is missing
	if databaseconfig["connection"] == nil {
		ilog.Error(fmt.Sprintf("initialize Database error: %s", "DatabaseConnection is missing"))
		return nil
	}

	// set the database type and connection string
	dbconn.DatabaseType = databaseconfig["type"].(string)
	dbconn.DatabaseConnection = databaseconfig["connection"].(string)

	// set the maximum idle connections, default to 5 if not provided or if the value is not a float64
	if databaseconfig["maxidleconns"] == nil {
		dbconn.MaxIdleConns = 5
	} else {
		if v, ok := databaseconfig["maxidleconns"].(float64); ok {
			dbconn.MaxIdleConns = int(v)
		} else {
			dbconn.MaxIdleConns = 5
		}
	}

	// set the maximum open connections, default to 10 if not provided or if the value is not a float64
	if databaseconfig["maxopenconns"] == nil {
		dbconn.MaxOpenConns = 10
	} else {
		if v, ok := databaseconfig["maxopenconns"].(float64); ok {
			dbconn.MaxOpenConns = int(v)
		} else {
			dbconn.MaxOpenConns = 10
		}
	}

	// connect to the database
	err := dbconn.ConnectDB()
	if err != nil {
		ilog.Error(fmt.Sprintf("initialize Database error: %s", err.Error()))
		return nil
	}

	return dbconn.DB
}

func initializedDocuments(ilog logger.Log, gconfig *config.GlobalConfig) *documents.DocDB {
	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime)
		ilog.PerformanceWithDuration("main.initializedDocuments", elapsed)
	}()

	if gconfig.DocumentConfig == nil {
		fmt.Errorf("documentdb configuration is missing")
		return nil
	}

	documentsConfig := gconfig.DocumentConfig

	ilog.Debug(fmt.Sprintf("initialize Documents, %v", documentsConfig))

	var DatabaseType = documentsConfig["type"].(string)             // "mongodb"
	var DatabaseConnection = documentsConfig["connection"].(string) //"mongodb://localhost:27017"
	var DatabaseName = documentsConfig["database"].(string)         //"IAC_CFG"

	if DatabaseType == "" {
		ilog.Error(fmt.Sprintf("initialize Documents error: %s", "DatabaseType is missing"))
		return nil
	}
	if DatabaseConnection == "" {
		ilog.Error(fmt.Sprintf("initialize Documents error: %s", "DatabaseConnection is missing"))
		return nil
	}
	if DatabaseName == "" {
		ilog.Error(fmt.Sprintf("initialize Documents error: %s", "DatabaseName is missing"))
		return nil
	}

	documents.ConnectDB(DatabaseType, DatabaseConnection, DatabaseName)

	return documents.DocDBCon
}
