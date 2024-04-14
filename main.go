package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mdaxf/iac/com"
	"github.com/mdaxf/iac/config"
	dbconn "github.com/mdaxf/iac/databases"
	"github.com/mdaxf/iac/documents"
	"github.com/mdaxf/iac/engine/trancode"
	"github.com/mdaxf/iac/framework/callback_mgr"
	"github.com/mdaxf/iac/integration/mqttclient"
	iacmb "github.com/mdaxf/iac/integration/signalr"
	"github.com/mdaxf/iac/logger"
	"github.com/mdaxf/signalrsrv/signalr"
)

func main() {
	startTime := time.Now()

	gconfig, err := config.LoadGlobalConfig()

	if err != nil {
		log.Fatalf("Failed to load global configuration: %v", err)
		//ilog.Error(fmt.Sprintf("Failed to load global configuration: %v", err))
	}
	initializeloger(gconfig)

	ilog := logger.Log{ModuleName: logger.Framework, User: "System", ControllerName: "iac-mqtt"}
	ilog.Debug("Start the iac-mqtt")

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

	initializeMqttClient(gconfig, ilog, DB, docDB, IACMessageBusClient)

	elapsed := time.Since(startTime)
	ilog.PerformanceWithDuration("iac-mqtt.main", elapsed)

	//	waitForTerminationSignal()
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

func waitForTerminationSignal() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fmt.Println("\nShutting down...")

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
