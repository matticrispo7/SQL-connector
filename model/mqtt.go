package model

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/exp/slices"
)

type ServerMQTT struct {
	ConfigID string `json:"configId"`
	Mac      string `json:"mac"`
	Cloud    struct {
		BufferSize  string `json:"bufferSize"`
		MinInterval string `json:"minInterval"`
		SampleTime  string `json:"sampleTime"`
		ProjectID   string `json:"projectId"`
	} `json:"cloud"`
	Data []DataMQTT `json:"data"`
}

type DataMQTT struct {
	TypeOfData string         `json:"typeOfData"`
	Values     []VariableMQTT `json:"values"`
}
type VariableMQTT struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Datatype    string `json:"datatype"`
	Dimension   int    `json:"dimension"`
}

type MessageMQTT struct {
	Signals []string `json:"signals"`
	//Values    map[string]string `json:"values"`
	Values [][]string `json:"values"`
}

/*
InitMQTT will initialize the connection to the MQTT broker on the given topic.
It will automatically get the hashed-device-id and build the "path" of the publish topics.
*/
func InitMQTT(logger *log.Logger, config *Config, idx int) mqtt.Client {
	fmt.Println("**** Initializing MQTT connection for ", config.MQTT.ServerName[idx])
	var messageHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
		// called when a message is received that does not match any known subscriptions
		logger.Printf("[MQTT -- %s] Received id %s from topic: %s\n", config.MQTT.ServerName[idx], msg.Payload(), msg.Topic())
		// set the hashed client ID and build the PubTopic
		hashed_id := string(msg.Payload())
		config.SetHashedDeviceID(hashed_id)
		// unsubscribe from this topic
		if token := client.Unsubscribe(msg.Topic()); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		}
		logger.Printf("[MQTT -- %s] Unsubscribed from topic %s \n", config.MQTT.ServerName[idx], msg.Topic())
	}

	var connectionHandler mqtt.OnConnectHandler = func(c mqtt.Client) {
		fmt.Println("[MQTT] Connected")
		logger.Println("MQTT client connected to broker")
	}

	var connectionLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
		fmt.Printf("[MQTT] Connection lost: %v", err)
		logger.Println("[ERROR] MQTT connection lost error: ", err)
	}

	opts := mqtt.NewClientOptions()
	broker := fmt.Sprintf("tcp://%s:1883", config.MQTT.Broker)
	opts.AddBroker(broker)
	opts.SetClientID(config.MQTT.ServerName[idx])
	opts.SetDefaultPublishHandler(messageHandler)
	opts.OnConnect = connectionHandler
	opts.OnConnectionLost = connectionLostHandler

	return mqtt.NewClient(opts)
}

/*
BuildPayload params:
mqttVars 	   -> list of the MQTT variables handled by the connection to the server
data 	 	   -> 2D matrix where each row has the values retrieved by the query for the selected columns
timestampIndex -> the index of the timestamp column in the SQL table
*/
func BuildPayload(mqttVars []string, data [][]string, timestampIndex uint16, logger *log.Logger) [][]byte {
	// read README for the mqtt message json structure

	/* If the #data to send exceeds the threshold N, then build multiple payloads with at most
	N "messages" in the 'values' field */
	fmt.Println("[MQTT] Total data to send: ", len(data))
	N := 30                          // max size of the 'values' field to send in a single mqtt message
	iteration := (len(data) / N) + 1 // total "blocks" of messages to build and send
	var msgToSend [][]byte           // 2D slice where each value is a "block" of messages (with at most N messages) to send
	timestamp := strconv.FormatInt(int64(time.Now().UnixMilli()), 10)
	for c := 0; c < iteration; c++ {
		signals := []string{"timestamp"}
		signals = append(signals, mqttVars...)
		var completeMessage [][]string
		if c == iteration-1 {
			// do not set the upper bound while getting data -> get all the remaining data
			for _, el := range data[c*N:] { // 'el' is the list with the variables values to send in mqtt
				insertDataInPayload(&completeMessage, el, timestampIndex, timestamp)
			}
		} else {
			// set the upper bound in data -> get the next N values
			for _, el := range data[c*N : (c+1)*N] {
				insertDataInPayload(&completeMessage, el, timestampIndex, timestamp)
			}
		}
		msg, err := json.Marshal(MessageMQTT{Signals: signals, Values: completeMessage})
		if err != nil {
			logger.Printf("[MQTT] *** Error building msg json")
		}
		msgToSend = append(msgToSend, msg) // save this block of messages
	}

	// logging
	for _, t := range msgToSend {
		fmt.Printf("[MQTT] Message built: %+v\n", string(t))
	}

	return msgToSend

}

/*
insertDataInPayload will append the data parameter (slice of string of the mqtt message) to
the msg parameter (which is the "complete" mqtt message, with fixed size, to send)

@param msg -> 2D matrix where each row is a list with the values of the sql row retrived by the query

@param data -> list with the values of the sql row retrived by the query

@param timestampIndex -> the position in the column of the timestamp/time

@param timestampValue -> is the timestamp used for all the messages splitted in blocks
*/
func insertDataInPayload(msg *[][]string, data []string, timestampIndex uint16, timestampValue string) {
	fmt.Printf("[MQTT build payload] data: %s\n", data)
	values := []string{timestampValue} // first value is the timestamp
	// save the value of timestamp retrieved from query
	timestamp := data[timestampIndex]
	// remove it from data
	data = append(data[:timestampIndex], data[timestampIndex+1:]...)
	// append timestamp to data
	data = append(data, timestamp)
	values = append(values, data...) // add the other values to the slice
	*msg = append(*msg, values)
}

func PublishMessage(mqttVars []string, data [][]string, mqttClient mqtt.Client, topic string, pubTimeout uint16, timestampIndex uint16, logger *log.Logger) {
	messages := BuildPayload(mqttVars, data, timestampIndex, logger)
	for _, m := range messages {
		// publish message retrieved from channel to topic
		t := mqttClient.Publish(topic, 0, false, string(m))
		time.Sleep(time.Duration(pubTimeout) * time.Millisecond)
		// Handle the token in a go routine so this loop keeps sending messages regardless of delivery status
		go func() {
			_ = t.Wait()
			if t.Error() != nil {
				fmt.Printf("Error publishing: %s\n", t.Error())
				logger.Println("[ERROR] MQTT Error publishing: ", t.Error())
				return
			}
			logger.Println("MQTT message for timestamp successfully published")
		}()
	}

}

/*
CreateMQTTServerConnection will build the json message that's processed by the broker to create new connection with given
variables' structure.

@param sqlIndex -> is the index of the SQL configuration (in the config's sql server list) that this MQTT client refers to
@param colNames -> the list of columns names used to build the MQTT var's name and type
*/
func CreateMQTTServerConnection(mqttClient mqtt.Client, config *Config, sqlIdx int, colNames []string, logger *log.Logger) {
	fmt.Println("[MQTT] CREATING SERVER CONNECTION")
	logger.Println("Creating MQTT server connection")

	confServerMQTT := ServerMQTT{
		ConfigID: "sample_config_id",
		Mac:      "34:35:36:37:38:39",
		Data:     make([]DataMQTT, 0),
	}
	confServerMQTT.Cloud.BufferSize = ""
	confServerMQTT.Cloud.MinInterval = "0"
	confServerMQTT.Cloud.SampleTime = "1000"
	confServerMQTT.Cloud.ProjectID = ""
	datamqtt := DataMQTT{TypeOfData: config.MQTT.ServerName[sqlIdx], Values: make([]VariableMQTT, 0)}
	var mqttVarNames []string        // list of all the mqtt variables (used to update mqtt.Variable in config)
	for idx, col := range colNames { // create the mqtt variable
		name := fmt.Sprintf("SQL.%s", col)
		colType := getType(config.SQL[sqlIdx].DataTypes[idx])

		mqttVarNames = append(mqttVarNames, name)
		mqttVar := VariableMQTT{name, col, colType, 1}
		datamqtt.Values = append(datamqtt.Values, mqttVar)
	}
	confServerMQTT.Data = append(confServerMQTT.Data, datamqtt)
	// update the mqtt.Variables (in config) with the mqtt variables created
	config.UpdateVariablesMQTT(sqlIdx, mqttVarNames)

	msg, err := json.Marshal(confServerMQTT)
	if err != nil {
		logger.Println("[ERROR] MQTT Error marshaling conf server: ", err)
	}
	fmt.Println("[MQTT] ConfigServer: ", string(msg))

	// publish the MQTT message to create the new connection
	t := mqttClient.Publish(config.MQTT.ServerConfigTopic[sqlIdx], 0, false, string(msg))
	// Handle the token in a go routine so this loop keeps sending messages regardless of delivery status
	go func() {
		_ = t.Wait()
		if t.Error() != nil {
			logger.Println("[ERROR] MQTT Error publishing: ", t.Error())
			return
		}
		logger.Println("Published MQTT message to create new connection for ", config.MQTT.ServerName)
	}()
}

/*
getType will map the sql column datatype, passed as parameter, to a datatype handled by the server
@param t ->  the sql column datatype
*/
func getType(t string) string {
	sqlStringType := []string{"CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR",
		"NTEXT", "BINARY", "VARBINARY", "IMAGE"}
	mysqlStringType := []string{"CHAR", "VARCHAR", "BINARY", "VARBINARY", "TINYBLOB", "TINYTEXT",
		"TEXT", "BLOB", "MEDIUMTEXT", "MEDIUMBLOB", "LONGTEXT", "LONGBLOB", "ENUM", "SET"}
	postgresStringType := []string{"CHAR", "VARCHAR", "TEXT"}
	sqlIntType := []string{"BIT", "TINYINT", "SMALLINT", "INT", "BIGINT"}
	mysqlIntType := []string{"BIT", "TINYINT", "BOOL", "BOOLEAN", "SMALLINT", "MEDIUMINT",
		"INT", "INTEGER", "BIGINT"}
	postgresIntType := []string{"SMALLINT", "INT", "SERIAL"}
	sqlFloatType := []string{"DECIMAL", "NUMERIC", "SMALLMONEY", "MONEY", "FLOAT", "REAL"}
	mysqlFloatType := []string{"FLOAT", "DOUBLE", "DOUBLE PRECISION", "DECIMAL", "DEC"}
	postgresFloatType := []string{"FLOAT", "REAL", "FLOAT8", "NUMERIC"}
	sqlDateType := []string{"DATETIME", "DATETIME2", "SMALLDATETIME", "DATE", "TIME",
		"DATETIMEOFFSET", "TIMESTAMP"}
	mysqlDateType := []string{"DATE", "DATETIME", "TIME", "TIMESTAMP", "YEAR"}
	postgresDateType := []string{"DATE", "TIME", "TIMESTAMP", "TIMESTAMPZ"}
	if slices.Contains(sqlStringType, t) || slices.Contains(mysqlStringType, t) || slices.Contains(postgresStringType, t) {
		return "STRING"
	} else if slices.Contains(sqlIntType, t) || slices.Contains(mysqlIntType, t) || slices.Contains(postgresIntType, t) {
		return "INTEGER"
	} else if slices.Contains(sqlFloatType, t) || slices.Contains(mysqlFloatType, t) || slices.Contains(postgresFloatType, t) {
		return "FLOAT"
	} else if slices.Contains(sqlDateType, t) || slices.Contains(mysqlDateType, t) || slices.Contains(postgresDateType, t) {
		return "TIMESTAMP"
	}
	return ""
}
