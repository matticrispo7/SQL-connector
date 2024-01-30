package main

import (
	"fmt"
	"log"
	"sql-client/db"
	"sql-client/model"
	"sql-client/utils"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {

	// LOAD CONFIGURATION
	config := utils.LoadConfiguration("config/conf.json")

	// load logger
	logger := utils.InitLogger(config)

	// "global" vars
	var (
		sqlServers         int             = len(config.SQL) // total sql servers to use
		mqttCreateServer   bool            = config.MQTT.CreateServer
		mqttPublishTimeout uint16          = uint16(config.MQTT.PublishTimeout)
		wg                 *sync.WaitGroup = &sync.WaitGroup{}
		channels                           = make([]chan []string, sqlServers)
	)
	// initialize the SQL and MQTT slices (from the config) with fixed size
	config.MQTT.PubTopic = make([]string, sqlServers)
	config.MQTT.ServerConfigTopic = make([]string, sqlServers)
	config.MQTT.Variables = make([][]string, sqlServers)
	timestampTableIndex := make([]int, sqlServers) // list of the index of the time column in the table
	columnNames := make([][]string, sqlServers)

	for idx, serverConf := range config.SQL {
		/* The goroutines work in pairs. Since the user can define a list of SQL server to connect to (in the config), there is a relation
		between the SQL server at position X (in the SQL list in config) and the MQTT.ServerName, PubTopic and so on at the same position X.
		This is done to make it possible to retrieve, from these lists, all the infos about a SQL server by using its index retrieved from
		the list (in SQL property in config)
		For each SQL server, both a goroutine for the SQL and the MQTT connection is created. A channel is then used to send
		data between these 2 goroutines. So, given N SQL server configs, there will be :
			- N goroutines for SQL  connections
			- N goroutines for MQTT connections
			- N channels for sending data from a i-th SQL connection and the related i-th MQTT connection

		The goroutine for the SQL connection will query the data and the MQTT one will send the message to the built topic */

		// create the channel used by this goroutine and append to the list of channels
		ch := make(chan []string, 200)
		channels[idx] = ch
		// local variables for this sql-mqtt goroutines pairing
		var (
			sqlDriver           string = config.SQL[idx].Driver
			sqlServer           string = config.SQL[idx].Server
			sqlUser             string = config.SQL[idx].User
			sqlPassword         string = config.SQL[idx].Password
			sqlPort             uint16 = uint16(config.SQL[idx].Port)
			sqlDatabase         string = config.SQL[idx].Database
			sqlSampling         uint32 = uint32(config.SQL[idx].Sampling)
			sqlSchema           string = config.SQL[idx].Schema
			sqlTableName        string = config.SQL[idx].TableName
			sqlTimestampColName string = config.SQL[idx].TimestampColName
			serverName          string = config.MQTT.ServerName[idx]
		)

		// format the table name with the schema (if set in the config)
		if sqlSchema != "" {
			sqlTableName = fmt.Sprintf("%s.%s", sqlSchema, sqlTableName)
		}
		logger.Println("SQL TABLE: ", sqlTableName)
		// get the db object to query the data
		db := db.InitDB(sqlDriver, sqlServer, sqlUser, sqlPassword, sqlDatabase, sqlPort, logger)

		// index of the timestamp column in the sql table with ALL the columns
		timestampTableIndex[idx] = db.GetTimestampIndex(sqlTableName, sqlTimestampColName, logger)
		err := db.UpdateConfigColNamesAndTypes(&config, &timestampTableIndex, idx, sqlTableName, sqlTimestampColName, logger)

		if err != nil {
			logger.Fatalln("Error while checking columns names: ", err)
		}
		// update the columns names with the list of the columns selected (or ALL ones if the flag is enabled) with timestamp in last position
		columnNames[idx] = config.SQL[idx].Columns
		// if the user has disabled the flag to get all columns, update the timestamp index based on the list with the columns selected

		wg.Add(1)
		// get the data from SQL server from a different goroutine
		go func(wg *sync.WaitGroup, ch chan<- []string, logger *log.Logger, idx int) {
			defer wg.Done()
			firstIteration := true
			var (
				dataRetrieved       [][]string // records retrieved from the DB
				timestampValues     []uint64   // all the values of the timestamp sql column
				timestampDates      []string   // list where value[i] is the date associated to timestampValues[i]
				latestTimestampDate string     // the formatted date retrieved from the query for the max timestamp retrieved
				latestTimestamp     uint64     // latest timestamp retrieved (used to build the new query)
				idxLatestTimestamp  int        // index, in the list of timestamp values, of the max timestamp found (the idx will be used to get
			)
			for {
				if firstIteration { // get all records
					fmt.Printf("[SQL -- %s] Getting all records\n", serverName)
					dataRetrieved, timestampValues, timestampDates, err = db.GetAllRecords("", sqlTableName, sqlTimestampColName, idx, logger, config.SQL[idx].Driver, serverName)
					if err != nil {
						logger.Printf("[SQL %s] Error retrieving all records: %v\n", serverName, err)
						time.Sleep(time.Duration(sqlSampling) * time.Millisecond)
						continue
					}
					firstIteration = false
				} else { // get records with timestamp >= given timestamp
					dataRetrieved, timestampValues, timestampDates, err = db.GetAllRecords(latestTimestampDate, sqlTableName, sqlTimestampColName, idx, logger, config.SQL[idx].Driver, serverName)
					if err != nil {
						logger.Printf("[SQL -- %s] Error retrieving files with timestamp > %s: %s \n", serverName, latestTimestampDate, err)
						time.Sleep(time.Duration(sqlSampling) * time.Millisecond)
						continue
					}
				}

				// get the lastest timestamp if at least 1 record is retrieved
				if len(dataRetrieved) > 0 {
					latestTimestamp, idxLatestTimestamp = utils.GetMaxTimestamp(timestampValues)
					latestTimestampDate = timestampDates[idxLatestTimestamp]
				}
				logger.Printf("[SQL --  %s] Latest timestamp: %d ==> date: %s\n ", serverName, latestTimestamp, latestTimestampDate)
				// send the data retrieved to the channel
				for _, f := range dataRetrieved {
					logger.Printf("[SQL -- %s] Put %+v to the channel.\n", serverName, f)
					ch <- f
				}
				// send a "stop" message (its an empty slice) in the channel to inform the other goroutine to grab all the data in the channel
				// and send the mqtt message
				var stop = []string{"STOP"}
				logger.Printf("[SQL -- %s] Put %v to the channel\n", serverName, stop)
				ch <- stop

				time.Sleep(time.Duration(sqlSampling) * time.Millisecond)
			}
		}(wg, channels[idx], logger, idx)

		// MQTT client
		wg.Add(1)
		go func(wg *sync.WaitGroup, ch <-chan []string, sampling uint32, timestampIndex uint16, colNames []string, logger *log.Logger, idx int) {
			defer wg.Done()
			mqttClient := model.InitMQTT(logger, &config, idx)
			if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
				logger.Fatalf("Error while connecting MQTT client to broker (for connection %s): %v \n", serverName, token.Error())
			}
			// Get the hashed_client_ID subscribing to that topic and set the ID in the config
			if token := mqttClient.Subscribe("hashed-device-id", 0, nil); token.Wait() && token.Error() != nil {
				logger.Printf("[MQTT -- %s] Error subscribing to topic: %v\n", serverName, token.Error())
			}
			// wait until the the hashed-device-id is not retrieved
			for {
				if config.MQTT.HashedID != "" {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			// set the topics where sending the mqtt messages (including the one for the server configuration)
			pubTopic := fmt.Sprintf("%s/data-processor/0/extern/telemetry/%s", config.MQTT.HashedID, config.MQTT.ServerName[idx])
			serverConfigTopic := fmt.Sprintf("%s/data-processor/0/extern/telemetry/config/%s", config.MQTT.HashedID, config.MQTT.ServerName[idx])
			config.SetPubTopic(idx, pubTopic)
			config.SetPubServerConfigTopic(idx, serverConfigTopic)

			// hashed-client-id retrieved. Go on and create the server connection
			if mqttCreateServer {
				model.CreateMQTTServerConnection(mqttClient, &config, idx, colNames, logger) // once the mqtt vars are created, set them in the config
			}

			// read data from channel and publish to the topic
			for {
				// publish the first data (returned while checking if the channel is opened)
				var messages [][]string
				if d, isChannelOpen := <-ch; isChannelOpen {
					fmt.Printf("[MQTT -- %s] Channel open. %d elements in channel\n", serverName, len(ch))
					messages = append(messages, d)

					// check if the data received from the channel is the "STOP" message
					if len(d) == 1 && d[0] == "STOP" {
						model.PublishMessage(config.MQTT.Variables[idx], messages, mqttClient, config.MQTT.PubTopic[idx], mqttPublishTimeout, timestampIndex, logger)
						messages = messages[:0] // clear the slice
					}

					// loop over the remaining data in the channel and publish them
					for data := range ch {
						fmt.Printf("[MQTT -- %s] Retrieved data: %+v\n", serverName, data)
						// check if the data received from the channel is the "STOP" message
						if len(data) == 1 && data[0] == "STOP" {
							model.PublishMessage(config.MQTT.Variables[idx], messages, mqttClient, config.MQTT.PubTopic[idx], mqttPublishTimeout, timestampIndex, logger)
							messages = messages[:0] // clear the slice
						} else {
							messages = append(messages, data)
						}
					}
				} else {
					// do some logs and exit program
					logger.Fatalln("Channel closed")
				}
			}
		}(wg, channels[idx], config.SQL[idx].Sampling, uint16(timestampTableIndex[idx]), columnNames[idx], logger, idx)

	}
	wg.Wait()
}
