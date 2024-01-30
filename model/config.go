package model

type SQL_Config struct {
	Driver           string   `json:"driver"`
	Server           string   `json:"server"`
	User             string   `json:"user"`
	Password         string   `json:"password"`
	Port             uint16   `json:"port"`
	Database         string   `json:"database"`
	Sampling         uint32   `json:"sampling"`
	Schema           string   `json:"schema"`
	TableName        string   `json:"tableName"`
	TimestampColName string   `json:"timestampColumnName"`
	SelectAllColumns bool     `json:"selectAllColumns"`
	Columns          []string `json:"columns"`
	DataTypes        []string
}

type MQTT_Config struct {
	Broker            string `json:"IPtoCloud"`
	GetIP             bool   `json:"getIP"`
	HashedID          string
	CreateServer      bool     `json:"createServer"`
	ServerName        []string `json:"serverName"`
	ServerConfigTopic []string
	PubTopic          []string
	PublishTimeout    uint16 `json:"publishTimeout"`
	Variables         [][]string
}

type Logger_Config struct {
	Size      int  `json:"size"`
	Backups   int  `json:"backups"`
	Age       int  `json:"age"`
	LocalTime bool `json:"localTime"`
	Compress  bool `json:"compress"`
}

type Config struct {
	SQL  []SQL_Config  `json:"sql"`
	MQTT MQTT_Config   `json:"mqtt"`
	LOG  Logger_Config `json:"log"`
}

func (c *Config) UpdateColumnNameSQL(idx int, colNames []string) {
	c.SQL[idx].Columns = colNames
}

func (c *Config) SetColumnDataTypes(idx int, types []string) {
	c.SQL[idx].DataTypes = types
}

func (c *Config) SetHashedDeviceID(id string) {
	c.MQTT.HashedID = id
}

func (c *Config) SetPubTopic(idx int, t string) {
	c.MQTT.PubTopic[idx] = t
}

func (c *Config) SetPubServerConfigTopic(idx int, t string) {
	c.MQTT.ServerConfigTopic[idx] = t
}

func (c *Config) UpdateVariablesMQTT(idx int, vars []string) {
	c.MQTT.Variables[idx] = vars
}
