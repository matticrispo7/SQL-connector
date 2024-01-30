package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"sql-client/model"
	"strconv"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

// InitLogger initialize a logger for each client that connects to a different host
func InitLogger(config model.Config) *log.Logger {
	// load logger
	e, err := os.OpenFile("./log/sql.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error opening log file: %v\n", err)
		os.Exit(1)
	}
	logger := log.New(e, "", log.Ldate|log.Ltime|log.Lmsgprefix)
	logger.SetOutput(&lumberjack.Logger{
		Filename:   "./log/sql.log",
		MaxSize:    config.LOG.Size,    // megabytes after which new file is created
		MaxBackups: config.LOG.Backups, // number of backups
		MaxAge:     config.LOG.Age,     // days
		LocalTime:  config.LOG.LocalTime,
		Compress:   config.LOG.Compress,
	})
	return logger
}
func GetIntEnv(name string) int {
	// get the env as string type, convert to int and return it
	env := os.Getenv(name)
	val, err := strconv.Atoi(env)
	if err != nil {
		panic(fmt.Sprintf("Error converting env %s from string to int: %s\n", env, err))
	}
	return val
}

func LoadConfiguration(file string) model.Config {
	var config model.Config
	f, err := os.ReadFile(file)
	if err != nil {
		log.Println(err)
	}
	json.Unmarshal([]byte(f), &config)

	// check if the IP needs to be automatically retrieved
	if config.MQTT.GetIP {
		fmt.Println("Getting IP of iface with lowest metric")
		config.MQTT.Broker = getIfaceIP()
	}
	return config
}

func getIfaceIP() string {
	cmd := exec.Command("route")
	out, err := cmd.Output()
	if err != nil {
		fmt.Println("Error executing route: ", err)
	}
	fmt.Println("route output: ", string(out))
	s := strings.Split(string(out), "\n") // split the output of route based on the new line
	for _, t := range s {
		fmt.Println("string: ", t)
	}
	routes := s[2:]     // get only the rows with info about a given route (the first 2 rows has useless info as route column names)
	metrics := []int{0} // metrics value
	var ifaces []string // interfaces names
	// get all the metric
	for _, r := range routes {
		if r != "" { // check if the route is not empty
			values := strings.Fields(r) // get a list with all the values, for a given route
			fmt.Printf("route %s   values %v\n", r, values)
			metric, err := strconv.Atoi(values[4])
			ifaces = append(ifaces, values[len(values)-1])
			if err != nil {
				fmt.Println("Error while converting metric to int: ", metric)
			}
			metrics = append(metrics, metric)
		}
	}

	// find the lowest metric != 0 (0 is used for docker networks) with the iface name associated
	lowestMetric := 999999
	lowestMetricIface := ""
	for idx, t := range metrics {
		if t != 0 && t < lowestMetric {
			lowestMetric = t
			lowestMetricIface = ifaces[idx]
		}
	}
	fmt.Println("Metrics: ", metrics)
	fmt.Printf("Lowest metric: %d   for iface %s\n", lowestMetric, lowestMetricIface)
	// get the IP about the iface with lowest metric
	iface, err := net.InterfaceByName(lowestMetricIface)
	if err != nil {
		fmt.Println("Error while getting iface info: ", err)
	}
	// get the interface's address
	addrs, err := iface.Addrs()
	if err != nil {
		fmt.Println("Error while getting address for iface: ", err)
	}

	var ifaceIP string
	for _, a := range addrs {
		var ip net.IP
		// check the type of the address and
		// assign it to the variable ip of type net.IP
		switch v := a.(type) {
		case *net.IPAddr:
			ip = v.IP
		case *net.IPNet:
			ip = v.IP
		default:
			continue
		}
		// Print the IP address
		ifaceIP = ip.String()
		fmt.Printf("\t%s: %s\n", lowestMetricIface, ip.String())
	}
	return ifaceIP
}

/*
Will check if the given string is contained in the slice
*/
//TODO: substitute with interfaces to have a single method
func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func ContainsInt(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

/* indexOf return the index the of the string in the slice of strings */
func indexOf(element string, data []string) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

/* Remove will remove the string value passed from the passed slice */
func Remove(l []string, item string) []string {
	for i, other := range l {
		if other == item {
			return append(l[:i], l[i+1:]...)
		}
	}
	return nil // TODO: check if ok return value
}

func GetMaxTimestamp(timestampValues []uint64) (uint64, int) {
	maxTimestamp := uint64(0)
	idxMaxTimestamp := 0
	for idx, t := range timestampValues {
		if t > maxTimestamp {
			maxTimestamp = t
			idxMaxTimestamp = idx
		}
	}
	fmt.Printf("[SQL] MaxTimestamp: %d at position %d\n", maxTimestamp, idxMaxTimestamp)
	return maxTimestamp, idxMaxTimestamp
}
