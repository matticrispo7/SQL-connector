package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sort"
	"sql-client/model"
	"sql-client/utils"
	"strconv"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"golang.org/x/exp/slices"
)

type Database struct {
	SqlDb *sql.DB
}

var dbContext = context.Background()
var config = utils.LoadConfiguration("config/conf.json")

// get only the columns I'm interested to along with the timestampColumn (setted in conf.json)
var colsSelected = make([][]string, len(config.SQL))

/*
InitDB will initialize the connection to the SQL database, customizing the connection
configuration string based on the selected driver.

Return the database object to use to query the DB
*/
func InitDB(driver, server, user, pwd, dbName string, port uint16, logger *log.Logger) Database {
	// build different db config based on the sql driver to use
	var dbHandle *sql.DB
	var err error
	var connectionString string
	switch driver {
	case "mssql":
		connectionString = fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;",
			server, user, pwd, port, dbName)
	case "mysql":
		cfg := mysql.Config{
			User:                 user,
			Passwd:               pwd,
			Net:                  "tcp",
			Addr:                 fmt.Sprintf("%s:%d", server, port),
			DBName:               dbName,
			AllowNativePasswords: true,
		}
		connectionString = cfg.FormatDSN()

	case "postgres":
		connectionString = fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d sslmode=disable", user, pwd, dbName, server, port)
	}
	// Get a database handle.
	dbHandle, err = sql.Open(driver, connectionString)
	if err != nil {
		logger.Fatal("Error opening database: ", err)
	}
	logger.Printf("Connection opened to database %s@%s\n", dbName, server)
	return Database{dbHandle}
}

/*
UpdateConfigColNamesAndTypes sets, in the config object, the column names and types updated (if the flag to select all the columns
is enabled, the config's columns will be set with the list of all the columns in tha table [the same for the columns types]);
otherwise, if the user has selected only some columns, the config's columns will be set with only those columns [the same for the
data types])
In any case, the timestamp column (and its type) will ALWAYS be the last value in those lists
*/
func (db Database) UpdateConfigColNamesAndTypes(config *model.Config, tsTableIndex *[]int, sqlIdx int, tableName string, tsColName string, logger *log.Logger) error {
	var allColNames, colsDataType, colNames []string
	var colIndexes []int
	var types []*sql.ColumnType
	var err error

	allColNames, types, err = db.GetAllColumnNamesAndTypes(tableName, tsColName, logger)
	if err != nil {
		logger.Println("[SQL] Error getting columns names and types: ", err)
		return err
	}
	tsColIndex := slices.Index(allColNames, tsColName) // timestamp column index in the list with ALL columns names
	tsDataType := types[tsColIndex].DatabaseTypeName()
	if !config.SQL[sqlIdx].SelectAllColumns {
		confColIndexMapping := make(map[int]string)
		// get only the columns (with their indexes) defined in the config file (without considering now the timestamp column)
		for _, c := range config.SQL[sqlIdx].Columns {
			colIndex := slices.Index(allColNames, c)
			colIndexes = append(colIndexes, colIndex)
			confColIndexMapping[colIndex] = c
		}
		// since the cols defined by the user may have a different order than the cols' order in the table,
		// sort the colIndexes in asc order, append the colName and type to avoid MQTT related msg problems
		// to match the column's order found in the sql table
		sort.Ints(colIndexes)

		// update the timestamp index
		dec := findTimestampIndex(colIndexes, tsColIndex)
		(*tsTableIndex)[sqlIdx] = tsColIndex - dec

		for _, v := range colIndexes {
			// save the column type and name
			colsDataType = append(colsDataType, types[v].DatabaseTypeName())
			colNames = append(colNames, confColIndexMapping[v])
		}
		// save the timestamp col name
		colNames = append(colNames, tsColName)
	} else {
		// save all the columns types
		for idx, c := range types {
			if idx != tsColIndex {
				// save the datatype if is not the one for the timestamp column
				colsDataType = append(colsDataType, c.DatabaseTypeName())
			}
		}
		allColNames = utils.Remove(allColNames, tsColName)
		colNames = allColNames
		colNames = append(colNames, tsColName) // append the timestamp column to all the other ones
	}
	// save as last value the timestamp column and its type
	colsDataType = append(colsDataType, tsDataType)
	config.UpdateColumnNameSQL(sqlIdx, colNames)
	config.SetColumnDataTypes(sqlIdx, colsDataType)
	colsSelected[sqlIdx] = colNames
	return nil
}

/*
GetAllColumnNamesAndTypes get all the columns names (with their types) in the table.
Returns the slice with the columns names (in the last position there's the timestamp column) and the column data types
*/
func (db Database) GetAllColumnNamesAndTypes(tableName string, tsColName string, logger *log.Logger) ([]string, []*sql.ColumnType, error) {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	row, err := db.SqlDb.Query(query)
	if err != nil {
		logger.Println("Error query executing query for getting columns names: ", err)
		return nil, nil, err
	}
	// get columns names
	colNames, err := row.Columns()
	if err != nil {
		logger.Println("Error query while getting columns names: ", err)
		return nil, nil, err
	}

	types, err := row.ColumnTypes()
	if err != nil {
		logger.Println("Error query while getting columns types:: ", err)
		return nil, nil, err
	}
	err = row.Close()
	if err != nil {
		logger.Println("Error closing rows while getting columns names: ", err)
		return nil, nil, err
	}

	return colNames, types, nil
}

/*
	GetTimestampIndex will get the columns names from the SQL table and return the position of the timestamp/time column

@param tableName -> the SQL table name

@param tsColName -> the timestamp/time column's name
*/
func (db Database) GetTimestampIndex(tableName string, tsColName string, logger *log.Logger) int {
	query := fmt.Sprintf("SELECT * FROM %s", tableName)
	row, err := db.SqlDb.Query(query)
	if err != nil {
		logger.Fatalln("Error query executing query for getting columns names: ", err)
	}
	// get columns names
	colNames, err := row.Columns()
	if err != nil {
		logger.Fatalln("Error query while getting columns names: ", err)
	}

	tsColIndex := slices.Index(colNames, tsColName)
	return tsColIndex
}

/*
GetAllRecords will get the records from the table "data".
If the timestamp passed as parameter is "", a select * is performed.
Otherwise the timestamp passed (that is the latest date retrieved from the previous query)
is used as filter to select records with timestamp > timestamp parameter passed.

Returns the records retrieved from the table and the error.
*/
func (db Database) GetAllRecords(latestTimestamp string, tableName string, timestampColName string, sqlIdx int, logger *log.Logger, driver, serverName string) ([][]string, []uint64, []string, error) {
	var query string     // query to run based
	var colIndexes []int // indexes of the columns i'm interested to
	timestampValues := []uint64{}
	timestampDates := []string{} // list of the timestamp (in date format) retrieved from the query
	var records [][]string       // 2D matrix where each row contains the values with index contained in colIndexes

	// first check wheter the db connection is alive or not
	err := db.SqlDb.PingContext(dbContext)
	if err != nil {
		logger.Println("[ERROR] Error pinging database: ", err)
		return nil, nil, nil, err
	}

	// build the query based on the latest timestamp retrieved (if any)
	if latestTimestamp != "" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > '%s'", tableName, timestampColName, latestTimestamp)
		logger.Printf("[SQL %s] Query with timestamp > %s from table %s\n", serverName, latestTimestamp, tableName)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s", tableName)
		logger.Printf("[SQL %s] Query all data from table %s\n", serverName, tableName)
	}
	// execute the query on the given table
	rows, queryErr := db.SqlDb.Query(query)
	if queryErr != nil {
		return nil, nil, nil, queryErr
	}

	// get all the columns name in the rows retrieved
	cols, _ := rows.Columns()
	getColumnsSelectedIndex(tableName, cols, colsSelected[sqlIdx], &colIndexes)
	// get the index of the "timestampColumnName" found in the query
	timestampIndex := slices.Index(cols, timestampColName)

	// Loop through rows, using Scan to assign column data to struct fields.
	row := make([][]byte, len(cols))
	rowPtr := make([]any, len(cols))
	for i := range row {
		rowPtr[i] = &row[i]
	}

	// iterate over the rows queried and get the data in that row
	if rows.Next() {
		// get the first row
		err := getQueryData(rows, rowPtr, row, &records, colIndexes, timestampIndex, &timestampValues, &timestampDates, logger, driver)
		if err != nil {
			logger.Println("[ERROR] Error getting data from queried rows: ", err)
		}
		for rows.Next() { // get the other rows
			err := getQueryData(rows, rowPtr, row, &records, colIndexes, timestampIndex, &timestampValues, &timestampDates, logger, driver)
			if err != nil {
				logger.Println("[ERROR] Error getting data from queried rows: ", err)
			}
		}
		return records, timestampValues, timestampDates, nil
	} else {
		// empty result, return error
		return nil, nil, nil, fmt.Errorf("empty result from query")
	}

}

/*
getQueryData will save the data retrieved by the query in the @param records which will be
used later to build the MQTT message to send
@param records -> is the list with values that will be actually sent to mqtt topic
@param timestampValues -> is used to get all the timestamp and then find the maximum value (to use as a filter for next query)
*/
func getQueryData(rows *sql.Rows, rowPtr []any, row [][]byte, records *[][]string, colIndexes []int, timestampIndex int, timestampValues *[]uint64, timestampDates *[]string, logger *log.Logger, driver string) error {
	err := rows.Scan(rowPtr...)
	if err != nil {
		logger.Printf("Error scanning row: %v\n", err)
		return err
	}
	var valsToSave []string
	// extract the values from the row based of values' indexes (if they match the index of the columns to save)
	for idx, r := range row { // 'r' is a single value in the row queried
		if utils.ContainsInt(colIndexes, idx) {
			// format the timestamp and save its value if the indexes matches
			if idx == timestampIndex {
				*timestampDates = append(*timestampDates, string(r))
				timestamp := parseDateToTimestamp(string(r), driver)
				*timestampValues = append(*timestampValues, uint64(timestamp))
				valsToSave = append(valsToSave, strconv.FormatUint(uint64(timestamp), 10))
			} else { // save the string represtantion of the value
				valsToSave = append(valsToSave, string(r))
			}

		} else {
			fmt.Printf("[SQL] Index %d NOT in colIndexes: %v\n", idx, colIndexes)
		}

	}
	*records = append(*records, valsToSave)
	return nil
}

/*
getColumnsSelectedIndex will extract the indexes of the colums selected (defined in the config file)
and append them in the @param colIndexes
*/
func getColumnsSelectedIndex(tableName string, cols []string, colsSelected []string, colIndexes *[]int) {
	for idx, c := range cols {
		if check := slices.Contains(colsSelected, c); check {
			*colIndexes = append(*colIndexes, idx)
		}
	}
}

/*
This function will convert the date parameter retrieved from the query (in the position of the
timestamp column) to a timestamp (int64). The date parsing may differ based on the sql driver used
MySQL DATETIME and TIMESTAMP layout: 'YYYY-MM-DD hh:mm:ss'
SQL SERVER DATETIME layout: 'YYYY-MM-DD hh:mm:ss[.nnn]'
*/
func parseDateToTimestamp(date string, driver string) int64 {
	var dateLayout string
	switch driver {
	case "mysql":
		dateLayout = "2006-01-02 15:04:05" // standard datetime layout for mysql DATETIME or TIMESTAMP
	case "mssql", "postgres":
		dateLayout = time.RFC3339
	}
	t, err := time.Parse(dateLayout, date)
	if err != nil {
		fmt.Println(err)
	}
	return t.UTC().UnixMilli() // return the timestamp in milliseconds
}

/*
@ param indexes -> list with the indexes of the columns selected by the user (so the flag to get ALL the columns is disabled)
@ param tsIndex -> the index of the timestamp column referring to ALL the columns in the SQL table
*/
func findTimestampIndex(indexes []int, tsIndex int) int {
	/* find the timestamp index in the columns selected by the user. Basically decrease the timestamp index
	(referred to the list will ALL columns) by the number of columns' indexes not selected before the timestamp one.
	Example:
		all columns in table: 		 text | col1 | timestamp | col2
		columns selected:			 text | col2 (timestamp is auto appended at the end of this list)
		indexes of selected columns: 0 (text) | 3 (col2) --> do not consider the timestamp index
		timestamp index in ALL cols: 2
	Since col1 is not selected by the user, the data queried will have the form:  <text | timestamp | col2 > so the
	timestamp index, in the data queried, is 1 and not 2. So the timestamp index is decreased by the number of
	columns (not selected by the user) before the timestamp columns.
	So, from the first column (text) to the timestamp there is only 'col1' that is not selected by the user so the
	timestamp index is decreased by 1.
	*/
	c := 0
	for idx := 0; idx < tsIndex; idx++ {
		if idxFound := slices.Index(indexes, idx); idxFound == -1 {
			// the index is not one referred to a column selected by the user
			c += 1
		}
	}
	return c
}
