/*
Package dbsql implements the go driver to Databricks SQL

# Usage

Clients should use the database/sql package in conjunction with the driver:

	import (
		"database/sql"

		_ "github.com/databricks/databricks-sql-go"
	)

	func main() {
		db, err := sql.Open("databricks", "token:<token>@<hostname>:<port>/<endpoint_path>")

		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()
	}

# Connection via DSN (Data Source Name)

Use sql.Open() to create a database handle via a data source name string:

	db, err := sql.Open("databricks", "<dsn_string>")

The DSN format is:

	token:[my_token]@[hostname]:[port]/[endpoint http path]?param=value

Supported optional connection parameters can be specified in param=value and include:

  - catalog: Sets the initial catalog name in the session
  - schema: Sets the initial schema name in the session
  - maxRows: Sets up the max rows fetched per request. Default is 10000
  - timeout: Adds timeout (in seconds) for the server query execution. Default is no timeout
  - userAgentEntry: Used to identify partners. Set as a string with format <isv-name+product-name>

Supported optional session parameters can be specified in param=value and include:

  - ansi_mode: (Boolean string). Session statements will adhere to rules defined by ANSI SQL specification. Default is "false"
  - timezone: (e.g. "America/Los_Angeles"). Sets the timezone of the session

# Connection via new connector object

Use sql.OpenDB() to create a database handle via a new connector object created with dbsql.NewConnector():

	import (
		"database/sql"
		dbsql "github.com/databricks/databricks-sql-go"
	)

	func main() {
		connector, err := dbsql.NewConnector(
			dbsql.WithServerHostname(<hostname>),
			dbsql.WithPort(<port>),
			dbsql.WithHTTPPath(<http_path>),
			dbsql.WithAccessToken(<my_token>)
		)
		if err != nil {
			log.Fatal(err)
		}
		defer db.close()
	}

	db := sql.OpenDB(connector)

Supported functional options include:

  - WithServerHostname(<hostname> string): Sets up the server hostname. Mandatory
  - WithPort(<port> int): Sets up the server port. Mandatory
  - WithAccessToken(<my_token> string): Sets up the Personal Access Token. Mandatory
  - WithHTTPPath(<http_path> string): Sets up the endpoint to the warehouse. Mandatory
  - WithInitialNamespace(<catalog> string, <schema> string): Sets up the catalog and schema name in the session. Optional
  - WithMaxRows(<max_rows> int): Sets up the max rows fetched per request. Default is 10000. Optional
  - WithSessionParams(<params_map> map[string]string): Sets up session parameters including "timezone" and "ansi_mode". Optional
  - WithTimeout(<timeout> Duration). Adds timeout (in time.Duration) for the server query execution. Default is no timeout. Optional
  - WithUserAgentEntry(<isv-name+product-name> string). Used to identify partners. Optional

# Query cancellation and timeout

Cancelling a query via context cancellation or timeout is supported.

	// Set up context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)
	defer cancel()

	// Execute query. Query will be cancelled after 30 seconds if still running
	res, err := db.ExecContext(ctx, "CREATE TABLE example(id int, message string)")

# CorrelationId and ConnId

Use the driverctx package under driverctx/ctx.go to add CorrelationId and ConnId to the context.
CorrelationId and ConnId makes it convenient to parse and create metrics in logging.

**Connection Id**
Internal id to track what happens under a connection. Connections can be reused so this would track across queries.

**Query Id**
Internal id to track what happens under a query. Useful because the same query can be used with multiple connections.

**Correlation Id**
External id, such as request ID, to track what happens under a request. Useful to track multiple connections in the same request.

	ctx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")

# Logging

Use the logger package under logger.go to set up logging (from zerolog).
By default, logging level is `warn`. If you want to disable logging, use `disabled`.
The user can also utilize Track() and Duration() to custom log the elapsed time of anything tracked.

	import (
		dbsqllog "github.com/databricks/databricks-sql-go/logger"
		dbsqlctx "github.com/databricks/databricks-sql-go/driverctx"
	)

	func main() {
		// Optional. Set the logging level with SetLogLevel()
		if err := dbsqllog.SetLogLevel("debug"); err != nil {
			log.Fatal(err)
		}

		// Optional. Set logging output with SetLogOutput()
		// Default is os.Stderr. If running in terminal, logger will use ConsoleWriter to prettify logs
		dbsqllog.SetLogOutput(os.Stdout)

		// Optional. Set correlation id with NewContextWithCorrelationId
		ctx := dbsqlctx.NewContextWithCorrelationId(context.Background(), "workflow-example")


		// Optional. Track time spent and log elapsed time
		msg, start := logger.Track("Run Main")
		defer log.Duration(msg, start)

		db, err := sql.Open("databricks", "<dsn_string>")
		...
	}

The result log may look like this:

	{"level":"debug","connId":"01ed6545-5669-1ec7-8c7e-6d8a1ea0ab16","corrId":"workflow-example","queryId":"01ed6545-57cc-188a-bfc5-d9c0eaf8e189","time":1668558402,"message":"Run Main elapsed time: 1.298712292s"}

# Supported Data Types

==================================

Thrift Server Type --> Golang Type

==================================

TTypeId_BOOLEAN_TYPE      --> bool

TTypeId_TINYINT_TYPE      --> int8

TTypeId_SMALLINT_TYPE     --> int16

TTypeId_INT_TYPE          --> int32

TTypeId_BIGINT_TYPE       --> int64

TTypeId_FLOAT_TYPE        --> float32

TTypeId_DOUBLE_TYPE       --> float64

TTypeId_NULL_TYPE         --> nil

TTypeId_STRING_TYPE       --> string

TTypeId_CHAR_TYPE         --> string

TTypeId_VARCHAR_TYPE      --> string

TTypeId_DATE_TYPE         --> time.Time

TTypeId_TIMESTAMP_TYPE    --> time.Time

TTypeId_DECIMAL_TYPE      --> sql.RawBytes

TTypeId_BINARY_TYPE       --> sql.RawBytes

TTypeId_ARRAY_TYPE        --> sql.RawBytes

TTypeId_STRUCT_TYPE       --> sql.RawBytes

TTypeId_MAP_TYPE          --> sql.RawBytes

TTypeId_UNION_TYPE        --> sql.RawBytes

TTypeId_USER_DEFINED_TYPE --> *interface{}

TTypeId_INTERVAL_DAY_TIME_TYPE --> string

TTypeId_INTERVAL_YEAR_MONTH_TYPE --> string
*/
package dbsql
