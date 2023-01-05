package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	_ "github.com/databricks/databricks-sql-go/logger"
)

func init() {
	sql.Register("databricks", &databricksDriver{})
}

type databricksDriver struct{}

// Open returns a new connection to Databricks database with a DSN string.
// Use sql.Open("databricks", <dsn string>) after importing this driver package.
func (d *databricksDriver) Open(dsn string) (driver.Conn, error) {
	cfg := config.WithDefaults()
	userCfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.UserConfig = userCfg
	c := client.RetryableClient(cfg)
	c.Timeout = cfg.ClientTimeout
	cn := &connector{
		cfg:    cfg,
		client: c,
	}
	return cn.Connect(context.Background())
}

// OpenConnector returns a new Connector.
// Used by sql.DB to obtain a Connector and invoke its Connect method to obtain each needed connection.
func (d *databricksDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg := config.WithDefaults()
	ucfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	cfg.UserConfig = ucfg
	c := client.RetryableClient(cfg)
	c.Timeout = cfg.ClientTimeout

	return &connector{cfg: cfg, client: c}, nil
}

var _ driver.Driver = (*databricksDriver)(nil)
var _ driver.DriverContext = (*databricksDriver)(nil)
