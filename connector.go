package dbsql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/databricks/databricks-sql-go/auth/noop"
	"github.com/databricks/databricks-sql-go/auth/sdk"
	"github.com/databricks/databricks-sql-go/auth/token"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/databricks/databricks-sql-go/internal/cli_service"
	"github.com/databricks/databricks-sql-go/internal/client"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/databricks/databricks-sql-go/logger"
	"golang.org/x/oauth2"
)

type connector struct {
	cfg    *config.Config
	client *http.Client
}

// Connect returns a connection to the Databricks database from a connection pool.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	var catalogName *cli_service.TIdentifier
	var schemaName *cli_service.TIdentifier
	if c.cfg.Catalog != "" {
		catalogName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Catalog))
	}
	if c.cfg.Schema != "" {
		schemaName = cli_service.TIdentifierPtr(cli_service.TIdentifier(c.cfg.Schema))
	}

	tclient, err := client.InitThriftClient(c.cfg, c.client)
	if err != nil {
		return nil, wrapErr(err, "error initializing thrift client")
	}

	session, err := tclient.OpenSession(ctx, &cli_service.TOpenSessionReq{
		ClientProtocol: c.cfg.ThriftProtocolVersion,
		Configuration:  make(map[string]string),
		InitialNamespace: &cli_service.TNamespace{
			CatalogName: catalogName,
			SchemaName:  schemaName,
		},
		CanUseMultipleCatalogs: &c.cfg.CanUseMultipleCatalogs,
	})

	if err != nil {
		return nil, wrapErrf(err, "error connecting: host=%s port=%d, httpPath=%s", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath)
	}

	conn := &conn{
		id:      client.SprintGuid(session.SessionHandle.GetSessionId().GUID),
		cfg:     c.cfg,
		client:  tclient,
		session: session,
	}
	log := logger.WithContext(conn.id, driverctx.CorrelationIdFromContext(ctx), "")

	log.Info().Msgf("connect: host=%s port=%d httpPath=%s", c.cfg.Host, c.cfg.Port, c.cfg.HTTPPath)

	for k, v := range c.cfg.SessionParams {
		setStmt := fmt.Sprintf("SET `%s` = `%s`;", k, v)
		_, err := conn.ExecContext(ctx, setStmt, []driver.NamedValue{})
		if err != nil {
			return nil, err
		}
		log.Info().Msgf("set session parameter: param=%s value=%s", k, v)
	}
	return conn, nil
}

// Driver returns underlying databricksDriver for compatibility with sql.DB Driver method
func (c *connector) Driver() driver.Driver {
	return &databricksDriver{}
}

var _ driver.Connector = (*connector)(nil)

type connOption func(*config.Config)

// NewConnector creates a connection that can be used with `sql.OpenDB()`.
// This is an easier way to set up the DB instead of having to construct a DSN string.
func NewConnector(options ...connOption) (driver.Connector, error) {
	// config with default options
	cfg := config.WithDefaults()

	for _, opt := range options {
		opt(cfg)
	}

	client := client.RetryableClient(cfg)

	return &connector{cfg: cfg, client: client}, nil
}

func withUserConfig(ucfg config.UserConfig) connOption {
	return func(c *config.Config) {
		c.UserConfig = ucfg
	}
}

// WithServerHostname sets up the server hostname. Mandatory.
func WithServerHostname(host string) connOption {
	return func(c *config.Config) {
		if host == "localhost" {
			c.Protocol = "http"
			c.Authenticator = &noop.NoopAuth{}
		}
		c.Host = host
		switch v := c.Authenticator.(type) {
		case *sdk.SdkAuth:
			v.Host = host
		}
	}
}

// WithPort sets up the server port. Mandatory.
func WithPort(port int) connOption {
	return func(c *config.Config) {
		c.Port = port
	}
}

// WithRetries sets up retrying logic. Sane defaults are provided. Negative retryMax will disable retry behavior
// By default retryWaitMin = 1 * time.Second
// By default retryWaitMax = 30 * time.Second
// By default retryMax = 4
func WithRetries(retryMax int, retryWaitMin time.Duration, retryWaitMax time.Duration) connOption {
	return func(c *config.Config) {
		c.RetryWaitMax = retryWaitMax
		c.RetryWaitMin = retryWaitMin
		c.RetryMax = retryMax
	}
}

// WithHTTPPath sets up the endpoint to the warehouse. Mandatory.
func WithHTTPPath(path string) connOption {
	return func(c *config.Config) {
		if !strings.HasPrefix(path, "/") {
			path = "/" + path
		}
		c.HTTPPath = path
	}
}

// WithMaxRows sets up the max rows fetched per request. Default is 10000
func WithMaxRows(n int) connOption {
	return func(c *config.Config) {
		if n != 0 {
			c.MaxRows = n
		}
	}
}

// WithTimeout adds timeout for the server query execution. Default is no timeout.
func WithTimeout(n time.Duration) connOption {
	return func(c *config.Config) {
		c.QueryTimeout = n
	}
}

// Sets the initial catalog name and schema name in the session.
// Use <select * from foo> instead of <select * from catalog.schema.foo>
func WithInitialNamespace(catalog, schema string) connOption {
	return func(c *config.Config) {
		c.Catalog = catalog
		c.Schema = schema
	}
}

// Used to identify partners. Set as a string with format <isv-name+product-name>.
func WithUserAgentEntry(entry string) connOption {
	return func(c *config.Config) {
		c.UserAgentEntry = entry
	}
}

// Sessions params will be set upon opening the session by calling SET function.
// If using connection pool, session params can avoid successive calls of "SET ..."
func WithSessionParams(params map[string]string) connOption {
	return func(c *config.Config) {
		for k, v := range params {
			if strings.ToLower(k) == "timezone" {
				if loc, err := time.LoadLocation(v); err != nil {
					logger.Error().Msgf("timezone %s is not valid", v)
				} else {
					c.Location = loc
				}

			}
		}
		c.SessionParams = params
	}
}

func WithNoAuth() connOption {
	return func(c *config.Config) {
		c.Authenticator = &noop.NoopAuth{}
	}
}

// WithAccessToken sets up the Personal Access Token. Mandatory for now.
func WithAccessToken(accessToken string) connOption {
	return func(c *config.Config) {
		c.AccessToken = accessToken
		tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
		c.Authenticator = &token.TokenAuth{
			TokenSource: tokenSource,
		}
	}
}

// WithTokenSource allows for custom token-based authentication flows
func WithTokenSource(tokenSource oauth2.TokenSource) connOption {
	return func(c *config.Config) {
		c.Authenticator = &token.TokenAuth{
			TokenSource: tokenSource,
		}
	}
}

func WithAzureADAuth(AzureClientID, AzureClientSecret, AzureTenantID, AzureResourceID string) connOption {
	return func(c *config.Config) {
		sdkAuth := &sdk.SdkAuth{}
		sdkAuth.Host = c.Host
		sdkAuth.AzureClientID = AzureClientID
		sdkAuth.AzureClientSecret = AzureClientSecret
		sdkAuth.AzureTenantID = AzureTenantID
		sdkAuth.AzureResourceID = AzureResourceID
		c.Authenticator = sdkAuth
	}
}

func WithGoogleServiceAccountAuth(serviceAccount string) connOption {
	return func(c *config.Config) {
		sdkAuth := &sdk.SdkAuth{}
		sdkAuth.Host = c.Host
		sdkAuth.GoogleServiceAccount = serviceAccount
	}
}

func WithGoogleCredentialsAuth(credentials string) connOption {
	return func(c *config.Config) {
		sdkAuth := &sdk.SdkAuth{}
		sdkAuth.Host = c.Host
		sdkAuth.GoogleCredentials = credentials
	}
}

/*
// Config represents configuration for Databricks Connectivity
type Config struct {
	// Connection profile specified within ~/.databrickscfg.
	Profile string `name:"profile" env:"DATABRICKS_CONFIG_PROFILE"`

	// Location of the Databricks CLI credentials file, that is created
	// by `databricks configure --token` command. By default, it is located
	// in ~/.databrickscfg.
	ConfigFile string `name:"config_file" env:"DATABRICKS_CONFIG_FILE"`

	GoogleServiceAccount string `name:"google_service_account" env:"DATABRICKS_GOOGLE_SERVICE_ACCOUNT" auth:"google"`
	GoogleCredentials    string `name:"google_credentials" env:"GOOGLE_CREDENTIALS" auth:"google,sensitive"`

	// Azure Resource Manager ID for Azure Databricks workspace, which is exchanged for a Host
	AzureResourceID string `name:"azure_workspace_resource_id" env:"DATABRICKS_AZURE_RESOURCE_ID" auth:"azure"`

	AzureUseMSI       bool   `name:"azure_use_msi" env:"ARM_USE_MSI" auth:"azure"`
	AzureClientSecret string `name:"azure_client_secret" env:"ARM_CLIENT_SECRET" auth:"azure,sensitive"`
	AzureClientID     string `name:"azure_client_id" env:"ARM_CLIENT_ID" auth:"azure"`
	AzureTenantID     string `name:"azure_tenant_id" env:"ARM_TENANT_ID" auth:"azure"`

	// AzureEnvironment (Public, UsGov, China, Germany) has specific set of API endpoints.
	AzureEnvironment string `name:"azure_environment" env:"ARM_ENVIRONMENT"`

	// Azure Login Application ID. Must be set if authenticating for non-production workspaces.
	AzureLoginAppID string `name:"azure_login_app_id" env:"DATABRICKS_AZURE_LOGIN_APP_ID" auth:"azure"`

	// When multiple auth attributes are available in the environment, use the auth type
	// specified by this argument. This argument also holds currently selected auth.
	AuthType string `name:"auth_type" env:"DATABRICKS_AUTH_TYPE" auth:"-"`


	ClientID     string `name:"client_id" env:"DATABRICKS_CLIENT_ID"`
	ClientSecret string `name:"client_secret" env:"DATABRICKS_CLIENT_SECRET"`

}
*/

// type TokenSource interface {
// 	// Token returns a token or an error.
// 	// Token must be safe for concurrent use by multiple goroutines.
// 	// The returned Token must not be modified.
// 	Token() (*Token, error)
// }
