package dbsql

import (
	"fmt"
	"testing"

	"github.com/databricks/databricks-sql-go/auth/token"
	"github.com/databricks/databricks-sql-go/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestOOpenConnector(t *testing.T) {
	t.Run("Should work with valid dsn", func(t *testing.T) {
		host := "databricks-host"
		port := 443
		accessToken := "token"
		httpPath := "http-path"
		expectedUserConfig := config.UserConfig{
			Host:          host,
			Port:          port,
			Protocol:      "https",
			AccessToken:   accessToken,
			HTTPPath:      "/" + httpPath,
			Authenticator: &token.TokenAuth{TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})},
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig.WithDefaults()
		d := &databricksDriver{}
		c, err := d.OpenConnector(fmt.Sprintf("token:%s@%s:%d/%s", accessToken, host, port, httpPath))
		require.NoError(t, err)
		coni, ok := c.(*connector)
		require.True(t, ok)
		assert.Equal(t, expectedCfg, coni.cfg)
		assert.NotNil(t, coni.client)
	})

}
