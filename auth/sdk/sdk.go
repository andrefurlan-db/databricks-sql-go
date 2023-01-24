package sdk

import (
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sql-go/auth"
)

type SdkAuth struct {
	config.Config
}

var _ auth.Authenticator = (*SdkAuth)(nil)
