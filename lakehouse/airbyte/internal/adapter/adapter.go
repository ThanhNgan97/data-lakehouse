package adapter

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"

	"github.com/datalakehouse/airbyte-config-tool/internal/model"
)

// DBAdapter defines the common contract for any supported relational database
type DBAdapter interface {
	Connect(ctx context.Context, cfg model.DBConfig) error
	Close() error
	Ping(ctx context.Context) (*model.DBInfo, error)
	DiscoverSchemas(ctx context.Context) ([]string, error)
	DiscoverTables(ctx context.Context, schema string) ([]model.TableMetadata, error)
	GenerateDDL(plan model.ProvisionPlan) (string, error)
	GenerateAuditSQL(plan model.ProvisionPlan) (string, error)
	ApplyPlan(ctx context.Context, sqlScript string) error
	VerifyReader(ctx context.Context, cfg model.DBConfig, plan model.ProvisionPlan, testTarget string) (string, error)
}

// NewAdapter creates an adapter instance based on engine
func NewAdapter(engine string) (DBAdapter, error) {
	switch engine {
	case "postgres", "postgresql":
		return NewPostgresAdapter(), nil
	case "mysql", "mariadb":
		return NewMySQLAdapter(), nil
	case "mssql", "sqlserver":
		return NewMSSQLAdapter(), nil
	default:
		return nil, fmt.Errorf("unsupported database engine: %s (supported: postgres, mysql, mssql)", engine)
	}
}

// GenerateStrongPassword generates a secure random password
func GenerateStrongPassword(length int) (string, error) {
	if length < 16 {
		length = 24
	}
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#%*_+="
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if err != nil {
			return "", err
		}
		result[i] = chars[num.Int64()]
	}
	return string(result), nil
}
