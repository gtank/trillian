package yolo_test

import (
	"context"
	"testing"

	"github.com/google/trillian"
	"github.com/google/trillian/crypto/keys"
	"github.com/google/trillian/extension"
	i "github.com/google/trillian/integration"
	"github.com/google/trillian/storage/yolo"
	"github.com/google/trillian/testonly/integration"
)

func TestInProcessLogIntegrationDuplicateLeaves(t *testing.T) {
	ctx := context.Background()
	const numSequencers = 2
	ms := yolo.NewLogStorage()

	reggie := extension.Registry{
		AdminStorage:  yolo.NewAdminStorage(ms),
		SignerFactory: keys.PEMSignerFactory{},
		LogStorage:    ms,
	}

	env, err := integration.NewLogEnvWithRegistry(ctx, numSequencers, "TestInProcessLogIntegrationDuplicateLeaves", reggie)
	if err != nil {
		t.Fatal(err)
	}
	defer env.Close()

	logID, err := env.CreateLog()
	if err != nil {
		t.Fatalf("Failed to create log: %v", err)
	}

	client := trillian.NewTrillianLogClient(env.ClientConn)
	params := i.DefaultTestParameters(logID)
	if err := i.RunLogIntegration(client, params); err != nil {
		t.Fatalf("Test failed: %v", err)
	}
}
