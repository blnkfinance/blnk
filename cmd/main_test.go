/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The root pre-run must load the file named by --config. It used to load a
// hard-coded "blnk.json" from the working directory instead, so the flag was
// silently ignored and Blnk fell back to environment variables.
//
// The env vars keep the old behaviour bootable, so a regression fails on the
// project name assertion rather than on log.Fatal in preRun.
func TestRootCommandHonoursConfigFlag(t *testing.T) {
	dsn := "postgres://postgres:@localhost:5432/blnk?sslmode=disable"
	t.Setenv("BLNK_DATA_SOURCE_DNS", dsn)
	t.Setenv("BLNK_REDIS_DNS", "localhost:6379")

	path := filepath.Join(t.TempDir(), "custom.json")
	body, err := json.Marshal(map[string]any{
		"project_name": "config-flag-test",
		"data_source":  map[string]string{"dns": dsn},
		"redis":        map[string]string{"dns": "localhost:6379"},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, body, 0o600))

	cli := NewCLI()
	cli.cmd.SetArgs([]string{"--config", path})
	require.NoError(t, cli.cmd.Execute(), "Postgres and Redis must be running for the cmd test suite")

	cnf, err := config.Fetch()
	require.NoError(t, err)
	assert.Equal(t, "config-flag-test", cnf.ProjectName, "configuration must come from the --config file")
}

func TestRootCommandConfigFlagDefault(t *testing.T) {
	flag := NewCLI().cmd.PersistentFlags().Lookup("config")
	require.NotNil(t, flag)
	assert.Equal(t, "./blnk.json", flag.DefValue)
}
