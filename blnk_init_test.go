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
package blnk_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBlnkInit(t *testing.T) {
	script, err := os.ReadFile("blnk-init.sh")
	if err != nil {
		t.Fatal(err)
	}
	var controls strings.Builder
	// Environment variables cannot contain NUL; cover every other JSON control byte.
	for c := byte(1); c < 32; c++ {
		controls.WriteByte(c)
	}
	const defaultDSN = "postgres://postgres:password@postgres:5432/blnk?sslmode=disable"
	tests := []struct {
		name    string
		env     []string
		project string
		dsn     string
	}{
		{name: "defaults", project: "Blnk", dsn: defaultDSN},
		{
			name: "encoded credentials and database",
			env: []string{
				"POSTGRES_USER=user@name", "POSTGRES_PASSWORD=p@ss word:/?#%\\\"é",
				"POSTGRES_DB=ledger/name space", "LC_ALL=C.UTF-8",
			},
			project: "Blnk",
			dsn:     "postgres://user%40name:p%40ss%20word%3A%2F%3F%23%25%5C%22%C3%A9@postgres:5432/ledger%2Fname%20space?sslmode=disable",
		},
		{
			name:    "project line break and JSON escapes",
			env:     []string{"BLNK_PROJECT_NAME=My \"project\"\\name\n"},
			project: "My \"project\"\\name\n", dsn: defaultDSN,
		},
		{
			name: "all control characters",
			env: []string{
				"BLNK_PROJECT_NAME=Project" + controls.String(),
				"BLNK_DATA_SOURCE_DNS=postgres://override/" + controls.String(),
			},
			project: "Project" + controls.String(),
			dsn:     "postgres://override/" + controls.String(),
		},
		{
			name: "explicit DSN override",
			env: []string{
				"POSTGRES_PASSWORD=ignored",
				"BLNK_DATA_SOURCE_DNS=postgres://custom:p%40ss@db/ledger?sslmode=require\t\r\n",
			},
			project: "Blnk",
			dsn:     "postgres://custom:p%40ss@db/ledger?sslmode=require\t\r\n",
		},
	}
	for _, shell := range []string{"sh", "busybox"} {
		t.Run(shell, func(t *testing.T) {
			if _, err := exec.LookPath(shell); err != nil {
				t.Skipf("%s unavailable: %v", shell, err)
			}
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					dir := t.TempDir()
					scriptPath := filepath.Join(dir, "blnk-init.sh")
					if err := os.WriteFile(scriptPath, script, 0600); err != nil {
						t.Fatal(err)
					}
					args := []string{"-c", `umask 022; . "$1"`, scriptPath, scriptPath}
					if shell == "busybox" {
						args = append([]string{"sh"}, args...)
					}
					run := func() {
						t.Helper()
						cmd := exec.Command(shell, args...)
						// Isolate the test from the developer's database configuration.
						cmd.Env = append([]string{"PATH=" + os.Getenv("PATH")}, tc.env...)
						if out, err := cmd.CombinedOutput(); err != nil {
							t.Fatalf("init failed: %v\n%s", err, out)
						}
					}
					run()
					configPath := filepath.Join(dir, "blnk.json")
					data, err := os.ReadFile(configPath)
					if err != nil {
						t.Fatal(err)
					}
					var config struct {
						ProjectName string `json:"project_name"`
						DataSource  struct {
							DNS string `json:"dns"`
						} `json:"data_source"`
					}
					if err := json.Unmarshal(data, &config); err != nil {
						t.Fatalf("invalid JSON: %v", err)
					}
					if config.ProjectName != tc.project || config.DataSource.DNS != tc.dsn {
						t.Errorf("got project %q, DSN %q; want project %q, DSN %q",
							config.ProjectName, config.DataSource.DNS, tc.project, tc.dsn)
					}
					info, err := os.Stat(configPath)
					if err != nil {
						t.Fatal(err)
					}
					if info.Mode().Perm() != 0600 {
						t.Errorf("config permissions = %o; want 600", info.Mode().Perm())
					}
					// An existing user-supplied configuration must survive another run.
					const existing = "custom configuration\n"
					if err := os.WriteFile(configPath, []byte(existing), 0600); err != nil {
						t.Fatal(err)
					}
					run()
					preserved, err := os.ReadFile(configPath)
					if err != nil || string(preserved) != existing {
						t.Fatalf("existing config changed: %q, error: %v", preserved, err)
					}
				})
			}
		})
	}
}
