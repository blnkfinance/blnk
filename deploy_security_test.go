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
	"os"
	"strings"
	"testing"
)

func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestShippedInstallIsNotAPublicOpenLedger(t *testing.T) {
	service := readRepoFile(t, "infrastructure/k8s-manifests/server-service.yaml")
	if strings.Contains(service, "LoadBalancer") {
		t.Fatal("server Service is still a LoadBalancer")
	}
	if !strings.Contains(service, "type: ClusterIP") {
		t.Fatal("server Service is not ClusterIP")
	}
	if strings.Contains(service, `port: 80`) || strings.Contains(service, `port: 443`) {
		t.Fatal("server Service still publishes port 80 or 443")
	}

	configMap := readRepoFile(t, "infrastructure/k8s-manifests/blnk-config.yaml")
	if !strings.Contains(configMap, `"secure": true`) {
		t.Fatal("bundled blnk.json does not enable server.secure")
	}
	if strings.Contains(configMap, "password") || strings.Contains(configMap, "secret_key") {
		t.Fatal("bundled blnk.json still contains a database password or master key")
	}

	for _, path := range []string{
		"infrastructure/k8s-manifests/server-deployment.yaml",
		"infrastructure/k8s-manifests/worker-deployment.yaml",
	} {
		deployment := readRepoFile(t, path)
		for _, key := range []string{"server-secret-key", "data-source-dns", `BLNK_SERVER_SECURE`} {
			if !strings.Contains(deployment, key) {
				t.Fatalf("%s does not wire %s", path, key)
			}
		}
	}

	secrets := readRepoFile(t, "infrastructure/k8s-manifests/blnk-secrets.yaml")
	for _, line := range []string{
		`server-secret-key: ""`,
		`postgres-password: ""`,
		`data-source-dns: ""`,
	} {
		if !strings.Contains(secrets, line) {
			t.Fatalf("blnk-secrets.yaml is missing an empty %s", line)
		}
	}

	postgres := readRepoFile(t, "infrastructure/k8s-manifests/postgres-statefulset.yaml")
	if strings.Contains(postgres, "value: password") {
		t.Fatal("postgres StatefulSet still uses the default password")
	}
	if !strings.Contains(postgres, "postgres-password") {
		t.Fatal("postgres StatefulSet does not read its password from blnk-secrets")
	}

	for _, path := range []string{"docker-compose.yaml", "docker-compose.dev.yaml"} {
		compose := readRepoFile(t, path)
		for _, binding := range []string{
			`"5001:5001"`,
			`"5004:5004"`,
			`"${POSTGRES_OUTER_PORT:-5432}:5432"`,
		} {
			if strings.Contains(compose, binding) {
				t.Fatalf("%s still publishes %s on all host interfaces", path, binding)
			}
		}
		for _, binding := range []string{
			`"127.0.0.1:5001:5001"`,
			`"127.0.0.1:5004:5004"`,
			`"127.0.0.1:${POSTGRES_OUTER_PORT:-5432}:5432"`,
		} {
			if !strings.Contains(compose, binding) {
				t.Fatalf("%s does not bind %s to localhost", path, binding)
			}
		}
	}
}
