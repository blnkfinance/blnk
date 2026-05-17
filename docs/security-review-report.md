# Security Review Report

Scope: local scan and targeted remediation in the Blnk Core ledger repository.

## Confirmed Issues

### API key material exposed through list operations

`Datasource.ListAPIKeys` returned the `Key` field for every API key record. API keys are bearer credentials, so returning stored key material from a list endpoint increases the blast radius of any caller, log, debug dump, or downstream consumer that can access list responses. The create path can still return the key once when appropriate; list responses should only expose metadata.

Fix: `database/api_key.go` now clears `apiKey.Key` before appending each listed key to the response. `database/api_key_test.go` was updated to assert list responses contain an empty key value.

### Webhook registration and updates accepted unsafe or malformed URLs and unbounded execution settings

Hook validation previously required only a non-empty URL. That allowed malformed URLs and non-HTTP schemes to be persisted, which could later be executed by the worker. Webhook execution is an outbound request path, so weak validation can enable unexpected local file or non-web scheme handling as HTTP client behavior evolves, operational misrouting, or avoidable SSRF-like risk where a caller controls hook destinations. Validation also allowed excessive timeout and retry settings, which could tie up workers or amplify outbound traffic.

Fix: `internal/hooks/manager.go` now parses hook URLs during registration and update, requires `http` or `https`, requires a host, preserves the existing default timeout and retry behavior, and rejects timeout values above 60 seconds or retry counts above 10. `internal/hooks/manager_test.go` adds regression coverage for unsupported URL schemes, malformed URLs, allowed HTTP(S) URLs, default timeout behavior, excessive timeout or retry values, and invalid update attempts.

### Webhook response bodies were read without a size limit

`executeHook` read the entire response body from remote webhook endpoints. A configured or compromised endpoint could return a very large body and cause excessive memory use in the worker process.

Fix: `internal/hooks/client.go` now caps hook response reads at 1 MiB and fails the hook if the response exceeds that limit.

### S3 backup uploads disabled SSL unconditionally

Backup manager construction always set `DisableSSL` to true for the S3 client. That forces backup uploads over plaintext HTTP even for HTTPS endpoints, exposing database backups and object-store credentials to network interception or tampering.

Fix: `internal/pg-backups/pg-backup-drive.go` now disables SSL only when the configured S3 endpoint explicitly uses the `http` scheme. `internal/pg-backups/pg-backup-drive_test.go` adds coverage for HTTP, HTTPS, empty, and invalid endpoints.

## Fixes Made

- Redacted API key secret material from API key list results.
- Added URL scheme and host validation for registered and updated hooks.
- Added bounded hook timeout and retry validation.
- Added a 1 MiB maximum webhook response body read.
- Preserved HTTPS for S3 backup clients by default, only disabling SSL for explicit HTTP endpoints.
- Added focused regression tests for the hook validation and S3 SSL decision logic.

## Change Explanations

The implemented fixes are intentionally narrow and avoid changing ledger posting, balance mutation, transaction state, reconciliation, or idempotency behavior. They only change credential exposure, outbound webhook validation/execution limits, and backup transport configuration.

Compatibility summary: the changes are defensive at security boundaries. They do not alter money movement semantics, database schema, transaction APIs, reconciliation behavior, or balance calculations. The intentional API-visible changes are that API key list responses no longer include bearer secret material, invalid webhook destinations are rejected earlier, oversized webhook responses fail instead of being fully buffered, and HTTPS S3 backup endpoints now keep TLS enabled.

### `database/api_key.go`

`ListAPIKeys` now blanks `apiKey.Key` before returning each listed record. This keeps list responses useful for management screens and API clients that need key metadata, while preventing bearer credential material from being exposed after creation. The corresponding test expectations in `database/api_key_test.go` now assert that listed keys have an empty `Key` field.

Behavior impact: API key creation and validation are unchanged; the create path can still return the plaintext key once, and authentication still verifies submitted keys against the stored hash. Only list responses are redacted.

### `internal/hooks/manager.go`

Hook validation now parses the configured URL and only accepts `http` or `https` URLs with a host. Registration and update both use this validation before storing hook changes. This prevents unsupported schemes and malformed destinations from being stored for later execution by the webhook worker. The same validation path now enforces upper bounds for hook timeout and retry count, preserving the existing defaults but rejecting values that could hold workers open or amplify outbound calls.

Behavior impact: valid HTTP(S) webhooks continue to register and update. Invalid schemes such as `file:` or `gopher:`, missing hosts, timeouts above 60 seconds, and retry counts above 10 are rejected before persistence.

Test coverage: `internal/hooks/manager_test.go` covers rejected schemes and malformed URLs, accepted HTTP(S) URLs, default timeout assignment, excessive timeout or retry values, and rejection of invalid updates without overwriting the stored hook.

### `internal/hooks/client.go`

Webhook execution now reads at most 1 MiB plus one byte from the response body. If the response exceeds the limit, the hook is marked failed and execution returns an error. This keeps a remote endpoint from forcing the worker to allocate memory for an unbounded response body.

Behavior impact: webhook request delivery semantics are unchanged for normal-sized responses. Oversized responses now fail the hook instead of being fully buffered in memory.

### `internal/pg-backups/pg-backup-drive.go`

S3 client setup now disables SSL only when the configured endpoint explicitly uses `http`. HTTPS endpoints keep TLS enabled, which avoids unintentionally sending database backups and object-store credentials over plaintext connections. The new helper is covered by tests for HTTP, HTTPS, empty, and malformed endpoint values.

Behavior impact: explicitly configured HTTP object-store endpoints keep the prior non-TLS behavior for local or compatible deployments. HTTPS and default endpoints now use TLS rather than being forced down to plaintext.

Test coverage: `internal/pg-backups/pg-backup-drive_test.go` covers HTTP, HTTPS, empty, and malformed endpoint values for the SSL decision helper.

### Test-only formatting updates

`database/identity_test.go`, `internal/pg-conn/pg_conn_test.go`, and `model/transaction_test.go` contain gofmt or whitespace-only cleanup from touching the worktree during the security changes. They do not change runtime behavior.

## Validation

Attempted:

- `go test ./...`
- `GOPATH=/tmp/codex-go go test ./internal/hooks ./internal/pg-backups ./database`
- `GOPATH=/tmp/codex-go go test ./...`
- `GOPATH=/tmp/codex-go GOTOOLCHAIN=local go test ./...`
- `GOPATH=/tmp/codex-go GOTOOLCHAIN=local go version`

Result: blocked in this sandbox. Without `GOPATH`, Go exits because no module cache path is configured. With `GOPATH=/tmp/codex-go`, Go attempts to download `golang.org/toolchain@v0.0.1-go1.25.0.linux-amd64`, but network access is restricted. With `GOTOOLCHAIN=local`, Go reports the installed local toolchain as Go 1.22.2 and exits because `go.mod` requires Go 1.25.0.

Safest next validation step: run `go test ./...` in an environment with Go 1.25.0 available or with a pre-populated module/toolchain cache.

## Remaining Risks and Follow-Ups

- Hook destination validation still allows arbitrary HTTP(S) hosts. Consider a policy layer for private-network blocking, allowlists, or tenant-scoped webhook destination controls if hooks are user-controlled in production.
- Hook response logging can include remote response bodies. Consider truncating or redacting response logs to reduce accidental sensitive-data retention.
- Backup files are created on local disk before S3 upload. Review local backup directory permissions, retention, and cleanup behavior separately.
- `BackupToS3` reads the full backup file into memory before upload. Large database backups could cause memory pressure; streaming upload would be safer operationally.
- Re-run full test coverage once the Go 1.25.0 toolchain is available.
