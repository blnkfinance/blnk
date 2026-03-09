# Contributing to Blnk

Thanks for contributing to Blnk. This guide explains how to get changes merged quickly and safely.

## Code of Conduct

By participating in this project, you agree to follow the [Code of Conduct](./CODE_OF_CONDUCT.md).

## Ways to Contribute

- Report bugs and request features through GitHub issues.
- Improve documentation and examples.
- Fix bugs, add tests, and ship new features.

## Development Prerequisites

- Go `1.25.0` or newer (this repo requires Go `1.25.0` in `go.mod`)
- Docker and Docker Compose
- PostgreSQL and Redis (you can run both via Docker Compose)

## Local Setup

1. Clone the repository:

```bash
git clone https://github.com/blnkfinance/blnk.git
cd blnk
```

2. Install dependencies:

```bash
go mod tidy
```

3. Build the binary:

```bash
go build -o blnk ./cmd/*.go
```

4. Start required services:

```bash
docker compose up -d postgres redis
```

5. Run migrations:

```bash
./blnk migrate up
```

## Running Tests

- Fast local test run:

```bash
go test -short ./...
```

- Full test run:

```bash
go test ./...
```

You can also use:

```bash
make test
```

## Coding Guidelines

- Keep changes focused and minimal.
- Write or update tests for behavior changes.
- Run `gofmt` on changed Go files.
- Avoid unrelated refactors in feature or bug-fix PRs.
- Keep public API changes backward-compatible unless clearly intentional.

## Database Changes

When your change affects schema or persistence behavior:

- Add a migration in `sql/`.
- Follow the existing migration naming pattern.
- Include tests that validate the new behavior.

## Pull Request Checklist

Before opening a PR, ensure:

- The branch is rebased on the latest `main`.
- `go test ./...` passes locally.
- New behavior is covered by tests.
- API changes are reflected in docs/examples when relevant.
- PR description explains:
  - what changed
  - why it changed
  - how it was tested

## Submitting a PR

1. Fork the repository and create a feature branch.
2. Commit your changes with clear commit messages.
3. Push your branch and open a PR against `main`.
4. Respond to review comments and update the PR until it is approved.

## Reporting Security Issues

Please do not open public issues for security vulnerabilities. Contact the blnk tean privately first.
