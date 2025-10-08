package database

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestCreateAPIKey_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	name := "Test API Key"
	ownerID := "owner123"
	scopes := []string{"read", "write"}
	expiresAt := time.Now().Add(24 * time.Hour)

	mock.ExpectExec("INSERT INTO blnk.api_keys").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), name, ownerID, pq.StringArray(scopes), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), false).
		WillReturnResult(sqlmock.NewResult(1, 1))

	apiKey, err := ds.CreateAPIKey(context.Background(), name, ownerID, scopes, expiresAt)
	assert.NoError(t, err)
	assert.NotNil(t, apiKey)
	assert.Equal(t, name, apiKey.Name)
	assert.Equal(t, ownerID, apiKey.OwnerID)
	assert.Equal(t, scopes, apiKey.Scopes)
	assert.NotEmpty(t, apiKey.APIKeyID)
	assert.NotEmpty(t, apiKey.Key)
	assert.False(t, apiKey.IsRevoked)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestCreateAPIKey_DatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	name := "Test API Key"
	ownerID := "owner123"
	scopes := []string{"read", "write"}
	expiresAt := time.Now().Add(24 * time.Hour)

	mock.ExpectExec("INSERT INTO blnk.api_keys").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), name, ownerID, pq.StringArray(scopes), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), false).
		WillReturnError(sql.ErrConnDone)

	apiKey, err := ds.CreateAPIKey(context.Background(), name, ownerID, scopes, expiresAt)
	assert.Error(t, err)
	assert.Nil(t, apiKey)
	assert.Equal(t, sql.ErrConnDone, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAPIKey_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	key := "test-api-key"
	apiKeyID := "api_key_123"
	name := "Test API Key"
	ownerID := "owner123"
	scopes := []string{"read", "write"}
	expiresAt := time.Now().Add(24 * time.Hour)
	createdAt := time.Now().Add(-1 * time.Hour)
	lastUsedAt := time.Now().Add(-30 * time.Minute)

	row := sqlmock.NewRows([]string{"api_key_id", "key", "name", "owner_id", "scopes", "expires_at", "created_at", "last_used_at", "is_revoked", "revoked_at"}).
		AddRow(apiKeyID, key, name, ownerID, pq.StringArray(scopes), expiresAt, createdAt, lastUsedAt, false, nil)

	mock.ExpectQuery("SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at FROM blnk.api_keys WHERE key = \\$1").
		WithArgs(key).
		WillReturnRows(row)

	apiKey, err := ds.GetAPIKey(context.Background(), key)
	assert.NoError(t, err)
	assert.NotNil(t, apiKey)
	assert.Equal(t, apiKeyID, apiKey.APIKeyID)
	assert.Equal(t, key, apiKey.Key)
	assert.Equal(t, name, apiKey.Name)
	assert.Equal(t, ownerID, apiKey.OwnerID)
	assert.Equal(t, scopes, apiKey.Scopes)
	assert.False(t, apiKey.IsRevoked)
	assert.Nil(t, apiKey.RevokedAt)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAPIKey_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	key := "non-existent-key"

	mock.ExpectQuery("SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at FROM blnk.api_keys WHERE key = \\$1").
		WithArgs(key).
		WillReturnError(sql.ErrNoRows)

	apiKey, err := ds.GetAPIKey(context.Background(), key)
	assert.Error(t, err)
	assert.Nil(t, apiKey)
	assert.Equal(t, ErrAPIKeyNotFound, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAPIKey_DatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	key := "test-api-key"

	mock.ExpectQuery("SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at FROM blnk.api_keys WHERE key = \\$1").
		WithArgs(key).
		WillReturnError(sql.ErrConnDone)

	apiKey, err := ds.GetAPIKey(context.Background(), key)
	assert.Error(t, err)
	assert.Nil(t, apiKey)
	assert.Equal(t, sql.ErrConnDone, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestRevokeAPIKey_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	apiKeyID := "api_key_123"
	ownerID := "owner123"

	mock.ExpectExec("UPDATE blnk.api_keys SET is_revoked = true, revoked_at = \\$1 WHERE api_key_id = \\$2 AND owner_id = \\$3").
		WithArgs(sqlmock.AnyArg(), apiKeyID, ownerID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.RevokeAPIKey(context.Background(), apiKeyID, ownerID)
	assert.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestRevokeAPIKey_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	apiKeyID := "non-existent-key"
	ownerID := "owner123"

	mock.ExpectExec("UPDATE blnk.api_keys SET is_revoked = true, revoked_at = \\$1 WHERE api_key_id = \\$2 AND owner_id = \\$3").
		WithArgs(sqlmock.AnyArg(), apiKeyID, ownerID).
		WillReturnResult(sqlmock.NewResult(1, 0))

	err = ds.RevokeAPIKey(context.Background(), apiKeyID, ownerID)
	assert.Error(t, err)
	assert.Equal(t, ErrAPIKeyNotFound, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestRevokeAPIKey_DatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	apiKeyID := "api_key_123"
	ownerID := "owner123"

	mock.ExpectExec("UPDATE blnk.api_keys SET is_revoked = true, revoked_at = \\$1 WHERE api_key_id = \\$2 AND owner_id = \\$3").
		WithArgs(sqlmock.AnyArg(), apiKeyID, ownerID).
		WillReturnError(sql.ErrConnDone)

	err = ds.RevokeAPIKey(context.Background(), apiKeyID, ownerID)
	assert.Error(t, err)
	assert.Equal(t, sql.ErrConnDone, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateLastUsed_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	apiKeyID := "api_key_123"

	mock.ExpectExec("UPDATE blnk.api_keys SET last_used_at = \\$1 WHERE api_key_id = \\$2").
		WithArgs(sqlmock.AnyArg(), apiKeyID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.UpdateLastUsed(context.Background(), apiKeyID)
	assert.NoError(t, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestUpdateLastUsed_DatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	apiKeyID := "api_key_123"

	mock.ExpectExec("UPDATE blnk.api_keys SET last_used_at = \\$1 WHERE api_key_id = \\$2").
		WithArgs(sqlmock.AnyArg(), apiKeyID).
		WillReturnError(sql.ErrConnDone)

	err = ds.UpdateLastUsed(context.Background(), apiKeyID)
	assert.Error(t, err)
	assert.Equal(t, sql.ErrConnDone, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestListAPIKeys_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	ownerID := "owner123"
	expiresAt := time.Now().Add(24 * time.Hour)
	createdAt := time.Now().Add(-1 * time.Hour)
	lastUsedAt := time.Now().Add(-30 * time.Minute)

	rows := sqlmock.NewRows([]string{"api_key_id", "key", "name", "owner_id", "scopes", "expires_at", "created_at", "last_used_at", "is_revoked", "revoked_at"}).
		AddRow("api_key_1", "key1", "API Key 1", ownerID, pq.StringArray([]string{"read"}), expiresAt, createdAt, lastUsedAt, false, nil).
		AddRow("api_key_2", "key2", "API Key 2", ownerID, pq.StringArray([]string{"read", "write"}), expiresAt, createdAt, lastUsedAt, true, &createdAt)

	mock.ExpectQuery("SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at FROM blnk.api_keys WHERE owner_id = \\$1 ORDER BY created_at DESC").
		WithArgs(ownerID).
		WillReturnRows(rows)

	apiKeys, err := ds.ListAPIKeys(context.Background(), ownerID)
	assert.NoError(t, err)
	assert.Len(t, apiKeys, 2)

	// Check first API key
	assert.Equal(t, "api_key_1", apiKeys[0].APIKeyID)
	assert.Equal(t, "key1", apiKeys[0].Key)
	assert.Equal(t, "API Key 1", apiKeys[0].Name)
	assert.Equal(t, ownerID, apiKeys[0].OwnerID)
	assert.Equal(t, []string{"read"}, apiKeys[0].Scopes)
	assert.False(t, apiKeys[0].IsRevoked)
	assert.Nil(t, apiKeys[0].RevokedAt)

	// Check second API key
	assert.Equal(t, "api_key_2", apiKeys[1].APIKeyID)
	assert.Equal(t, "key2", apiKeys[1].Key)
	assert.Equal(t, "API Key 2", apiKeys[1].Name)
	assert.Equal(t, ownerID, apiKeys[1].OwnerID)
	assert.Equal(t, []string{"read", "write"}, apiKeys[1].Scopes)
	assert.True(t, apiKeys[1].IsRevoked)
	assert.NotNil(t, apiKeys[1].RevokedAt)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestListAPIKeys_EmptyResult(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	ownerID := "owner123"

	rows := sqlmock.NewRows([]string{"api_key_id", "key", "name", "owner_id", "scopes", "expires_at", "created_at", "last_used_at", "is_revoked", "revoked_at"})

	mock.ExpectQuery("SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at FROM blnk.api_keys WHERE owner_id = \\$1 ORDER BY created_at DESC").
		WithArgs(ownerID).
		WillReturnRows(rows)

	apiKeys, err := ds.ListAPIKeys(context.Background(), ownerID)
	assert.NoError(t, err)
	assert.Len(t, apiKeys, 0)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestListAPIKeys_DatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	ownerID := "owner123"

	mock.ExpectQuery("SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at FROM blnk.api_keys WHERE owner_id = \\$1 ORDER BY created_at DESC").
		WithArgs(ownerID).
		WillReturnError(sql.ErrConnDone)

	apiKeys, err := ds.ListAPIKeys(context.Background(), ownerID)
	assert.Error(t, err)
	assert.Nil(t, apiKeys)
	assert.Equal(t, sql.ErrConnDone, err)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestListAPIKeys_ScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}

	ownerID := "owner123"

	// Create a row with invalid data type for scopes to trigger scan error
	rows := sqlmock.NewRows([]string{"api_key_id", "key", "name", "owner_id", "scopes", "expires_at", "created_at", "last_used_at", "is_revoked", "revoked_at"}).
		AddRow("api_key_1", "key1", "API Key 1", ownerID, "invalid_scopes_type", time.Now(), time.Now(), time.Now(), false, nil)

	mock.ExpectQuery("SELECT api_key_id, key, name, owner_id, scopes, expires_at, created_at, last_used_at, is_revoked, revoked_at FROM blnk.api_keys WHERE owner_id = \\$1 ORDER BY created_at DESC").
		WithArgs(ownerID).
		WillReturnRows(rows)

	apiKeys, err := ds.ListAPIKeys(context.Background(), ownerID)
	assert.Error(t, err)
	assert.Nil(t, apiKeys)

	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}
