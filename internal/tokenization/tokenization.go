package tokenization

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
)

// TokenizationMode defines the type of tokenization to use
type TokenizationMode int

const (
	// StandardMode uses regular AES-GCM encryption with base64 encoding
	StandardMode TokenizationMode = iota

	// FormatPreservingMode maintains the format of the original data
	FormatPreservingMode
)

// TokenizableFields defines which fields can be tokenized in the Identity model
var TokenizableFields = []string{
	"FirstName",
	"LastName",
	"OtherNames",
	"EmailAddress",
	"PhoneNumber",
	"Street",
	"PostCode",
}

// TokenizationService handles converting PII to tokens and back
type TokenizationService struct {
	key []byte // Encryption key for tokenization
}

// NewTokenizationService creates a new tokenization service.
//
// Parameters:
// - encryptionKey []byte: The encryption key used for tokenization.
//
// Returns:
// - *TokenizationService: A new instance of TokenizationService.
func NewTokenizationService(encryptionKey []byte) *TokenizationService {
	return &TokenizationService{
		key: encryptionKey,
	}
}

// Tokenize converts a PII value to a token using the default standard mode.
//
// Parameters:
// - value string: The original PII value to be tokenized.
//
// Returns:
// - string: The tokenized value.
// - error: An error if tokenization fails.
func (s *TokenizationService) Tokenize(value string) (string, error) {
	return s.TokenizeWithMode(value, StandardMode)
}

// TokenizeWithMode converts a PII value to a token using the specified mode.
//
// Parameters:
// - value string: The original PII value to be tokenized.
// - mode TokenizationMode: The tokenization mode to use.
//
// Returns:
// - string: The tokenized value.
// - error: An error if tokenization fails.
func (s *TokenizationService) TokenizeWithMode(value string, mode TokenizationMode) (string, error) {
	if mode == FormatPreservingMode {
		return s.formatPreservingTokenize(value)
	}

	// Standard tokenization using AES encryption
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return "", err
	}

	// Create GCM
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Create nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	// Encrypt
	ciphertext := gcm.Seal(nonce, nonce, []byte(value), nil)

	// Return base64 encoded token
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Detokenize converts a token back to the original PII value using automatic mode detection.
//
// Parameters:
// - token string: The token to be converted back to PII.
//
// Returns:
// - string: The original PII value.
// - error: An error if detokenization fails.
func (s *TokenizationService) Detokenize(token string) (string, error) {
	// Auto-detect the token type based on prefix
	if strings.HasPrefix(token, "FPT:") {
		// Format-preserving token
		return s.formatPreservingDetokenize(token)
	}

	// Standard token
	return s.standardDetokenize(token)
}

// DetokenizeWithMode converts a token back to the original PII value using the specified mode.
//
// Parameters:
// - token string: The token to be converted back to PII.
// - mode TokenizationMode: The tokenization mode to use.
//
// Returns:
// - string: The original PII value.
// - error: An error if detokenization fails.
func (s *TokenizationService) DetokenizeWithMode(token string, mode TokenizationMode) (string, error) {
	if mode == FormatPreservingMode {
		return s.formatPreservingDetokenize(token)
	}

	return s.standardDetokenize(token)
}

// standardDetokenize handles detokenization of standard tokens
func (s *TokenizationService) standardDetokenize(token string) (string, error) {
	// Decode the base64 token
	data, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}

	// Create cipher block
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return "", err
	}

	// Create GCM
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	// Extract nonce and ciphertext
	nonceSize := gcm.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("token too short")
	}

	nonce, ciphertext := data[:nonceSize], data[nonceSize:]

	// Decrypt
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// formatPreservingTokenize tokenizes a value while preserving its format.
// It uses a deterministic approach to generate a token with the same format.
//
// Parameters:
// - value string: The original value to tokenize.
//
// Returns:
// - string: The tokenized value with the same format as the original.
// - error: An error if tokenization fails.
func (s *TokenizationService) formatPreservingTokenize(value string) (string, error) {
	// 1. Create a deterministic seed using HMAC of the value
	h := hmac.New(sha256.New, s.key)
	h.Write([]byte(value))
	seed := h.Sum(nil)

	// 2. Use the seed to generate a format-preserving token
	visibleToken, err := generateTokenWithFormat(seed, value)
	if err != nil {
		return "", err
	}

	// 3. Encrypt the original value using standard encryption
	standardToken, err := s.Tokenize(value)
	if err != nil {
		return "", err
	}

	// 4. Create the final token by combining the format-preserving token and its identifier
	// We'll prefix format-preserving tokens with FPT: to identify them
	finalToken := fmt.Sprintf("FPT:%s:%s", visibleToken, standardToken)

	return finalToken, nil
}

// formatPreservingDetokenize detokenizes a format-preserving token.
//
// Parameters:
// - token string: The token to detokenize.
//
// Returns:
// - string: The original value.
// - error: An error if detokenization fails.
func (s *TokenizationService) formatPreservingDetokenize(token string) (string, error) {
	// Check if this is a format-preserving token
	if !strings.HasPrefix(token, "FPT:") {
		return "", fmt.Errorf("not a format-preserving token")
	}

	// Split the token
	parts := strings.SplitN(token, ":", 3)
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid format-preserving token format")
	}

	// Extract the standard token part
	standardToken := parts[2]

	// Use standard detokenization to get the original value
	return s.standardDetokenize(standardToken)
}

// generateTokenWithFormat creates a token that matches the format of the original value.
func generateTokenWithFormat(seed []byte, originalValue string) (string, error) {
	runes := []rune(originalValue)
	result := make([]rune, len(runes))

	// Use the seed to generate random runes that preserve the format
	for i, char := range runes {
		seedByte := seed[i%len(seed)]

		if 'A' <= char && char <= 'Z' {
			// Uppercase letter
			result[i] = 'A' + rune(seedByte%26)
		} else if 'a' <= char && char <= 'z' {
			// Lowercase letter
			result[i] = 'a' + rune(seedByte%26)
		} else if '0' <= char && char <= '9' {
			// Digit
			result[i] = '0' + rune(seedByte%10)
		} else {
			// Preserve special characters (including UTF-8 characters)
			result[i] = char
		}
	}

	return string(result), nil
}
