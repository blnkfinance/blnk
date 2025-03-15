package tokenization

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
)

func TestNewTokenizationService(t *testing.T) {
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)

	if service == nil {
		t.Fatal("NewTokenizationService returned nil")
	}

	if !bytes.Equal(service.key, key) {
		t.Errorf("Expected key %v, got %v", key, service.key)
	}
}

func TestTokenizeDetokenize_StandardMode(t *testing.T) {
	// Setup
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)
	testData := []string{
		"John Doe",
		"john.doe@example.com",
		"+1-555-123-4567",
		"123 Main St, Anytown, USA",
		"12345",
		"",   // Test empty string
		"🔒🔑", // Test Unicode characters
	}

	for _, original := range testData {
		// Test standard tokenization
		token, err := service.Tokenize(original)
		if err != nil {
			t.Errorf("Tokenize(%q) error: %v", original, err)
			continue
		}

		// Verify token format
		_, err = base64.StdEncoding.DecodeString(token)
		if err != nil {
			t.Errorf("Token %q is not valid base64", token)
		}

		// Test detokenization
		decrypted, err := service.Detokenize(token)
		if err != nil {
			t.Errorf("Detokenize(%q) error: %v", token, err)
			continue
		}

		// Verify the result
		if decrypted != original {
			t.Errorf("Expected %q, got %q", original, decrypted)
		}
	}
}

func TestTokenizeDetokenize_FormatPreservingMode(t *testing.T) {
	// Setup
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)
	testData := []string{
		"John Doe",
		"john.doe@example.com",
		"+1-555-123-4567",
		"123 Main St, Anytown, USA",
		"12345",
	}

	for _, original := range testData {
		// Test format-preserving tokenization
		token, err := service.TokenizeWithMode(original, FormatPreservingMode)
		if err != nil {
			t.Errorf("TokenizeWithMode(%q, FormatPreservingMode) error: %v", original, err)
			continue
		}

		// Verify token format
		if !strings.HasPrefix(token, "FPT:") {
			t.Errorf("Format-preserving token %q does not have FPT: prefix", token)
		}

		// Test format preservation
		parts := strings.SplitN(token, ":", 3)
		if len(parts) < 3 {
			t.Errorf("Invalid format-preserving token format: %q", token)
			continue
		}

		visibleToken := parts[1]
		if len(visibleToken) != len(original) {
			t.Errorf("Format-preserving token length mismatch: expected %d, got %d", len(original), len(visibleToken))
		}

		// Check character types are preserved
		for i, char := range original {
			// Skip if the visible token is shorter than expected
			if i >= len(visibleToken) {
				break
			}

			tokenChar := rune(visibleToken[i])

			// For format preserving tokenization, we're only checking that:
			// 1. Uppercase letters remain uppercase
			// 2. Lowercase letters remain lowercase
			// 3. Digits remain digits
			// 4. Special characters remain exactly the same

			if 'A' <= char && char <= 'Z' {
				if tokenChar < 'A' || tokenChar > 'Z' {
					t.Errorf("Character type not preserved at position %d: expected uppercase, got %q", i, string(tokenChar))
				}
			} else if 'a' <= char && char <= 'z' {
				if tokenChar < 'a' || tokenChar > 'z' {
					t.Errorf("Character type not preserved at position %d: expected lowercase, got %q", i, string(tokenChar))
				}
			} else if '0' <= char && char <= '9' {
				if tokenChar < '0' || tokenChar > '9' {
					t.Errorf("Character type not preserved at position %d: expected digit, got %q", i, string(tokenChar))
				}
			} else if tokenChar != char {
				t.Errorf("Special character not preserved at position %d: expected %q, got %q", i, string(char), string(tokenChar))
			}
		}

		// Test detokenization
		decrypted, err := service.Detokenize(token)
		if err != nil {
			t.Errorf("Detokenize(%q) error: %v", token, err)
			continue
		}

		// Verify the result
		if decrypted != original {
			t.Errorf("Expected %q, got %q", original, decrypted)
		}
	}
}

func TestDetokenize_AutoDetection(t *testing.T) {
	// Setup
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)
	original := "John Doe"

	// Create both types of tokens
	standardToken, err := service.TokenizeWithMode(original, StandardMode)
	if err != nil {
		t.Fatalf("TokenizeWithMode(%q, StandardMode) error: %v", original, err)
	}

	formatPreservingToken, err := service.TokenizeWithMode(original, FormatPreservingMode)
	if err != nil {
		t.Fatalf("TokenizeWithMode(%q, FormatPreservingMode) error: %v", original, err)
	}

	// Test auto-detection for standard token
	decrypted, err := service.Detokenize(standardToken)
	if err != nil {
		t.Errorf("Detokenize(%q) error: %v", standardToken, err)
	}
	if decrypted != original {
		t.Errorf("Expected %q, got %q", original, decrypted)
	}

	// Test auto-detection for format-preserving token
	decrypted, err = service.Detokenize(formatPreservingToken)
	if err != nil {
		t.Errorf("Detokenize(%q) error: %v", formatPreservingToken, err)
	}
	if decrypted != original {
		t.Errorf("Expected %q, got %q", original, decrypted)
	}
}

func TestDetokenizeWithMode(t *testing.T) {
	// Setup
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)
	original := "TestDetokenizeWithMode"

	// Create a token in standard mode
	standardToken, err := service.TokenizeWithMode(original, StandardMode)
	if err != nil {
		t.Fatalf("TokenizeWithMode(%q, StandardMode) error: %v", original, err)
	}

	// Create a token in format-preserving mode
	formatPreservingToken, err := service.TokenizeWithMode(original, FormatPreservingMode)
	if err != nil {
		t.Fatalf("TokenizeWithMode(%q, FormatPreservingMode) error: %v", original, err)
	}

	// Test DetokenizeWithMode with StandardMode
	decrypted, err := service.DetokenizeWithMode(standardToken, StandardMode)
	if err != nil {
		t.Errorf("DetokenizeWithMode(%q, StandardMode) error: %v", standardToken, err)
	}
	if decrypted != original {
		t.Errorf("Expected %q, got %q", original, decrypted)
	}

	// Test DetokenizeWithMode with FormatPreservingMode
	decrypted, err = service.DetokenizeWithMode(formatPreservingToken, FormatPreservingMode)
	if err != nil {
		t.Errorf("DetokenizeWithMode(%q, FormatPreservingMode) error: %v", formatPreservingToken, err)
	}
	if decrypted != original {
		t.Errorf("Expected %q, got %q", original, decrypted)
	}

	// Test with incorrect mode (should fail)
	_, err = service.DetokenizeWithMode(standardToken, FormatPreservingMode)
	if err == nil {
		t.Error("Expected error when using standard token with FormatPreservingMode, got nil")
	}

	_, err = service.DetokenizeWithMode(formatPreservingToken, StandardMode)
	if err == nil {
		t.Error("Expected error when using format-preserving token with StandardMode, got nil")
	}
}

func TestDetokenize_InvalidToken(t *testing.T) {
	// Setup
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)

	// Test cases for invalid tokens
	testCases := []struct {
		name  string
		token string
	}{
		{"InvalidBase64", "not-base64!"},
		{"EmptyToken", ""},
		{"TooShort", "dG9vc2hvcnQ="}, // "tooshort" in base64
		{"InvalidFPTFormat", "FPT:only-one-part"},
		{"CorruptedToken", "FPT:visible:not-base64!"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.Detokenize(tc.token)
			if err == nil {
				t.Errorf("Expected error for invalid token %q, got nil", tc.token)
			}
		})
	}
}

func TestGenerateTokenWithFormat(t *testing.T) {
	testCases := []struct {
		original string
	}{
		{"John Doe"},
		{"UPPERCASE"},
		{"lowercase"},
		{"12345"},
		{"Mixed123Case"},
		{"Special!@#$%^&*()"},
	}

	// Generate a random seed
	seed := make([]byte, 32)
	_, err := rand.Read(seed)
	if err != nil {
		t.Fatalf("Failed to generate random seed: %v", err)
	}

	for _, tc := range testCases {
		t.Run(tc.original, func(t *testing.T) {
			token, err := generateTokenWithFormat(seed, tc.original)
			if err != nil {
				t.Fatalf("generateTokenWithFormat error: %v", err)
			}

			// Check length
			if len(token) != len(tc.original) {
				t.Errorf("Token length %d doesn't match original length %d", len(token), len(tc.original))
			}

			// Check format preservation
			for i, origChar := range tc.original {
				tokenChar := rune(token[i])

				if 'A' <= origChar && origChar <= 'Z' {
					// Uppercase letter should remain uppercase
					if tokenChar < 'A' || tokenChar > 'Z' {
						t.Errorf("Expected uppercase at position %d, got %q", i, string(tokenChar))
					}
				} else if 'a' <= origChar && origChar <= 'z' {
					// Lowercase letter should remain lowercase
					if tokenChar < 'a' || tokenChar > 'z' {
						t.Errorf("Expected lowercase at position %d, got %q", i, string(tokenChar))
					}
				} else if '0' <= origChar && origChar <= '9' {
					// Digit should remain digit
					if tokenChar < '0' || tokenChar > '9' {
						t.Errorf("Expected digit at position %d, got %q", i, string(tokenChar))
					}
				} else {
					// Special character should be preserved exactly
					if tokenChar != origChar {
						t.Errorf("Expected special character %q at position %d, got %q", string(origChar), i, string(tokenChar))
					}
				}
			}
		})
	}
}

func TestTokenizableFields(t *testing.T) {
	// Verify the tokenizable fields are as expected
	expectedFields := []string{
		"FirstName",
		"LastName",
		"OtherNames",
		"EmailAddress",
		"PhoneNumber",
		"Street",
		"PostCode",
	}

	if len(TokenizableFields) != len(expectedFields) {
		t.Errorf("Expected %d tokenizable fields, got %d", len(expectedFields), len(TokenizableFields))
	}

	for i, field := range expectedFields {
		if i >= len(TokenizableFields) {
			t.Errorf("Missing expected tokenizable field: %s", field)
			continue
		}
		if TokenizableFields[i] != field {
			t.Errorf("Expected field %q at position %d, got %q", field, i, TokenizableFields[i])
		}
	}
}

// TestKeySize ensures the service works with different key sizes
func TestKeySize(t *testing.T) {
	testKeySizes := []int{16, 24, 32} // AES-128, AES-192, AES-256
	testValue := "Test value"

	for _, size := range testKeySizes {
		key := make([]byte, size)
		_, err := rand.Read(key)
		if err != nil {
			t.Fatalf("Failed to generate random key: %v", err)
		}

		service := NewTokenizationService(key)
		token, err := service.Tokenize(testValue)
		if err != nil {
			t.Errorf("Tokenize with %d-byte key error: %v", size, err)
			continue
		}

		decrypted, err := service.Detokenize(token)
		if err != nil {
			t.Errorf("Detokenize with %d-byte key error: %v", size, err)
			continue
		}

		if decrypted != testValue {
			t.Errorf("Expected %q, got %q with %d-byte key", testValue, decrypted, size)
		}
	}
}

// TestInvalidKeySize ensures appropriate error handling for invalid key sizes
func TestInvalidKeySize(t *testing.T) {
	invalidSizes := []int{1, 8, 10, 20, 30, 64}
	testValue := "Test value"

	for _, size := range invalidSizes {
		key := make([]byte, size)
		_, err := rand.Read(key)
		if err != nil {
			t.Fatalf("Failed to generate random key: %v", err)
		}

		service := NewTokenizationService(key)
		_, err = service.Tokenize(testValue)

		// AES only accepts key sizes of 16, 24, or 32 bytes
		if size == 16 || size == 24 || size == 32 {
			if err != nil {
				t.Errorf("Expected success with %d-byte key, got error: %v", size, err)
			}
		} else if err == nil {
			t.Errorf("Expected error with invalid %d-byte key, got nil", size)
		}
	}
}
