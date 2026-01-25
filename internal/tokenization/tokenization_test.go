package tokenization

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
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

// TestKeySize ensures the service works with the required 32-byte key size (AES-256)
// and rejects other key sizes
func TestKeySize(t *testing.T) {
	testValue := "Test value"

	// Test that 32-byte key works (AES-256 - the only supported size)
	t.Run("32-byte key should work", func(t *testing.T) {
		key := make([]byte, 32)
		_, err := rand.Read(key)
		if err != nil {
			t.Fatalf("Failed to generate random key: %v", err)
		}

		service := NewTokenizationService(key)
		token, err := service.Tokenize(testValue)
		if err != nil {
			t.Errorf("Tokenize with 32-byte key error: %v", err)
			return
		}

		decrypted, err := service.Detokenize(token)
		if err != nil {
			t.Errorf("Detokenize with 32-byte key error: %v", err)
			return
		}

		if decrypted != testValue {
			t.Errorf("Expected %q, got %q with 32-byte key", testValue, decrypted)
		}
	})

	// Test that non-32-byte keys are rejected (tokenization disabled)
	invalidSizes := []int{16, 24} // AES-128 and AES-192 are not supported
	for _, size := range invalidSizes {
		t.Run(fmt.Sprintf("%d-byte key should be rejected", size), func(t *testing.T) {
			key := make([]byte, size)
			_, err := rand.Read(key)
			if err != nil {
				t.Fatalf("Failed to generate random key: %v", err)
			}

			service := NewTokenizationService(key)
			_, err = service.Tokenize(testValue)
			if err == nil {
				t.Errorf("Expected error with %d-byte key, but got none", size)
			}
		})
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

// TestUTF8Tokenization tests tokenization with UTF-8 characters
func TestUTF8Tokenization(t *testing.T) {
	// Setup
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)

	// Test cases with UTF-8 characters mentioned in the bug report
	testData := []string{
		"Taizé",            // é character that caused the original bug
		"José",             // Common Spanish name with accent
		"François",         // French name with ç
		"Müller",           // German name with umlaut ü
		"Björk",            // Scandinavian name with ö
		"Tschüß",           // German word with ü and ß
		"Ångström",         // Swedish name with Å
		"Zoë",              // Name with ë
		"Pañuelos",         // Spanish word with ñ
		"Café",             // Common word with é
		"naïve",            // Word with ï
		"résumé",           // Word with multiple accents
		"José María Aznar", // Full name with spaces and accents
		"北京",               // Chinese characters
		"東京",               // Japanese characters
		"москва",           // Russian characters
		"العربية",          // Arabic characters
		"हिन्दी",           // Hindi characters
		"🔒🔑",               // Emoji (already tested but keeping for UTF-8 completeness)
	}

	for _, original := range testData {
		t.Run(fmt.Sprintf("Standard_%s", original), func(t *testing.T) {
			// Test standard tokenization
			token, err := service.Tokenize(original)
			if err != nil {
				t.Errorf("Tokenize(%q) error: %v", original, err)
				return
			}

			// Verify token is valid base64
			_, err = base64.StdEncoding.DecodeString(token)
			if err != nil {
				t.Errorf("Token %q is not valid base64", token)
				return
			}

			// Test detokenization
			decrypted, err := service.Detokenize(token)
			if err != nil {
				t.Errorf("Detokenize(%q) error: %v", token, err)
				return
			}

			// Verify the result matches exactly
			if decrypted != original {
				t.Errorf("Expected %q, got %q", original, decrypted)
			}
		})

		t.Run(fmt.Sprintf("FormatPreserving_%s", original), func(t *testing.T) {
			// Test format-preserving tokenization
			token, err := service.TokenizeWithMode(original, FormatPreservingMode)
			if err != nil {
				t.Errorf("TokenizeWithMode(%q, FormatPreservingMode) error: %v", original, err)
				return
			}

			// Verify token format
			if !strings.HasPrefix(token, "FPT:") {
				t.Errorf("Format-preserving token %q does not have FPT: prefix", token)
				return
			}

			// Test detokenization
			decrypted, err := service.Detokenize(token)
			if err != nil {
				t.Errorf("Detokenize(%q) error: %v", token, err)
				return
			}

			// Verify the result matches exactly
			if decrypted != original {
				t.Errorf("Expected %q, got %q", original, decrypted)
			}
		})
	}
}

// TestUTF8FormatPreservation tests that format-preserving tokenization
// correctly handles UTF-8 characters while preserving format
func TestUTF8FormatPreservation(t *testing.T) {
	key := []byte("0123456789ABCDEF0123456789ABCDEF") // 32-byte key for AES-256
	service := NewTokenizationService(key)

	testCases := []struct {
		original string
		name     string
	}{
		{"Café", "French_word_with_accent"},
		{"Müller", "German_name_with_umlaut"},
		{"José", "Spanish_name_with_accent"},
		{"Björk", "Scandinavian_name"},
		{"naïve", "Word_with_diaeresis"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			token, err := service.TokenizeWithMode(tc.original, FormatPreservingMode)
			if err != nil {
				t.Fatalf("TokenizeWithMode error: %v", err)
			}

			// Extract the visible part of the format-preserving token
			parts := strings.SplitN(token, ":", 3)
			if len(parts) < 3 {
				t.Fatalf("Invalid format-preserving token format: %q", token)
			}

			visibleToken := parts[1]
			originalRunes := []rune(tc.original)
			tokenRunes := []rune(visibleToken)

			// Check length matches (in runes, not bytes)
			if len(tokenRunes) != len(originalRunes) {
				t.Errorf("Token length mismatch: expected %d runes, got %d runes", len(originalRunes), len(tokenRunes))
			}

			// Check character types are preserved
			for i, origChar := range originalRunes {
				if i >= len(tokenRunes) {
					break
				}

				tokenChar := tokenRunes[i]

				if 'A' <= origChar && origChar <= 'Z' {
					if tokenChar < 'A' || tokenChar > 'Z' {
						t.Errorf("Character type not preserved at position %d: expected uppercase, got %q", i, string(tokenChar))
					}
				} else if 'a' <= origChar && origChar <= 'z' {
					if tokenChar < 'a' || tokenChar > 'z' {
						t.Errorf("Character type not preserved at position %d: expected lowercase, got %q", i, string(tokenChar))
					}
				} else if '0' <= origChar && origChar <= '9' {
					if tokenChar < '0' || tokenChar > '9' {
						t.Errorf("Character type not preserved at position %d: expected digit, got %q", i, string(tokenChar))
					}
				} else {
					// Special characters (including UTF-8) should be preserved exactly
					if tokenChar != origChar {
						t.Errorf("Special character not preserved at position %d: expected %q, got %q", i, string(origChar), string(tokenChar))
					}
				}
			}

			// Verify detokenization works
			decrypted, err := service.Detokenize(token)
			if err != nil {
				t.Errorf("Detokenize error: %v", err)
			}
			if decrypted != tc.original {
				t.Errorf("Expected %q, got %q", tc.original, decrypted)
			}
		})
	}
}

// TestGenerateTokenWithFormat_UTF8 specifically tests the generateTokenWithFormat function with UTF-8
func TestGenerateTokenWithFormat_UTF8(t *testing.T) {
	testCases := []struct {
		original string
		name     string
	}{
		{"Café", "French_accent"},
		{"Müller", "German_umlaut"},
		{"José", "Spanish_accent"},
		{"Björk", "Scandinavian_char"},
		{"Tschüß", "German_special_chars"},
		{"北京", "Chinese_chars"},
		{"москва", "Cyrillic_chars"},
	}

	// Generate a random seed
	seed := make([]byte, 32)
	_, err := rand.Read(seed)
	if err != nil {
		t.Fatalf("Failed to generate random seed: %v", err)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			token, err := generateTokenWithFormat(seed, tc.original)
			if err != nil {
				t.Fatalf("generateTokenWithFormat error: %v", err)
			}

			originalRunes := []rune(tc.original)
			tokenRunes := []rune(token)

			// Check length matches (in runes, not bytes)
			if len(tokenRunes) != len(originalRunes) {
				t.Errorf("Token length %d doesn't match original length %d", len(tokenRunes), len(originalRunes))
			}

			// Check format preservation for each character
			for i, origChar := range originalRunes {
				if i >= len(tokenRunes) {
					break
				}

				tokenChar := tokenRunes[i]

				if 'A' <= origChar && origChar <= 'Z' {
					if tokenChar < 'A' || tokenChar > 'Z' {
						t.Errorf("Expected uppercase at position %d, got %q", i, string(tokenChar))
					}
				} else if 'a' <= origChar && origChar <= 'z' {
					if tokenChar < 'a' || tokenChar > 'z' {
						t.Errorf("Expected lowercase at position %d, got %q", i, string(tokenChar))
					}
				} else if '0' <= origChar && origChar <= '9' {
					if tokenChar < '0' || tokenChar > '9' {
						t.Errorf("Expected digit at position %d, got %q", i, string(tokenChar))
					}
				} else {
					// UTF-8 and special characters should be preserved exactly
					if tokenChar != origChar {
						t.Errorf("Expected UTF-8/special character %q at position %d, got %q", string(origChar), i, string(tokenChar))
					}
				}
			}

			// Verify the token is valid UTF-8
			if !utf8.ValidString(token) {
				t.Errorf("Generated token is not valid UTF-8: %q", token)
			}
		})
	}
}
