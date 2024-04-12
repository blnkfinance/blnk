package blnk

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jerry-enebeli/blnk/model"
)

func parseASL(asl string) (model.Transaction, error) {
	var transaction model.Transaction

	// Validate the ASL format before parsing
	if err := validateASLFormat(asl); err != nil {
		return model.Transaction{}, err
	}

	lines := strings.Split(asl, "\n")

	for i, line := range lines {
		if skipLine(line) {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 2 {
			return model.Transaction{}, fmt.Errorf("line %d is too short, missing keyword or value", i+1)
		}

		keyword, values := parts[0], parts[1:]
		if err := processLine(keyword, values, &transaction); err != nil {
			return model.Transaction{}, fmt.Errorf("error processing line %d: %w", i+1, err)
		}
	}

	return transaction, nil
}

func validateASLFormat(asl string) error {
	keywords := []string{"AMOUNT", "REFERENCE", "CURRENCY", "SOURCE", "DESTINATION", "ALLOW_OVERDRAFT"}
	for _, keyword := range keywords {
		pattern := " " + keyword
		if strings.Contains(asl, pattern) {
			return fmt.Errorf("format error: '%s' must begin on a new line", keyword)
		}
	}
	return nil
}

func skipLine(line string) bool {
	return strings.HasPrefix(line, "#") || strings.TrimSpace(line) == ""
}

func processLine(keyword string, values []string, transaction *model.Transaction) error {
	switch keyword {
	case "AMOUNT":
		if err := parseAmount(values[0], transaction); err != nil {
			return err
		}
	case "REFERENCE":
		transaction.Reference = strings.Join(values, " ")
	case "CURRENCY":
		if err := parseCurrency(values[0], transaction); err != nil {
			return err
		}
	case "SOURCE", "DESTINATION":
		if err := parseDistribution(keyword, values, transaction); err != nil {
			return err
		}
	case "ALLOW_OVERDRAFT":
		if err := parseAllowOverdraft(values[0], transaction); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown keyword: %s", keyword)
	}
	return nil
}

func parseAmount(value string, transaction *model.Transaction) error {
	amount, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fmt.Errorf("invalid amount: %v", err)
	}
	transaction.Amount = amount
	return nil
}

func parseCurrency(value string, transaction *model.Transaction) error {
	// Add validation for currency if necessary, e.g., check if it's a valid ISO currency code.
	transaction.Currency = value
	return nil
}

func parseDistribution(keyword string, values []string, transaction *model.Transaction) error {
	if len(values) < 2 {
		return fmt.Errorf("%s requires an identifier and distribution", strings.ToLower(keyword))
	}
	distribution := model.Distribution{Identifier: values[0], Distribution: values[1]}
	if keyword == "SOURCE" {
		transaction.Sources = append(transaction.Sources, distribution)
	} else {
		transaction.Destinations = append(transaction.Destinations, distribution)
	}
	return nil
}

func parseAllowOverdraft(value string, transaction *model.Transaction) error {
	allowOverdraft, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("invalid value for ALLOW_OVERDRAFT: %v", err)
	}
	transaction.AllowOverdraft = allowOverdraft
	return nil
}
