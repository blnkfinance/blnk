package api

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
)

func TestCreateIdentity(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	tests := []struct {
		name         string
		payload      model.Identity
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Valid Identity",
			payload: model.Identity{
				FirstName:        gofakeit.FirstName(),
				LastName:         gofakeit.LastName(),
				EmailAddress:     gofakeit.Email(),
				OrganizationName: gofakeit.Company(),
				Category:         "individual",
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Valid Identity with metadata",
			payload: model.Identity{
				FirstName:        gofakeit.FirstName(),
				LastName:         gofakeit.LastName(),
				EmailAddress:     gofakeit.Email(),
				OrganizationName: gofakeit.Company(),
				Category:         "individual",
				MetaData:         map[string]interface{}{"key": "value"},
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.Identity
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/identities",
				Auth:     "",
				Router:   router,
			}

			resp, err := SetUpTestRequest(testRequest)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetUpTestRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedCode, resp.Code)

			if tt.expectedCode == http.StatusCreated {
				assert.NotEmpty(t, response.IdentityID)
			}
		})
	}
}

func TestGetIdentity(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newIdentity, err := b.CreateIdentity(model.Identity{
		FirstName:        gofakeit.FirstName(),
		LastName:         gofakeit.LastName(),
		EmailAddress:     gofakeit.Email(),
		OrganizationName: gofakeit.Company(),
		Category:         "individual",
	})
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	t.Run("Valid identity ID", func(t *testing.T) {
		var response model.Identity
		testRequest := TestRequest{
			Payload:  nil,
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/identities/%s", newIdentity.IdentityID),
			Auth:     "",
			Router:   router,
		}

		resp, err := SetUpTestRequest(testRequest)
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Equal(t, newIdentity.IdentityID, response.IdentityID)
	})

	t.Run("Invalid identity ID", func(t *testing.T) {
		var response map[string]interface{}
		testRequest := TestRequest{
			Payload:  nil,
			Response: &response,
			Method:   "GET",
			Route:    "/identities/invalid-id",
			Auth:     "",
			Router:   router,
		}

		resp, _ := SetUpTestRequest(testRequest)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}

func TestUpdateIdentity(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newIdentity, err := b.CreateIdentity(model.Identity{
		FirstName:        gofakeit.FirstName(),
		LastName:         gofakeit.LastName(),
		EmailAddress:     gofakeit.Email(),
		OrganizationName: gofakeit.Company(),
		Category:         "individual",
	})
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	t.Run("Valid update", func(t *testing.T) {
		updatedIdentity := model.Identity{
			FirstName: "UpdatedFirstName",
			LastName:  "UpdatedLastName",
		}
		payloadBytes, _ := request.ToJsonReq(&updatedIdentity)

		var response map[string]interface{}
		testRequest := TestRequest{
			Payload:  payloadBytes,
			Response: &response,
			Method:   "PUT",
			Route:    fmt.Sprintf("/identities/%s", newIdentity.IdentityID),
			Auth:     "",
			Router:   router,
		}

		resp, err := SetUpTestRequest(testRequest)
		if err != nil {
			t.Error(err)
			return
		}
		assert.Equal(t, http.StatusOK, resp.Code)
	})
}

func TestGetAllIdentities(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	_, err = b.CreateIdentity(model.Identity{
		FirstName:        gofakeit.FirstName(),
		LastName:         gofakeit.LastName(),
		EmailAddress:     gofakeit.Email(),
		OrganizationName: gofakeit.Company(),
		Category:         "individual",
	})
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	var response []model.Identity
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    "/identities",
		Auth:     "",
		Router:   router,
	}

	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.True(t, len(response) >= 1)
}
