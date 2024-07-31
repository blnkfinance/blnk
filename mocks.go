package blnk

import "github.com/jerry-enebeli/blnk/model"

type MockBlnk struct {
	Blnk
	mockGetTransaction func(string) (*model.Transaction, error)
}

func (m *MockBlnk) GetTransaction(id string) (*model.Transaction, error) {
	if m.mockGetTransaction != nil {
		return m.mockGetTransaction(id)
	}
	return m.Blnk.GetTransaction(id)
}
