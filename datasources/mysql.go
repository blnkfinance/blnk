package datasources

import (
	"github.com/jerry-enebeli/saifu/model"
)

type mysql struct{}

func (m mysql) CreateWallet() (model.Wallet, error) {
	panic("implement me")
}

func (m mysql) GetWalletByIdentifier(identifier model.WalletIdentifier) (model.Wallet, error) {
	panic("implement me")
}

func (m mysql) GetAllWallets(filter model.WalletFilter) ([]model.Wallet, error) {
	panic("implement me")
}

func (m mysql) UpdateWallet(identifier model.WalletIdentifier, wallet model.Wallet) ([]model.Wallet, error) {
	panic("implement me")
}

func (m mysql) DeleteWallet(identifier model.WalletIdentifier) ([]model.Wallet, error) {
	panic("implement me")
}
