package datasources

import (
	"github.com/jerry-enebeli/saifu/model"
)

type postgres struct{}

func (p postgres) CreateWallet() (model.Wallet, error) {
	panic("implement me")
}

func (p postgres) GetWalletByIdentifier(identifier model.WalletIdentifier) (model.Wallet, error) {
	panic("implement me")
}

func (p postgres) GetAllWallets(filter model.WalletFilter) ([]model.Wallet, error) {
	panic("implement me")
}

func (p postgres) UpdateWallet(identifier model.WalletIdentifier, wallet model.Wallet) ([]model.Wallet, error) {
	panic("implement me")
}

func (p postgres) DeleteWallet(identifier model.WalletIdentifier) ([]model.Wallet, error) {
	panic("implement me")
}
