package main

import (
	"os"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
	"github.com/alpacahq/alpaca-trade-api-go/common"
	"github.com/dakimura/alpacaapitest/alpacaapi"
)

func main() {
	apiKeyID := os.Getenv("API_KEY_ID")
	apiSecretKey := os.Getenv("API_SECRET_KEY")

	symbols, err := alpacaapi.GetSymbols()
	if err != nil {
		panic(err)
	}

	cred := &common.APIKey{
		ID:           apiKeyID,
		PolygonKeyID: apiKeyID,
		Secret:       apiSecretKey,
	}
	client := alpaca.NewClient(cred)
	if err := alpacaapi.Get1DBar(client, symbols); err != nil {
		panic(err)
	}
	if err = alpacaapi.Get1MinBar(client, symbols); err != nil {
		panic(err)
	}
	if err = alpacaapi.GetQuotes(client, symbols); err != nil {
		panic(err)
	}
	if err = alpacaapi.GetTrades(client, symbols); err != nil {
		panic(err)
	}
}
