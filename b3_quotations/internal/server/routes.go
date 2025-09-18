package server

import (
	"net/http"
)

func SetupRoutes(tickerHandler *TickerService) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/tickers/", tickerHandler.GetTickerInfo)

	return mux
}
