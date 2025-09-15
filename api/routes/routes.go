package routes

import (
	"net/http"

	"github.com/thiago-r-goveia/b3-quotations/api/handlers/handlers"
)

func SetupRoutes(tickerHandler *handlers.TickerHandler) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/tickers/", tickerHandler.GetTickerInfo)

	return mux
}
