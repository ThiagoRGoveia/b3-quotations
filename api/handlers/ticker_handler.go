package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/thiago-r-goveia/b3-quotations/api/handlers/db"
)

type TickerHandler struct {
	DBManager db.TickerDBManager
}

func NewTickerHandler(dbManager db.TickerDBManager) *TickerHandler {
	return &TickerHandler{DBManager: dbManager}
}

func (h *TickerHandler) GetTickerInfo(w http.ResponseWriter, r *http.Request) {
	ticker := strings.TrimPrefix(r.URL.Path, "/tickers/")
	if ticker == "" {
		http.Error(w, "Ticker is required in the URL path /tickers/{ticker}", http.StatusBadRequest)
		return
	}

	var startDate time.Time
	startDateStr := r.URL.Query().Get("data_inicio")

	if startDateStr == "" {
		startDate = getPastBusinessDay(7)
	} else {
		var err error
		startDate, err = time.Parse("2006-01-02", startDateStr)
		if err != nil {
			http.Error(w, "Invalid 'data_inicio' format. Use YYYY-MM-DD.", http.StatusBadRequest)
			return
		}
	}

	ctx := r.Context()
	tickerInfo, err := h.DBManager.GetTickerInfo(ctx, ticker, startDate)
	if err != nil {
		http.Error(w, "Failed to retrieve ticker information", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(tickerInfo); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// getPastBusinessDay calculates the date n business days in the past.
func getPastBusinessDay(days int) time.Time {
	date := time.Now()
	for businessDays := 0; businessDays < days; {
		date = date.AddDate(0, 0, -1)
		if date.Weekday() != time.Saturday && date.Weekday() != time.Sunday {
			businessDays++
		}
	}
	return date
}
