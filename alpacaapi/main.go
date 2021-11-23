package alpacaapi

import (
	"encoding/csv"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"time"

	v2 "github.com/alpacahq/alpaca-trade-api-go/v2"

	"gopkg.in/xmlpath.v2"

	"github.com/alpacahq/alpaca-trade-api-go/alpaca"
)

// GetSymbols scrapes list of symbols of S&P 500 companies
// from https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
func GetSymbols() ([]string, error) {
	const wikipediaSP500Page = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
	resp, err := http.Get(wikipediaSP500Page)
	if err != nil {
		return nil, fmt.Errorf("get 'List of S&P 500 companies' page from wikipedia: %w", err)
	}
	defer resp.Body.Close()

	path := xmlpath.MustCompile(`//*[@id="constituents"]/tbody/tr[*]/td[1]/a`)

	root, err := xmlpath.ParseHTML(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse html: %w", err)
	}

	iter := path.Iter(root)
	var sp500companies []string
	for iter.Next() {
		n := iter.Node()
		sp500companies = append(sp500companies, n.String())
	}
	return sp500companies, nil
}

func writeCSV(filepath string, bars []alpaca.Bar) error {
	f, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("create CSV file %s: %w", filepath, err)
	}

	w := csv.NewWriter(f)

	w.Comma = ','
	//w.UseCRLF = true // set change-line code to CRLF(\r\n)

	for _, bar := range bars {
		record := []string{strconv.FormatInt(bar.Time, 10),
			strconv.FormatFloat(float64(bar.Open), 'f', -1, 32),
			strconv.FormatFloat(float64(bar.High), 'f', -1, 32),
			strconv.FormatFloat(float64(bar.Low), 'f', -1, 32),
			strconv.FormatFloat(float64(bar.Close), 'f', -1, 32),
			strconv.FormatInt(int64(bar.Volume), 10),
		}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("write data to %s: %w", filepath, err)
		}
	}

	w.Flush() // flush and write all the buffer

	if err := w.Error(); err != nil {
		return fmt.Errorf("write/flush data to %s: %w", filepath, err)
	}
	return nil
}

func Get1MinBar(client *alpaca.Client, symbols []string) error {
	const maxSymbolsPerRequest = 100
	limit := 1000
	dayStartdt := time.Date(2021, 11, 17, 0, 0, 0, 0, time.UTC).
		Add(-24 * time.Hour)
	dayEnddt := time.Date(2021, 11, 17, 0, 0, 0, 0, time.UTC)

	for idx := range IndexChunks(len(symbols), maxSymbolsPerRequest) {
		// 1Min bars
		fmt.Printf("ListBars 1Min: symbols=%v, startTime=%v, endTime=%v, limit=%d\n",
			symbols[idx.From:idx.To], dayStartdt, dayEnddt, limit)
		symbolBarMinMap, err := client.ListBars(symbols[idx.From:idx.To], alpaca.ListBarParams{
			Timeframe: "1Min",
			StartDt:   &dayStartdt,
			EndDt:     &dayEnddt,
			Limit:     &limit,
		})
		if err != nil {
			return fmt.Errorf("LisBars API call: %w", err)
		}

		for symbol, bars := range symbolBarMinMap {
			err := writeCSV(fmt.Sprintf("data_1Min/%s_1Min.csv", symbol), bars)
			if err != nil {
				return err
			}
		}

		// to avoid rate-limit
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func Get1DBar(client *alpaca.Client, symbols []string) error {
	const maxSymbolsPerRequest = 100
	limit := 1000
	startdt := time.Now().Add(-24 * time.Hour * 365) // 1year
	enddt := time.Now()

	for idx := range IndexChunks(len(symbols), maxSymbolsPerRequest) {
		// 1D bars
		fmt.Printf("ListBars 1D: symbols=%v, startTime=%v, endTime=%v, limit=%d\n",
			symbols[idx.From:idx.To], startdt, enddt, limit)
		symbolBarDayMap, err := client.ListBars(symbols[idx.From:idx.To], alpaca.ListBarParams{
			Timeframe: "1D",
			StartDt:   &startdt,
			EndDt:     &enddt,
			Limit:     &limit,
		})
		if err != nil {
			return fmt.Errorf("ListBars API call: %w", err)
		}

		for symbol, bars := range symbolBarDayMap {
			// fmt.Printf("%s: num of Bars: %d \n", symbol, len(barSet))
			// fmt.Printf("%s: Bars: %v \n", symbol, barSet)
			err := writeCSV(fmt.Sprintf("data_1D/%s_1D.csv", symbol), bars)
			if err != nil {
				return err
			}
		}

		// to avoid rate limit
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

func GetQuotes(client *alpaca.Client, symbols []string) error {
	const maxQuotesPerRequest = 10000


	for _, symbol := range symbols {
		pageStartTimestamp := time.Date(2021, 11, 17, 0, 0, 0, 0, time.UTC)
		endTimestamp := time.Unix(pageStartTimestamp.Unix(), int64(pageStartTimestamp.Nanosecond())).Add(24 * time.Hour)

		quotesChunk := make(map[int64]v2.Quote, 0)
		for {
			fmt.Printf("GetQuotes: symbol=%s, startTime=%v, endTime=%v\n", symbol, pageStartTimestamp, pageStartTimestamp)
			quotesChan := client.GetQuotes(symbol, pageStartTimestamp, pageStartTimestamp.Add(24*time.Hour), maxQuotesPerRequest)
			for quote := range quotesChan {
				if quote.Error != nil {
					return fmt.Errorf("GetQuoteAPI call: %w", quote.Error)
				}

				pageStartTimestamp = quote.Quote.Timestamp.Add(1 * time.Nanosecond)

				if pageStartTimestamp.After(endTimestamp) {
					break
				}

				quotesChunk[quote.Quote.Timestamp.UnixNano()] = quote.Quote
			}
			if pageStartTimestamp.After(endTimestamp) {
				break
			}
		}
		err := writeCSVQuote(fmt.Sprintf("data_Quote/%s.csv", symbol), quotesChunk)
		if err != nil {
			return err
		}

		// to avoid rate limit
		time.Sleep(500 * time.Millisecond)
	}
	return nil
}

// quotes: key=unix nano second
func writeCSVQuote(filepath string, quotes map[int64]v2.Quote) error {
	f, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("create CSV file %s: %w", filepath, err)
	}

	w := csv.NewWriter(f)

	w.Comma = ','
	//w.UseCRLF = true // set change-line code to CRLF(\r\n)

	// sort keys(epochnanosec)
	epochnanos := make([]int64, len(quotes))
	index := 0
	for key := range quotes {
		epochnanos[index] = key
		index++
	}
	sort.SliceStable(epochnanos, func(i, j int) bool {
		return epochnanos[i] < epochnanos[j]
	})

	for _, epochnano := range epochnanos {
		record := []string{strconv.FormatInt(epochnano, 10),
			strconv.FormatFloat(float64(quotes[epochnano].AskPrice), 'f', -1, 32),
			strconv.FormatUint(uint64(quotes[epochnano].AskSize), 10),
			strconv.FormatFloat(float64(quotes[epochnano].BidPrice), 'f', -1, 64),
			strconv.FormatUint(uint64(quotes[epochnano].BidSize), 10),
		}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("write data to %s: %w", filepath, err)
		}
	}

	w.Flush() // flush and write all the buffer

	if err := w.Error(); err != nil {
		return fmt.Errorf("write/flush data to %s: %w", filepath, err)
	}
	return nil
}

func GetTrades(client *alpaca.Client, symbols []string) error {
	maxTradesPerRequest := 10000

	for _, symbol := range symbols {
		pageStartTimestamp := time.Date(2021, 11, 17, 0, 0, 0, 0, time.UTC)
		endTimestamp := time.Unix(pageStartTimestamp.Unix(), int64(pageStartTimestamp.Nanosecond())).Add(24 * time.Hour)

		tradesChunk := make(map[int64]v2.Trade, 0)
		for {
			fmt.Printf("GetTrades: symbol=%s, startTime=%v, endTime=%v\n", symbol, pageStartTimestamp, pageStartTimestamp)
			tradesChan := client.GetTrades(symbol, pageStartTimestamp, pageStartTimestamp.Add(24*time.Hour), maxTradesPerRequest)
			for trade := range tradesChan {
				if trade.Error != nil {
					return fmt.Errorf("GetTrade API call: %w", trade.Error)
				}

				pageStartTimestamp = trade.Trade.Timestamp.Add(1 * time.Nanosecond)

				if pageStartTimestamp.After(endTimestamp) {
					break
				}

				tradesChunk[trade.Trade.Timestamp.UnixNano()] = trade.Trade
			}
			if pageStartTimestamp.After(endTimestamp) {
				break
			}
		}
		err := writeCSVTrade(fmt.Sprintf("data_Trade/%s.csv", symbol), tradesChunk)
		if err != nil {
			return err
		}

		// to avoid rate limit
		time.Sleep(1 * time.Second)
	}
	return nil
}

// quotes: key=unix nano second
func writeCSVTrade(filepath string, trades map[int64]v2.Trade) error {
	f, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("create CSV file %s: %w", filepath, err)
	}

	w := csv.NewWriter(f)

	w.Comma = ','
	//w.UseCRLF = true // set change-line code to CRLF(\r\n)

	// sort keys(epochnanosec)
	epochnanos := make([]int64, len(trades))
	index := 0
	for key := range trades {
		epochnanos[index] = key
		index++
	}
	sort.SliceStable(epochnanos, func(i, j int) bool {
		return epochnanos[i] < epochnanos[j]
	})

	for _, epochnano := range epochnanos {
		record := []string{strconv.FormatInt(epochnano, 10),
			strconv.FormatFloat(float64(trades[epochnano].Price), 'f', -1, 32),
			strconv.FormatUint(uint64(trades[epochnano].Size), 10),
		}
		if err := w.Write(record); err != nil {
			return fmt.Errorf("write data to %s: %w", filepath, err)
		}
	}

	w.Flush() // flush and write all the buffer

	if err := w.Error(); err != nil {
		return fmt.Errorf("write/flush data to %s: %w", filepath, err)
	}
	return nil
}
