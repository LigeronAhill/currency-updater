package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/surrealdb/surrealdb.go"
	"golang.org/x/net/html/charset"
)

func main() {
	db, err := initDB("root", "root", "test", "test")
	if err != nil {
		panic(err)
	}
	errorsCount := 0
	for {
		result, err := getCurrencies()
		if err != nil {
			log.Println(err)
			errorsCount++
			continue
		}
		log.Println("Currencies received successfully")
		err = updateCurrencies(db, result)
		if err != nil {
			log.Println(err)
			errorsCount++
			continue
		}
		if errorsCount > 10 {
			log.Println("Errors count greaer then 10")
			break
		}
		log.Println("Currencies in DB updated")
		log.Println("Paused for 24 hours")
		time.Sleep(24 * time.Hour)
	}
}

type ValCurs struct {
	XMLName xml.Name `xml:"ValCurs"`
	Text    string   `xml:",chardata"`
	Date    string   `xml:"Date,attr"`
	Name    string   `xml:"name,attr"`
	Valute  []struct {
		Text      string `xml:",chardata"`
		ID        string `xml:"ID,attr"`
		NumCode   string `xml:"NumCode"`
		CharCode  string `xml:"CharCode"`
		Nominal   string `xml:"Nominal"`
		Name      string `xml:"Name"`
		Value     string `xml:"Value"`
		VunitRate string `xml:"VunitRate"`
	} `xml:"Valute"`
}

func initDB(user, pass, ns, dbName string) (*surrealdb.DB, error) {
	db, err := surrealdb.New("ws://localhost:8000/rpc")
	if err != nil {
		return nil, err
	}
	if _, err = db.Signin(map[string]interface{}{
		"user": user,
		"pass": pass,
	}); err != nil {
		return nil, err
	}
	if _, err = db.Use(ns, dbName); err != nil {
		return nil, err
	}
	initQuery := []string{
		`DEFINE TABLE IF NOT EXISTS currency SCHEMAFULL;`,
		`DEFINE FIELD IF NOT EXISTS name ON TABLE currency TYPE string;`,
		`DEFINE FIELD IF NOT EXISTS char_code ON TABLE currency TYPE string VALUE string::uppercase($value);`,
		`DEFINE FIELD IF NOT EXISTS rate ON TABLE currency TYPE float;`,
		`DEFINE FIELD IF NOT EXISTS updated ON TABLE currency TYPE datetime VALUE time::now();`,
		`DEFINE INDEX charCodeIndex ON TABLE currency COLUMNS char_code UNIQUE;`,
		`INSERT INTO currency (id, name, char_code, rate)
		VALUES 
		('rub', 'Российский рубль', 'rub', 1.0)
		ON DUPLICATE KEY UPDATE
		rate = $input.rate,
		updated = time::now();`,
	}
	for _, q := range initQuery {
		if _, err = db.Query(q, nil); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func getCurrencies() (*ValCurs, error) {
	client := http.Client{}
	url := "http://www.cbr.ru/scripts/XML_daily.asp"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add(http.CanonicalHeaderKey("user-agent"), " Mozilla/5.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result ValCurs
	reader := bytes.NewReader(body)
	decoder := xml.NewDecoder(reader)
	decoder.CharsetReader = charset.NewReaderLabel
	err = decoder.Decode(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func updateCurrencies(db *surrealdb.DB, res *ValCurs) error {
	for _, c := range res.Valute {
		id := strings.ToLower(c.CharCode)
		char_code := c.CharCode
		name := c.Name
		rate, err := strconv.ParseFloat(strings.ReplaceAll(c.Value, ",", "."), 64)
		if err != nil {
			return err
		}
		query := fmt.Sprintf("INSERT INTO currency (id, name, char_code, rate)  VALUES ('%s', '%s', '%s', %v) ON DUPLICATE KEY UPDATE rate = $input.rate, updated = time::now();", id, name, char_code, rate)
		if _, err = db.Query(query, nil); err != nil {
			return err
		}
	}
	return nil
}
