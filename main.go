package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/xh3b4sd/budget/v3"
	"github.com/xh3b4sd/budget/v3/pkg/breaker"
	"github.com/xh3b4sd/eth-prices/pkg/apicliaws"
	"github.com/xh3b4sd/framer"
)

const (
	apifmt = "https://api.coingecko.com/api/v3/coins/ethereum/history?date=%s&localization=false"
	dayzer = "2020-12-01T00:00:00Z"
	bucnam = "chiron-data-collector"
	filpat = "eth/prices.csv"
	reqlim = 5
)

type csvrow struct {
	Dat time.Time
	APR float64
}

type resstr struct {
	MarketData resstrdat `json:"market_data"`
}

type resstrdat struct {
	CurrentPrice resstrdatpri `json:"current_price"`
}

type resstrdatpri struct {
	USD float64 `json:"usd"`
}

func main() {
	var err error

	var cli *apicliaws.AWS
	{
		cli = apicliaws.New()
	}

	var byt []byte
	{
		byt, err = cli.Download(bucnam, filpat)
		if apicliaws.IsNotFound(err) {
			// fall through
		} else if err != nil {
			panic(err)
		}
	}

	var row [][]string
	{
		row, err = csv.NewReader(bytes.NewReader(byt)).ReadAll()
		if err != nil {
			log.Fatal(err)
		}
	}

	cur := map[time.Time]float64{}
	for _, x := range row[1:] {
		cur[mustim(x[0])] = musf64(x[1])
	}

	var sta time.Time
	{
		sta = mustim(dayzer)
	}

	var end time.Time
	{
		end = time.Date(time.Now().Year(), time.Now().Month(), time.Now().Day(), 0, 0, 0, 0, time.UTC)
	}

	var bud budget.Interface
	{
		bud = breaker.Default()
	}

	var fra *framer.Framer
	{
		fra = framer.New(framer.Config{
			Sta: sta,
			End: end,
			Len: 24 * time.Hour,
		})
	}

	var cou int
	des := map[time.Time]float64{}
	for _, x := range fra.List() {
		f64, exi := cur[x.Sta]
		if exi {
			{
				// log.Printf("setting cached prices for %s\n", x.Sta)
			}

			{
				des[x.Sta] = f64
			}
		} else if cou < reqlim {
			{
				cou++
			}

			{
				log.Printf("filling remote prices for %s\n", x.Sta)
			}

			var act func() error
			{
				act = func() error {
					var f64 float64
					{
						f64 = musapi(x.Sta)
					}

					if f64 == 0 {
						return budget.Cancel
					}

					{
						des[x.Sta] = f64
					}

					return nil
				}
			}

			{
				err = bud.Execute(act)
				if budget.IsCancel(err) {
					break
				} else if budget.IsPassed(err) {
					break
				} else if err != nil {
					log.Fatal(err)
				}
			}

			{
				time.Sleep(1 * time.Second)
			}
		}
	}

	var lis []csvrow
	for k, v := range des {
		lis = append(lis, csvrow{Dat: k, APR: v})
	}

	{
		sort.SliceStable(lis, func(i, j int) bool { return lis[i].Dat.Before(lis[j].Dat) })
	}

	var res [][]string
	{
		res = append(res, []string{"date", "price"})
	}

	for _, x := range lis {
		res = append(res, []string{x.Dat.Format(time.RFC3339), fmt.Sprintf("%.16f", x.APR)})
	}

	var wri *bytes.Buffer
	{
		wri = bytes.NewBufferString("")
	}

	{
		err = csv.NewWriter(wri).WriteAll(res)
		if err != nil {
			log.Fatal(err)
		}
	}

	{
		err = cli.Upload(bucnam, filpat, *bytes.NewReader(wri.Bytes()))
		if err != nil {
			panic(err)
		}
	}
}

func musapi(des time.Time) float64 {
	var err error

	var str string
	{
		str = des.Format("02-01-2006")
	}

	var cli *http.Client
	{
		cli = &http.Client{Timeout: 10 * time.Second}
	}

	var res *http.Response
	{
		u := fmt.Sprintf(apifmt, str)

		res, err = cli.Get(u)
		if err != nil {
			log.Fatal(err)
		}
	}

	{
		defer res.Body.Close()
	}

	var byt []byte
	{
		byt, err = io.ReadAll(res.Body)
		if err != nil {
			log.Fatal(err)
		}
	}

	var dat resstr
	{
		err = json.Unmarshal(byt, &dat)
		if err != nil {
			log.Fatal(err)
		}
	}

	return dat.MarketData.CurrentPrice.USD
}

func musf64(str string) float64 {
	f64, err := strconv.ParseFloat(str, 64)
	if err != nil {
		log.Fatal(err)
	}

	return f64
}

func mustim(str string) time.Time {
	tim, err := time.Parse(time.RFC3339, str)
	if err != nil {
		log.Fatal(err)
	}

	return tim
}
