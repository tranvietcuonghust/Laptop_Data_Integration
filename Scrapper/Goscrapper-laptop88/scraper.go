package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"strconv"

	"github.com/gocolly/colly"
)

type item struct {
	Name  string `json:name`
	Price string `json:price`
	// ImgUrl   string `json:imgurl`
	Cpu      string `json:cpu`
	Storage  string `json:storage`
	Ram      string `json:ram`
	Dislay   string `json:display`
	Graphics string `json:graphics`
	// Os string `json:os`
}

func main() {
	page_number := 2
	fileName := "laptops.csv"
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Could not create %s", fileName)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"Name", "Price", "Cpu", "Ram", "Storage", "Display", "Graphics"})
	c := colly.NewCollector(
		colly.AllowedDomains("laptop88.vn"),
	)
	var items []item

	c.OnHTML("div.product-item", func(h *colly.HTMLElement) {
		// laptop_detail := h.Request.AbsoluteURL(h.ChildAttr("a", "href"))
		item := item{
			Name:     h.ChildText("div.product-info > div.product-title > a"),
			Price:    h.ChildText("div.product-info > div.product-price > div.price-bottom > span"),
			Cpu:      h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(1) > td:nth-child(2)"),
			Ram:      h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(2) > td:nth-child(2)"),
			Storage:  h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(3) > td:nth-child(2)"),
			Dislay:   h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(5) > td:nth-child(2)"),
			Graphics: h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(4) > td:nth-child(2)"),
		}
		fmt.Println(item.Name)
		fmt.Println(item.Price)
		fmt.Println(item.Cpu)
		fmt.Println(item.Ram)
		fmt.Println(item.Storage)
		fmt.Println(item.Dislay)
		fmt.Println(item.Graphics)
		// fmt.Println(item.Battery)
		writer.Write([]string{item.Name, item.Price, item.Cpu, item.Ram, item.Storage, item.Dislay, item.Graphics})
		items = append(items, item)
		// c.Visit(laptop_detail)

	})

	c.OnHTML("div.paging.d-flex.align-items.space-center", func(h *colly.HTMLElement) {

		next_page := h.Request.AbsoluteURL("https://laptop88.vn/laptop-cu.html?page=" + strconv.Itoa(page_number))

		c.OnHTML("div.product-item", func(h *colly.HTMLElement) {
			// laptop_detail := h.Request.AbsoluteURL(h.ChildAttr("a", "href"))
			item := item{
				Name:     h.ChildText("div.product-info > div.product-title > a"),
				Price:    h.ChildText("div.product-info > div.product-price > div.price-bottom > span"),
				Cpu:      h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(1) > td:nth-child(2)"),
				Ram:      h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(2) > td:nth-child(2)"),
				Storage:  h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(3) > td:nth-child(2)"),
				Dislay:   h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(5) > td:nth-child(2)"),
				Graphics: h.ChildText("div.product-info > div.product-promotion > table > tbody > tr:nth-child(4) > td:nth-child(2)"),
			}

			fmt.Println(item.Name)
			fmt.Println(item.Price)
			fmt.Println(item.Cpu)
			fmt.Println(item.Ram)
			fmt.Println(item.Storage)
			fmt.Println(item.Dislay)
			fmt.Println(item.Graphics)
			// fmt.Println(item.Battery)
			writer.Write([]string{item.Name, item.Price, item.Cpu, item.Ram, item.Storage, item.Dislay, item.Graphics})
			items = append(items, item)
			// c.Visit(laptop_detail)

		})
		for page_number < 8 {
			page_number++
			c.Visit(next_page)
		}

	})
	c.OnRequest(func(r *colly.Request) {
		fmt.Println(r.URL.String())
	})
	c.Visit("https://laptop88.vn/laptop-cu.html")

	// for i := range items {
	// 	item := items[i]
	// 	fmt.Println(item.Name)
	// 	fmt.Println(item.Price)
	// 	fmt.Println(item.ImgUrl)
	// }
	// content, err := json.Marshal(items)
	// content,err := csv.Marshal(items)
	// if err != nil {
	// 	fmt.Println("errrrr:", err.Error())
	// }
	// os.WriteFile("laptop.json", content, 0644)
	// f, err := os.Create("laptops.csv")
	// defer f.Close()

	// if err != nil {

	//     log.Fatalln("failed to open file", err)
	// }

	// w := csv.NewWriter(f)
	// err = w.WriteAll(items) // calls Flush internally

	// if err != nil {
	//     log.Fatal(err)
	// }
}
