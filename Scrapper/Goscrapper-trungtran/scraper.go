package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

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
	Battery  string `json:battery`
	// Os string `json:os`
}

func main() {
	fileName := "laptops.csv"
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Could not create %s", fileName)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"Name", "Price", "Cpu", "Ram", "Storage", "Display", "Graphics", "Battery"})
	c := colly.NewCollector(
		colly.AllowedDomains("trungtran.vn"),
	)
	var items []item

	// c.OnHTML("div.product-small.box", func(h *colly.HTMLElement) {
	// 	item := item{
	// 		Name:   h.ChildText("div.box-text.box-text-products > div.title-wrapper > p > a"),
	// 		ImgUrl: h.ChildAttr("div.box-image > div.image-zoom > a > img", "src"),
	// 	}

	// 	if h.ChildText(" div.box-text.box-text-products > div.price-wrapper > span > ins > span > bdi") != "" {
	// 		item.Price = h.ChildText(" div.box-text.box-text-products > div.price-wrapper > span > ins > span > bdi")
	// 	} else {
	// 		item.Price = h.ChildText("div.box-text.box-text-products > div.price-wrapper > span > span > bdi")
	// 	}
	// 	// fmt.Println(item.Name)
	// 	// fmt.Println(item.Price)
	// 	// fmt.Println(item.ImgUrl)
	// 	items = append(items, item)
	// })

	// #box_product > div.col-sm-5ths.bor_top_left > div > div > a:nth-child(1)

	c.OnHTML("div.frame_inner", func(h *colly.HTMLElement) {
		laptop_detail := h.Request.AbsoluteURL(h.ChildAttr("a", "href"))
		item := item{
			Name:  h.ChildText("a > div.frame_title > h3"),
			Price: h.ChildText("div.frame_price > span.price"),
		}
		c.OnHTML("table.charactestic_table", func(h *colly.HTMLElement) {

			// if h.ChildText(" a	 > div.price-wrapper > span > ins > span > bdi") != "" {
			// 	item.Price = h.ChildText(" div.box-text.box-te > div.price-wrapper > span > ins > span > bdi")
			// } else {
			// 	item.Price = h.ChildText("div.box-text.box-text-products > div.price-wrapper > span > span > bdi")
			// }
			// fmt.Println(item.Name)
			// fmt.Println(item.Price)
			// fmt.Println(item.ImgUrl)
			item.Cpu = h.ChildText("tbody > tr:nth-child(1) > td.content_charactestic")
			item.Ram = h.ChildText("tbody > tr:nth-child(2) > td.content_charactestic")
			item.Storage = h.ChildText("tbody > tr:nth-child(3) > td.content_charactestic")
			item.Dislay = h.ChildText("tbody > tr:nth-child(4) > td.content_charactestic")
			item.Graphics = h.ChildText("tbody > tr:nth-child(5) > td.content_charactestic")
			item.Battery = h.ChildText("tbody > tr:nth-child(6)> td.content_charactestic")
			// fmt.Println(item.Name)
			// fmt.Println(item.Price)
			// fmt.Println(item.Cpu)
			// fmt.Println(item.Ram)
			// fmt.Println(item.Storage)
			// fmt.Println(item.Dislay)
			// fmt.Println(item.Graphics)
			// fmt.Println(item.Battery)
			writer.Write([]string{item.Name, item.Price, item.Cpu, item.Ram, item.Storage, item.Dislay, item.Graphics, item.Battery})
			items = append(items, item)
		})
		c.Visit(laptop_detail)

	})

	c.OnHTML("a.next-page", func(h *colly.HTMLElement) {
		next_page := h.Request.AbsoluteURL(h.Attr("href"))
		c.Visit(next_page)
	})
	c.OnRequest(func(r *colly.Request) {
		fmt.Println(r.URL.String())
	})
	c.Visit("https://trungtran.vn/laptop/filter/tinh-trang=like-new-used,like-new-used/")

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
