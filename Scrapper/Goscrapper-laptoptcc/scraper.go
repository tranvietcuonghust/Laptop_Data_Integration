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
	// Cpu      string `json:cpu`
	// Storage  string `json:storage`
	// Ram      string `json:ram`
	// Dislay   string `json:display`
	// Graphics string `json:graphics`
	// Battery  string `json:battery`
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
	writer.Write([]string{"Name", "Price"})
	c := colly.NewCollector(
		colly.AllowedDomains("laptoptcc.com"),
	)
	var items []item
	laptop_link := [5]string{"https://laptoptcc.com/danh-muc/laptop-dell/",
		"https://laptoptcc.com/danh-muc/laptop-hp/",
		"https://laptoptcc.com/danh-muc/laptop-lenovo-thinkpad/",
		"https://laptoptcc.com/danh-muc/laptop-chuyen-gaming/laptop-msi-gaming/",
		"https://laptoptcc.com/danh-muc/laptop-chuyen-gaming/laptop-asus-gaming/"}

	c.OnHTML("div.product-inner", func(h *colly.HTMLElement) {
		laptop_detail := h.Request.AbsoluteURL(h.ChildAttr("div.product-loop-body > a", "href"))

		item := item{
			Name:  h.ChildText("div.product-loop-body > a.woocommerce-loop-product__link > h2.woocommerce-loop-product__title"),
			Price: h.ChildText("div.product-loop-footer > div.price-add-to-cart > span > span > ins > span > bdi"),
		}
		// 		<div class="woocommerce-variation-description">
		// 		<p><span><b>CPU</b> Intel Core i5-11400H </span><br>
		// <span><b>Ram</b> 8GB DDR4 </span><br>
		// <span><b>Ổ cứng</b> 512GB M.2 SSD </span><br>
		// <span><b>Màn hình</b> 15.6″ FHD </span><br>
		// <span><b>VGA</b> VGA Nvidia Geforce RTX 3050</span><br>
		// <span><b>Tình trạng</b> New </span></p>

		// 	</div>
		// fmt.Println(h.ChildText("div.product-loop-body > a.woocommerce-loop-product__link > h2.woocommerce-loop-product__title"))
		//fmt.Println(item.Name)

		// <span>
		// <b>Ổ cứng</b>
		//  SSD 256GB
		// </span>
		// div.woocommerce-variation-description > p > span:nth-child(5)
		c.OnHTML("div.woocommerce-variation-description", func(h *colly.HTMLElement) {
			fmt.Println(h.ChildText("p > span:nth-child(5)"))
			// item.Cpu = h.ChildText("table > tbody > tr:nth-child(5) > td:nth-child(2)")
			// item.Ram = h.ChildText("table > tbody > tr:nth-child(6) > td:nth-child(2)")
			// item.Storage = h.ChildText("table > tbody > tr:nth-child(8) > td:nth-child(2)")
			// item.Dislay = h.ChildText("table > tbody > tr:nth-child(9) > td:nth-child(2)")
			// item.Graphics = h.ChildText("table > tbody > tr:nth-child(7) > td:nth-child(2)")
			// item.Battery = h.ChildText("table > tbody > tr:nth-child(16) > td:nth-child(2)")
			// fmt.Println(item.Name)
			// fmt.Println(item.Price)
			// fmt.Println(item.Cpu)
			// fmt.Println(item.Ram)
			// fmt.Println(item.Storage)
			// fmt.Println(item.Dislay)
			// fmt.Println(item.Graphics)
			// writer.Write([]string{item.Name, item.Price, item.Cpu, item.Ram, item.Storage, item.Dislay, item.Graphics, item.Battery})
			items = append(items, item)
		})
		writer.Write([]string{item.Name, item.Price})
		c.Visit(laptop_detail)

	})

	c.OnHTML("a.next.page-numbers", func(h *colly.HTMLElement) {
		next_page := h.Request.AbsoluteURL(h.Attr("href"))
		c.Visit(next_page)
	})
	c.OnRequest(func(r *colly.Request) {
		fmt.Println(r.URL.String())
	})

	for i, s := range laptop_link {
		fmt.Println(i)
		c.Visit(s)

	}
	// c.Visit("https://laptoptcc.com/danh-muc/laptop-chuyen-gaming/laptop-asus-gaming/")

	// for i := range items {
	// 	item := items[i]
	// 	fmt.Println(item.Name)
	// 	fmt.Println(item.Price)
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
