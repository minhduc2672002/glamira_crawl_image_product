# GLAMIRA WEBSITE PRODUCT INFORMATION CRAWLING
![image](https://github.com/user-attachments/assets/f87df3b0-d6de-40ca-a1bf-8855297f6c4f)

# Table of Contents

1. [Purpose](#purpose)
2. [Example Product Data](#example-Product-Data)

## Purpose
- As a Data Engineer working for an AI company, your task is to design and implement an efficient web crawling bot to gather product photos along with their descriptions and information on website www.glamira.com for the purpose of training a machine learning model for jewelry product detection.

- Develop a web crawler that can efficiently and accurately extract product photos, descriptions, and other relevant information from targeted websites. Ensure the crawler adheres to the websites' robots.txt rules and terms of service. Implement mechanisms to handle dynamic content, pagination, and possible anti-crawling measures. Optimize the crawler for speed and low resource consumption
- The dictionary should include keys for  names, descriptions, image URLs, prices, categories, and any other relevant information. Ensure the dictionary format is consistent and easily convertible to a structured data format like JSON or CSV for further processing.
## Example Product Data
```"[
    {
        "calatog_name": "apple-watch-cases",
        "products": [
            {
                "product_name": "GLAMIRA Apple Watch\u00ae Case Apasa",
                "image_link": "https://cdn-media.glamira.com/media/catalog/product/a/p/apasa_view_2.jpg",
                "short_description": "18K Rose Gold IP Plated 316L Stainless Steel Adorned With 42 Swarovski Crystals",
                "price": "$283.00",
                "product_link": "https://www.glamira.com/glamira-apple-watchr-case-apasa.html"
            },
            {
                "product_name": "GLAMIRA Apple Watch\u00ae Case Korseon",
                "image_link": "https://cdn-media.glamira.com/media/catalog/product/k/o/korseon_view_2_2.jpg",
                "short_description": "18K Gold  IP Plated 316L Stainless Steel",
                "price": "$566.00",
                "product_link": "https://www.glamira.com/glamira-apple-watchr-case-apasa.html"
            },
            {
                "product_name": "GLAMIRA Apple Watch\u00ae Case Psara",
                "image_link": "https://cdn-media.glamira.com/media/catalog/product/p/s/psara_view__2_2.jpg",
                "short_description": "18K Gold IP Plated 316L Stainless Steel",
                "price": "$220.00",
                "product_link": "https://www.glamira.com/glamira-apple-watchr-case-korseon.html"
            }]
}
]
