# 💍 GLAMIRA Website Product Information Crawling

![GLAMIRA Banner](https://github.com/user-attachments/assets/f87df3b0-d6de-40ca-a1bf-8855297f6c4f)

A focused web crawling project designed to extract jewelry product data (names, descriptions, images, prices, etc.) from [www.glamira.com](https://www.glamira.com) for AI model training purposes.

---

## 📚 Table of Contents

1. [🎯 Purpose](#-purpose)
2. [📦 Example Product Data](#-example-product-data)
3. [🛠 Features](#-features)
4. [🚀 How to Run](#-how-to-run)
5. [📁 Output Format](#-output-format)
6. [⚠️ Legal Disclaimer](#️-legal-disclaimer)

---

## 🎯 Purpose

As a **Data Engineer** working for an AI company, your mission is to build an efficient web crawler to gather high-quality data from the [GLAMIRA](https://www.glamira.com) e-commerce site.

### ✅ Project Goals:
- Extract **product images**, **descriptions**, **categories**, and **pricing** from glamira.com.
- Handle:
  - 🔄 **Pagination**
  - 🔍 **Dynamic content**
  - 🛡️ **Anti-crawling mechanisms**
- Output a **structured dataset** (e.g. JSON or CSV) for downstream ML training pipelines.

---

## 📦 Example Product Data

```json
[
  {
    "calatog_name": "apple-watch-cases",
    "products": [
      {
        "product_name": "GLAMIRA Apple Watch® Case Apasa",
        "image_link": "https://cdn-media.glamira.com/media/catalog/product/a/p/apasa_view_2.jpg",
        "short_description": "18K Rose Gold IP Plated 316L Stainless Steel Adorned With 42 Swarovski Crystals",
        "price": "$283.00",
        "product_link": "https://www.glamira.com/glamira-apple-watchr-case-apasa.html"
      },
      {
        "product_name": "GLAMIRA Apple Watch® Case Korseon",
        "image_link": "https://cdn-media.glamira.com/media/catalog/product/k/o/korseon_view_2_2.jpg",
        "short_description": "18K Gold IP Plated 316L Stainless Steel",
        "price": "$566.00",
        "product_link": "https://www.glamira.com/glamira-apple-watchr-case-apasa.html"
      },
      {
        "product_name": "GLAMIRA Apple Watch® Case Psara",
        "image_link": "https://cdn-media.glamira.com/media/catalog/product/p/s/psara_view__2_2.jpg",
        "short_description": "18K Gold IP Plated 316L Stainless Steel",
        "price": "$220.00",
        "product_link": "https://www.glamira.com/glamira-apple-watchr-case-korseon.html"
      }
    ]
  }
]
```
