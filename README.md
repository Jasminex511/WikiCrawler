# 🕸️ Wikipedia Profile Extractor

A pipeline that crawls Wikipedia pages, extracts person-related information using LLMs, and stores enriched data into **MongoDB** and **Elasticsearch** for downstream usage, providing a search API for external access.

## 🔧 Project Structure

- **Scrapy Crawler**: Starts from Wikipedia main page and discovers person profile pages.
- **Kafka**: Acts as a message broker to decouple components.
- **LLM**: Consumes raw HTML content and uses OpenAI to extract structured person information.
- **MongoDB**: Persists structured profile data.
- **Elasticsearch**: Indexes data for full-text search.
- **Flask**: Provides `/search` API endpoint to query indexed person profiles.

---

## 🧱 Architecture

```mermaid
flowchart TB
  Scrapy["Scrapy Crawler"]
  HTML["HTML Consumer"]
  LLM["LLM Processor"]
  Mongo["MongoDB"]
  ES["Elasticsearch"]

  subgraph Kafka
    direction TB
    K1["Topic: wiki-content"]
    K2["Topic: wiki-profile"]
  end

  Scrapy -->|produce| K1
  K1 -->|consume| HTML
  HTML --> LLM
  LLM -->|produce| K2
  K2 -->|consume| Mongo
  K2 -->|consume| ES
