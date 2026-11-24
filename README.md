# App Lookup &bull; iOS &amp; Android Store Metadata Enricher

A fast, lightweight, and unified tool to look up and enrich mobile application metadata from the **Apple App Store** and **Google Play Store**.

Features a subtle, minimalist single web UI, batch Excel/CSV file processing, and interactive real-time lookup.

---

## Features

- **Dual-Platform Support**:
  - **Apple App Store**: High-speed batch queries via iTunes Lookup API with fallback web page scraping.
  - **Google Play Store**: Scrapes structured metadata (JSON-LD `SoftwareApplication`) with multi-country storefront support.
- **Smart Auto-Detection**: Automatically identifies whether an input is an iOS App ID/URL or an Android package name/URL.
- **Batch Excel / CSV Enricher**:
  - Upload `.xlsx`, `.xls`, or `.csv` files.
  - Automatically identifies the column containing app IDs or URLs.
  - Generates and downloads an enriched spreadsheet containing:
    `Platform`, `Extracted_ID`, `App_Name`, `Developer`, `Category`, `Store_URL`, `Storefront`, and `Source`.
- **Interactive Quick Search**: Look up multiple app IDs or URLs in real time with an on-screen preview table.
- **Self-Contained & Fast**: Pure Python backend with asynchronous concurrency (`asyncio` + `httpx`), openpyxl, and BeautifulSoup.

---

## Local Setup & Running

### 1. Clone the repository
```bash
git clone https://github.com/vedantkudalkarmobstar/App-Lookup.git
cd App-Lookup
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start the application
```bash
python app_store_api.py
```
*(Or run `uvicorn app_store_api:app --reload --port 8080`)*

Open your browser at:
- **Web Interface:** [http://127.0.0.1:8080](http://127.0.0.1:8080)
- **Interactive Swagger API Docs:** [http://127.0.0.1:8080/docs](http://127.0.0.1:8080/docs)

---

## API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/` | Unified Web User Interface |
| `GET` | `/lookup/ios/{app_id}` | Look up single iOS app by numeric ID or URL |
| `GET` | `/lookup/android/{package_name}` | Look up single Android app by package name or URL |
| `GET` | `/lookup/auto/{identifier}` | Auto-detect and look up single app |
| `POST` | `/lookup/quick` | JSON endpoint for live interactive search |
| `POST` | `/lookup/batch` | Batch JSON lookup for multiple app IDs |
| `POST` | `/lookup/excel` | Upload `.xlsx`/`.csv` file and download enriched Excel sheet |

---

## Deployment

### Deploy on Render
1. Create a new **Web Service** on [dashboard.render.com](https://dashboard.render.com).
2. Connect this GitHub repository.
3. Set the following:
   - **Runtime:** `Python 3`
   - **Build Command:** `pip install -r requirements.txt`
   - **Start Command:** `uvicorn app_store_api:app --host 0.0.0.0 --port $PORT`
4. Click **Create Web Service**.

### Deploy on Vercel
1. Import this repository on [vercel.com](https://vercel.com).
2. Vercel will automatically use the included `vercel.json` and deploy as serverless functions.
