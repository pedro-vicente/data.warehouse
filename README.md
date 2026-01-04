# Top US Companies Data Warehouse

Data warehouse for tracking the top 500 US companies by market capitalization, implemented with C++ and ODBC for Microsoft SQL Server.

## Kimball Star Schema Methodology

This project implements Ralph Kimball's dimensional modeling approach:

**Live Demo:** https://nostro.cloud:9500/

![Star Schema Diagram](https://github.com/user-attachments/assets/b8f07cda-74e0-45d2-a880-4e33953b7c83)

```
                      ┌─────────────┐
                      │   DimDate   │
                      └──────┬──────┘
                             │
  ┌─────────────┐     ┌──────┴──────┐     ┌─────────────┐
  │ DimCompany  │────►│ FactStock   │◄────│  DimSector  │
  └─────────────┘     └─────────────┘     └─────────────┘
                             │
                      ┌──────┴──────┐
                      │FactFinancial│
                      └─────────────┘
```

## Schema Design

### Dimension Tables

- **DimDate** - Calendar dimension with fiscal year support (Federal Oct-Sep)
- **DimCompany** - Company attributes with SCD Type 2 for historical tracking
- **DimSector** - Industry sector classification

### Fact Tables

- **FactDailyStock** - Daily stock prices, volume, market cap, technical indicators
- **FactFinancials** - Quarterly financial statements (revenue, margins, ROE, ROA)
- **FactValuation** - Valuation ratios (P/E, P/S, EV/EBITDA, etc.)

## Build Instructions

### Install Boost (Linux)

On Ubuntu/Debian:
```bash
sudo apt install libboost-all-dev
```

On Windows, build from source:
```bash
build.boost.bat
```

build with 

```bash
./build.wt.sh
./build.cmake.sh
```

## Database Setup (Linux)

### Prerequisites

Install SQL Server:

```bash
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl -fsSL https://packages.microsoft.com/config/ubuntu/24.04/mssql-server-preview.list | sudo tee /etc/apt/sources.list.d/mssql-server-preview.list
sudo apt-get update
sudo apt-get install -y mssql-server
sudo /opt/mssql/bin/mssql-conf setup
systemctl status mssql-server --no-pager
```

Install the SQL Server command-line tools

```bash
curl -sSL -O https://packages.microsoft.com/config/ubuntu/24.04/packages-microsoft-prod.deb
sudo dpkg -i packages-microsoft-prod.deb
sudo apt-get update
sudo apt-get install mssql-tools18 unixodbc-dev
sudo apt install msodbcsql18
```

### Generate Database from schema.sql

```bash
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourPassword123!' -Q "CREATE DATABASE data_warehouse" -C 
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourPassword123!' -d data_warehouse -i schema.sql -C
```

## Executables

| Target | Description |
|--------|-------------|
| fetch | Data fetcher - retrieves financial data from Alpha Vantage API |
| etl | ETL pipeline - loads CSV data into SQL Server |
| web | Wt web application for data visualization |

## Data Fetcher (fetch)

The `fetch` program retrieves real financial data from the Alpha Vantage REST API and generates CSV files for the ETL pipeline.

### Usage

```bash
./fetch [OPTIONS]
```

### Fetch Options

| Option | Description |
|--------|-------------|
| `--stocks` | Fetch daily stock prices (OHLCV data) |
| `--companies` | Fetch company overview/info |
| `--income` | Fetch income statements |
| `--balance` | Fetch balance sheets |
| `--financials` | Fetch both income and balance (merged) |
| `--all` | Fetch all data types (default if none specified) |

### Other Options

| Option | Description |
|--------|-------------|
| `-t, --ticker SYM` | Fetch single ticker only |
| `-n, --count N` | Number of companies to fetch (default: all) |
| `-d, --days N` | Days of stock history (default: 2) |
| `-w, --wait N` | Seconds between API calls (default: 12) |
| `--test` | Test mode: 1 company, 3 sec wait |
| `-h, --help` | Display help message |

### Output Files

| File | Description |
|------|-------------|
| `stock_data.csv` | Daily OHLCV data |
| `companies.csv` | Company information |
| `financials.csv` | Financial statements |
| `tickers.csv` | Sorted ticker list by market cap |

### Examples

```bash
# Test with 1 company (quick test)
./fetch --test

# Fetch top 50 companies by market cap, stock prices only
./fetch -n 50 --stocks

# Fetch all S&P 500 companies (all data types)
./fetch --all

# Fetch single ticker only
./fetch --ticker AAPL

# Fetch income statements for top 10 companies
./fetch -n 10 --income

# Fetch with custom wait time (for API rate limits)
./fetch -n 100 -w 15 --stocks
```

### API Key Setup

Get a free API key from: https://www.alphavantage.co/support/#api-key

Save the key to `alpha.vantage.txt`:
```bash
echo "YOUR_API_KEY" > alpha.vantage.txt
```

## ETL Pipeline (etl)

The `etl` program loads CSV data into SQL Server using the Kimball star schema methodology.

### Usage

```bash
./etl -S SERVER -d DATABASE [-U USER] [-P PASSWORD] [--delete]
```

### Options

| Option | Description |
|--------|-------------|
| `-S SERVER` | SQL Server hostname or IP address (required) |
| `-d DATABASE` | Database name (required) |
| `-U USER` | SQL Server username (omit for trusted connection) |
| `-P PASSWORD` | SQL Server password |
| `--delete` | Delete all data from all tables |

### Examples

```bash
# Load data with SQL authentication
./etl -S localhost -d data_warehouse -U sa -P 'YourPassword123!'

# Load data with Windows trusted connection
./etl -S localhost -d data_warehouse

# Delete all data before reloading
./etl -S localhost -d data_warehouse -U sa -P 'YourPassword123!' --delete
```

### SQL Queries in ETL

The ETL pipeline uses the following SQL operations:

#### Schema Creation (create_schema)

Creates dimension and fact tables if they don't exist:

```sql
-- DimDate: Calendar dimension with fiscal year support
CREATE TABLE DimDate (
  DateKey INT PRIMARY KEY,
  FullDate DATE, Year INT, Quarter INT, Month INT,
  MonthName VARCHAR(15), Week INT, DayOfWeek VARCHAR(10),
  IsWeekend BIT, FiscalYear INT, FiscalQuarter INT
)

-- DimCompany: Company dimension with SCD Type 2 support
CREATE TABLE DimCompany (
  CompanyKey INT PRIMARY KEY IDENTITY(1,1),
  Ticker VARCHAR(10), CompanyName VARCHAR(100),
  Sector VARCHAR(50), Industry VARCHAR(100),
  CEO VARCHAR(100), Founded INT, Headquarters VARCHAR(100),
  Employees INT, MarketCapTier VARCHAR(20),
  EffectiveDate DATE, ExpiryDate DATE, IsCurrent BIT DEFAULT 1
)

-- FactDailyStock: Daily stock prices with foreign keys to dimensions
CREATE TABLE FactDailyStock (
  StockFactKey BIGINT PRIMARY KEY IDENTITY(1,1),
  DateKey INT, CompanyKey INT,
  OpenPrice DECIMAL(12,2), HighPrice DECIMAL(12,2),
  LowPrice DECIMAL(12,2), ClosePrice DECIMAL(12,2),
  Volume BIGINT, MarketCap DECIMAL(18,2),
  DailyReturn DECIMAL(8,6), MovingAvg50 DECIMAL(12,2),
  FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
  FOREIGN KEY (CompanyKey) REFERENCES DimCompany(CompanyKey)
)
```

#### Company Key Lookup (get_company_key)

Retrieves surrogate key for a ticker symbol:

```sql
-- Lookup company surrogate key by ticker (current record only)
SELECT CompanyKey FROM DimCompany WHERE Ticker='AAPL' AND IsCurrent=1
```

#### Duplicate Check Before Insert

Prevents duplicate records in fact tables:

```sql
-- Check for existing stock data before insert
SELECT StockFactKey FROM FactDailyStock WHERE DateKey=20251230 AND CompanyKey=1

-- Check for existing financial data before insert
SELECT FinancialKey FROM FactFinancials WHERE DateKey=20250930 AND CompanyKey=1
```

#### SCD Type 2 Update (update_company_scd2)

Implements slowly changing dimension Type 2 for historical tracking:

```sql
-- Step 1: Expire current record
UPDATE DimCompany SET ExpiryDate=GETDATE(), IsCurrent=0
WHERE Ticker='MSFT' AND IsCurrent=1

-- Step 2: Insert new record with updated field
INSERT INTO DimCompany (Ticker, CompanyName, Sector, Industry, CEO, ...)
SELECT Ticker, CompanyName, Sector, Industry,
  CASE WHEN 'CEO'='CEO' THEN 'New CEO Name' ELSE CEO END, ...
FROM DimCompany WHERE Ticker='MSFT' AND ExpiryDate=CAST(GETDATE() AS DATE)
```

#### Analytics Queries (run_analytics)

Market cap rankings with window function:

```sql
-- Top 50 companies by market cap with ranking
SELECT TOP 50 c.Ticker, c.CompanyName, c.Sector, f.MarketCap/1e12 AS MarketCapT,
  RANK() OVER (ORDER BY f.MarketCap DESC) AS Rank
FROM FactDailyStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
  AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock)
ORDER BY Rank
```

Sector breakdown with aggregation:

```sql
-- Sector-level aggregation
SELECT c.Sector, COUNT(DISTINCT c.Ticker) AS Companies,
  SUM(f.MarketCap)/1e12 AS TotalMarketCapT
FROM FactDailyStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
  AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock)
GROUP BY c.Sector
ORDER BY TotalMarketCapT DESC
```

Financial metrics summary:

```sql
-- Top 20 companies by revenue with financial ratios
SELECT TOP 20 c.Ticker, c.CompanyName,
  ff.Revenue/1e9 AS RevenueB, ff.NetIncome/1e9 AS NetIncomeB,
  ff.GrossMargin, ff.NetMargin, ff.ROE, ff.ROA
FROM FactFinancials ff
JOIN DimCompany c ON ff.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
  AND ff.DateKey = (SELECT MAX(DateKey) FROM FactFinancials)
ORDER BY ff.Revenue DESC
```

## Web Frontend (web)

The `web` application provides an interactive web interface built with the [Wt C++ Web Framework](https://www.webtoolkit.eu/wt).

### Features

- **Dashboard** - Top 10 companies by market cap, sector breakdown
- **Companies** - Company directory with sector filtering
- **Stock Data** - Daily stock prices with company filtering
- **Financials** - Quarterly financial statements with key ratios
- **Sectors** - Sector-level analysis and aggregations

### Usage

```bash
./web -S SERVER -d DATABASE -U USER -P PASSWORD --http-address=0.0.0.0 --http-port=8080 --docroot=.
```

### Options

| Option | Description |
|--------|-------------|
| `-S SERVER` | SQL Server hostname or IP address (required) |
| `-d DATABASE` | Database name (required) |
| `-U USER` | SQL Server username (required on Linux) |
| `-P PASSWORD` | SQL Server password (required on Linux) |
| `--http-address` | HTTP listen address (default: 0.0.0.0) |
| `--http-port` | HTTP port (default: 8080) |
| `--docroot` | Document root directory |

### Example

```bash
./web -S localhost -d data_warehouse -U sa -P password --http-address=0.0.0.0 --http-port=8080 --docroot=.
```

Access the application at `http://localhost:8080`

### SQL Queries in Web Application

The web application uses the following SQL queries for each view:

#### Dashboard View

Sector breakdown for dashboard:

```sql
-- Sector breakdown with total market cap
SELECT c.Sector, COUNT(DISTINCT c.Ticker) AS Companies,
  SUM(f.MarketCap)/1e12 AS TotalMarketCapT
FROM FactDailyStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
  AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock)
GROUP BY c.Sector
ORDER BY TotalMarketCapT DESC
```

All companies listing:

```sql
-- All companies with latest market cap
SELECT c.Ticker, c.CompanyName, c.Sector, c.Industry,
  COALESCE(f.MarketCap/1e9, 0) AS MarketCapB
FROM DimCompany c
LEFT JOIN (SELECT CompanyKey, MarketCap FROM FactDailyStock
  WHERE DateKey = (SELECT MAX(DateKey) FROM FactDailyStock)) f
ON c.CompanyKey = f.CompanyKey
WHERE c.IsCurrent = 1
ORDER BY c.Ticker
```

#### Companies View

Filterable company directory:

```sql
-- Company list with optional sector filter
SELECT Ticker, CompanyName, Sector, Industry, CEO,
  Headquarters, Employees, MarketCapTier
FROM DimCompany
WHERE IsCurrent=1
  AND Sector='Technology'  -- optional filter
ORDER BY Ticker
```

#### Stock Data View

Daily stock prices with joins:

```sql
-- Stock prices with company and date dimensions
SELECT TOP 100 c.Ticker, d.FullDate, f.OpenPrice, f.HighPrice,
  f.LowPrice, f.ClosePrice, f.Volume, f.MarketCap/1e9 AS MarketCapB,
  f.DailyReturn
FROM FactDailyStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
JOIN DimDate d ON f.DateKey = d.DateKey
WHERE c.IsCurrent = 1
  AND c.Ticker='AAPL'  -- optional filter
ORDER BY d.FullDate DESC, c.Ticker
```

#### Financials View

Financial statements with per-company latest date:

```sql
-- Financial metrics with most recent quarter per company
SELECT c.Ticker, c.CompanyName,
  ff.Revenue/1e9 AS RevenueB, ff.NetIncome/1e9 AS NetIncomeB,
  ff.GrossMargin * 100 AS GrossMargin, ff.NetMargin * 100 AS NetMargin,
  ff.ROE * 100 AS ROE, ff.ROA * 100 AS ROA
FROM FactFinancials ff
JOIN DimCompany c ON ff.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
  AND ff.DateKey = (SELECT MAX(ff2.DateKey) FROM FactFinancials ff2
                    WHERE ff2.CompanyKey = ff.CompanyKey)
ORDER BY ff.Revenue DESC
```

#### Sectors View

Sector-level aggregations with averages:

```sql
-- Sector analysis with market cap, revenue, and margin averages
SELECT c.Sector, COUNT(DISTINCT c.Ticker) AS Companies,
  SUM(f.MarketCap)/1e12 AS TotalMarketCapT,
  AVG(ff.Revenue)/1e9 AS AvgRevenueB,
  AVG(ff.GrossMargin) * 100 AS AvgGrossMargin,
  AVG(ff.NetMargin) * 100 AS AvgNetMargin
FROM FactDailyStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
LEFT JOIN FactFinancials ff ON c.CompanyKey = ff.CompanyKey
  AND ff.DateKey = (SELECT MAX(ff2.DateKey) FROM FactFinancials ff2
                    WHERE ff2.CompanyKey = ff.CompanyKey)
WHERE c.IsCurrent = 1
  AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock)
GROUP BY c.Sector
ORDER BY TotalMarketCapT DESC
```

## CSV Data Format

### companies.csv

```csv
Ticker,CompanyName,Sector,Industry,CEO,Founded,Headquarters,Employees,MarketCapTier
AAPL,Apple Inc.,Technology,Consumer Electronics,Tim Cook,1976,"Cupertino, CA",164000,Mega Cap
```

### stock_data.csv

```csv
Ticker,Date,OpenPrice,HighPrice,LowPrice,ClosePrice,Volume,MarketCap,DailyReturn
AAPL,2025-12-20,254.12,257.89,253.45,256.78,45678900,3890000000000,0.0082
```

### financials.csv

```csv
Ticker,QuarterEnd,Revenue,GrossProfit,OperatingIncome,NetIncome,EPS,EBITDA,TotalAssets,TotalLiabilities,CashAndEquivalents,TotalDebt,FreeCashFlow,RnDExpense,GrossMargin,OperatingMargin,NetMargin,ROE,ROA
AAPL,2025-09-30,94930000000,43900000000,29600000000,24300000000,1.64,33200000000,364980000000,290437000000,29943000000,106629000000,26800000000,7800000000,0.4625,0.3118,0.2560,0.1642,0.0666
```

## Top 50 Companies Included

| Sector | Companies |
|--------|-----------|
| Technology | AAPL, MSFT, NVDA, AVGO, ORCL, AMD, ADBE, CSCO, ACN, INTC, IBM, INTU, NOW, TXN |
| Financials | BRK.B, JPM, V, MA, BAC, WFC, GS, AXP, SPGI |
| Healthcare | LLY, UNH, JNJ, ABBV, MRK, TMO, ABT |
| Consumer Discretionary | AMZN, TSLA, HD, MCD |
| Communication Services | META, GOOGL, NFLX, DIS, VZ |
| Consumer Staples | WMT, PG, COST, KO, PEP |
| Energy | XOM, CVX |
| Industrials | GE, CAT |
| Materials | LIN |

## Data Sources - Alpha Vantage REST API

The CSV data files are populated using the [Alpha Vantage](https://www.alphavantage.co) REST API. Alpha Vantage provides free APIs for real-time and historical stock market data.

### API Endpoints Used

| Endpoint | Description | Free |
|----------|-------------|------|
| TIME_SERIES_DAILY | Daily OHLCV prices | ✓ |
| OVERVIEW | Company profile and stats | ✓ |
| INCOME_STATEMENT | Quarterly/annual income | ✓ |
| BALANCE_SHEET | Quarterly/annual balance sheet | ✓ |

## ETL Pipeline Steps

1. **Create Schema** - Creates dimension and fact tables if not exists
2. **Load Date Dimension** - Populates DimDate with calendar data (2020-2026)
3. **Load Companies** - Reads companies.csv into DimCompany
4. **Load Stock Data** - Reads stock_data.csv into FactDailyStock
5. **Load Financials** - Reads financials.csv into FactFinancials
6. **Run Analytics** - Displays market cap rankings and sector breakdown
