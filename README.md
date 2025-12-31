# Top US Companies Data Warehouse

A Kimball-style star schema data warehouse for tracking the top 50 US companies by market capitalization, implemented with C++ and ODBC for Microsoft SQL Server.

## Kimball Star Schema Methodology

This project implements Ralph Kimball's dimensional modeling approach:

**Live Demo:** http://nostro.cloud:9500/

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
| etl | ETL pipeline - loads CSV data into SQL Server |
| web | Wt web application for data visualization |

## Web Frontend

The `web` application provides an interactive web interface built with the [Wt C++ Web Framework](https://www.webtoolkit.eu/wt).

### Features

- **Dashboard** - Top 10 companies by market cap, sector breakdown
- **Companies** - Company directory with sector filtering
- **Stock Data** - Daily stock prices with company filtering
- **Financials** - Quarterly financial statements with key ratios
- **Sectors** - Sector-level analysis and aggregations

### Running the Web Server

```bash
./web -S localhost -d data_warehouse -U sa -P password  --http-address=0.0.0.0 --http-port=8080 --docroot=.
```

Access the application at `http://localhost:8080`

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

## Sample Queries

### Market Cap Ranking

```sql
SELECT c.Ticker, c.CompanyName, f.MarketCap/1e12 AS MarketCapTrillions,
       RANK() OVER (ORDER BY f.MarketCap DESC) AS Rank
FROM FactDailyStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
  AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock)
ORDER BY Rank;
```

### Sector Analysis

```sql
SELECT c.Sector, COUNT(DISTINCT c.Ticker) AS Companies, 
       SUM(f.MarketCap)/1e12 AS TotalMarketCapT
FROM FactDailyStock f
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
GROUP BY c.Sector
ORDER BY TotalMarketCapT DESC;
```

### Financial Performance

```sql
SELECT c.Ticker, c.CompanyName,
       ff.Revenue/1e9 AS RevenueB,
       ff.GrossMargin, ff.NetMargin, ff.ROE, ff.ROA
FROM FactFinancials ff
JOIN DimCompany c ON ff.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
ORDER BY ff.Revenue DESC;
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

## ETL Pipeline

1. **Create Schema** - Creates dimension and fact tables if not exists
2. **Load Date Dimension** - Populates DimDate with calendar data (2020-2026)
3. **Load Companies** - Reads companies.csv into DimCompany
4. **Load Stock Data** - Reads stock_data.csv into FactDailyStock
5. **Load Financials** - Reads financials.csv into FactFinancials
6. **Run Analytics** - Displays market cap rankings and sector breakdown
