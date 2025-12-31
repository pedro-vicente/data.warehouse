# Top US Companies Data Warehouse

A Kimball-style star schema data warehouse for tracking the top 50 US companies by market capitalization, implemented with C++ and ODBC for Microsoft SQL Server.

## Kimball Star Schema Methodology

This project implements Ralph Kimball's dimensional modeling approach:

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

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Fact Tables** | Numeric measurements (prices, volume, revenue) with foreign keys |
| **Dimension Tables** | Descriptive attributes (company name, sector, dates) |
| **Surrogate Keys** | System-generated keys (CompanyKey) vs natural keys (Ticker) |
| **Star Schema** | Denormalized dimensions for query performance |
| **Conformed Dimensions** | Shared dimensions across multiple fact tables |

### SCD Type 2 (Slowly Changing Dimensions)

DimCompany tracks historical changes by expiring old records and inserting new ones:

```
CompanyKey | Ticker | CEO           | EffectiveDate | ExpiryDate | IsCurrent
-----------+--------+---------------+---------------+------------+----------
1          | MSFT   | Satya Nadella | 2020-01-01    | 2025-06-01 | 0
2          | MSFT   | New CEO       | 2025-06-01    | NULL       | 1
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

## Table Definitions

### DimDate

| Column | Type | Description |
|--------|------|-------------|
| DateKey | INT | Primary key (YYYYMMDD) |
| FullDate | DATE | Full date value |
| Year | INT | Calendar year |
| Quarter | INT | Calendar quarter (1-4) |
| Month | INT | Month number (1-12) |
| MonthName | VARCHAR(15) | Month name |
| Week | INT | Week of year |
| DayOfWeek | VARCHAR(10) | Day name |
| IsWeekend | BIT | Weekend flag |
| FiscalYear | INT | Federal fiscal year (Oct-Sep) |
| FiscalQuarter | INT | Federal fiscal quarter |

### DimCompany

| Column | Type | Description |
|--------|------|-------------|
| CompanyKey | INT IDENTITY | Surrogate primary key |
| Ticker | VARCHAR(10) | Stock ticker symbol |
| CompanyName | VARCHAR(100) | Full company name |
| Sector | VARCHAR(50) | Industry sector |
| Industry | VARCHAR(100) | Industry classification |
| CEO | VARCHAR(100) | Chief Executive Officer |
| Founded | INT | Year founded |
| Headquarters | VARCHAR(100) | HQ location |
| Employees | INT | Employee count |
| MarketCapTier | VARCHAR(20) | Mega Cap, Large Cap |
| EffectiveDate | DATE | SCD Type 2 effective date |
| ExpiryDate | DATE | SCD Type 2 expiry date |
| IsCurrent | BIT | Current record flag |

### DimSector

| Column | Type | Description |
|--------|------|-------------|
| SectorKey | INT IDENTITY | Surrogate primary key |
| SectorName | VARCHAR(50) | Sector name |
| SectorDescription | VARCHAR(200) | Sector description |

### FactDailyStock

| Column | Type | Description |
|--------|------|-------------|
| StockFactKey | BIGINT IDENTITY | Surrogate primary key |
| DateKey | INT | FK to DimDate |
| CompanyKey | INT | FK to DimCompany |
| OpenPrice | DECIMAL(12,2) | Opening price |
| HighPrice | DECIMAL(12,2) | Daily high |
| LowPrice | DECIMAL(12,2) | Daily low |
| ClosePrice | DECIMAL(12,2) | Closing price |
| Volume | BIGINT | Trading volume |
| MarketCap | DECIMAL(18,2) | Market capitalization |
| DailyReturn | DECIMAL(8,6) | Daily return percentage |
| MovingAvg50 | DECIMAL(12,2) | 50-day moving average |
| MovingAvg200 | DECIMAL(12,2) | 200-day moving average |
| RSI | DECIMAL(6,2) | Relative Strength Index |

### FactFinancials

| Column | Type | Description |
|--------|------|-------------|
| FinancialKey | BIGINT IDENTITY | Surrogate primary key |
| DateKey | INT | FK to DimDate |
| CompanyKey | INT | FK to DimCompany |
| Revenue | DECIMAL(18,2) | Total revenue |
| GrossProfit | DECIMAL(18,2) | Gross profit |
| OperatingIncome | DECIMAL(18,2) | Operating income |
| NetIncome | DECIMAL(18,2) | Net income |
| EPS | DECIMAL(10,4) | Earnings per share |
| EBITDA | DECIMAL(18,2) | EBITDA |
| TotalAssets | DECIMAL(18,2) | Total assets |
| TotalLiabilities | DECIMAL(18,2) | Total liabilities |
| CashAndEquivalents | DECIMAL(18,2) | Cash and equivalents |
| TotalDebt | DECIMAL(18,2) | Total debt |
| FreeCashFlow | DECIMAL(18,2) | Free cash flow |
| RnDExpense | DECIMAL(18,2) | R&D expense |
| GrossMargin | DECIMAL(8,4) | Gross margin ratio |
| OperatingMargin | DECIMAL(8,4) | Operating margin ratio |
| NetMargin | DECIMAL(8,4) | Net margin ratio |
| ROE | DECIMAL(8,4) | Return on equity |
| ROA | DECIMAL(8,4) | Return on assets |

### FactValuation

| Column | Type | Description |
|--------|------|-------------|
| ValuationKey | BIGINT IDENTITY | Surrogate primary key |
| DateKey | INT | FK to DimDate |
| CompanyKey | INT | FK to DimCompany |
| PERatio | DECIMAL(10,2) | Price-to-earnings |
| ForwardPE | DECIMAL(10,2) | Forward P/E |
| PEGRatio | DECIMAL(10,4) | PEG ratio |
| PriceToSales | DECIMAL(10,2) | Price-to-sales |
| PriceToBook | DECIMAL(10,2) | Price-to-book |
| EVToEBITDA | DECIMAL(10,2) | EV/EBITDA |
| EVToRevenue | DECIMAL(10,2) | EV/Revenue |
| DividendYield | DECIMAL(8,4) | Dividend yield |
| Beta | DECIMAL(6,4) | Stock beta |
| ShortRatio | DECIMAL(8,2) | Short interest ratio |


## Build Instructions

```bash
./build.cmake.sh
```

### Prerequisites

- CMake 3.28 or higher
- C++17 compatible compiler (GCC 13, Clang, MSVC 2022)
- Boost 1.88 libraries
- OpenSSL 3.0+
- ODBC driver (unixODBC on Linux, built-in on Windows/macOS)

### Install Boost (Linux)

On Ubuntu/Debian:
```
sudo apt install libboost-all-dev
```

On Fedora/RHEL:

```
sudo dnf install boost-devel
```

On Windows, build from source 
```batch
build.boost.bat
```


### Instal ODBC drivers (Linux)

# Install unixODBC driver manager
```
sudo apt update
sudo apt install unixodbc unixodbc-dev
```

# For SQL Server
```
sudo apt install odbcinst
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt update
sudo ACCEPT_EULA=Y apt install msodbcsql18
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
./finmart_web --http-address=0.0.0.0 --http-port=8080 --docroot=.
```

Access the application at `http://localhost:8080`

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| FINMART_SERVER | SQL Server hostname | localhost |
| FINMART_DATABASE | Database name | data_warehouse |

### Dependencies

| Library | Version | Description |
|---------|---------|-------------|
| Wt | 4.12.1 | C++ web framework |
| Boost | 1.88.0 | C++ libraries (filesystem, thread, program_options, chrono) |
| ODBC | - | Database connectivity |

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

### SCD Type 2 History Query

```sql
SELECT Ticker, CEO, EffectiveDate, ExpiryDate, IsCurrent
FROM DimCompany
WHERE Ticker = 'MSFT'
ORDER BY EffectiveDate;
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
