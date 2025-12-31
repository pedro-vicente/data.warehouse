-- ============================================
-- DATABASE CREATION
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'data_warehouse')
BEGIN
    CREATE DATABASE data_warehouse;
END
GO

USE data_warehouse;
GO

-- ============================================
-- DIMENSION TABLES
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimDate')
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,              -- YYYYMMDD
    FullDate DATE NOT NULL,
    Year INT,
    Quarter INT,
    Month INT,
    MonthName VARCHAR(15),
    Week INT,
    DayOfWeek VARCHAR(10),
    IsWeekend BIT,
    IsHoliday BIT,
    FiscalYear INT,
    FiscalQuarter INT
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimCompany')
CREATE TABLE DimCompany (
    CompanyKey INT PRIMARY KEY IDENTITY(1,1),
    Ticker VARCHAR(10) NOT NULL,
    CompanyName VARCHAR(100),
    Sector VARCHAR(50),
    Industry VARCHAR(100),
    CEO VARCHAR(100),
    Founded INT,
    Headquarters VARCHAR(100),
    Employees INT,
    MarketCapTier VARCHAR(20),
    Description VARCHAR(500),
    -- SCD Type 2 fields
    EffectiveDate DATE,
    ExpiryDate DATE,
    IsCurrent BIT DEFAULT 1
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimSector')
CREATE TABLE DimSector (
    SectorKey INT PRIMARY KEY IDENTITY(1,1),
    SectorName VARCHAR(50),
    SectorDescription VARCHAR(200)
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimExchange')
CREATE TABLE DimExchange (
    ExchangeKey INT PRIMARY KEY IDENTITY(1,1),
    ExchangeCode VARCHAR(10),
    ExchangeName VARCHAR(50),
    Country VARCHAR(50),
    Timezone VARCHAR(50)
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimMetricType')
CREATE TABLE DimMetricType (
    MetricTypeKey INT PRIMARY KEY IDENTITY(1,1),
    MetricCode VARCHAR(30),
    MetricName VARCHAR(100),
    MetricCategory VARCHAR(50),
    Unit VARCHAR(20)
);

-- ============================================
-- FACT TABLES
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='FactDailyStock')
CREATE TABLE FactDailyStock (
    StockFactKey BIGINT PRIMARY KEY IDENTITY(1,1),
    DateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    CompanyKey INT FOREIGN KEY REFERENCES DimCompany(CompanyKey),
    OpenPrice DECIMAL(12,2),
    HighPrice DECIMAL(12,2),
    LowPrice DECIMAL(12,2),
    ClosePrice DECIMAL(12,2),
    AdjustedClose DECIMAL(12,2),
    Volume BIGINT,
    MarketCap DECIMAL(18,2),
    SharesOutstanding BIGINT,
    DailyReturn DECIMAL(8,6),
    MovingAvg50 DECIMAL(12,2),
    MovingAvg200 DECIMAL(12,2),
    RSI DECIMAL(6,2)
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='FactFinancials')
CREATE TABLE FactFinancials (
    FinancialKey BIGINT PRIMARY KEY IDENTITY(1,1),
    DateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    CompanyKey INT FOREIGN KEY REFERENCES DimCompany(CompanyKey),
    Revenue DECIMAL(18,2),
    GrossProfit DECIMAL(18,2),
    OperatingIncome DECIMAL(18,2),
    NetIncome DECIMAL(18,2),
    EPS DECIMAL(10,4),
    EBITDA DECIMAL(18,2),
    TotalAssets DECIMAL(18,2),
    TotalLiabilities DECIMAL(18,2),
    TotalEquity DECIMAL(18,2),
    CashAndEquivalents DECIMAL(18,2),
    TotalDebt DECIMAL(18,2),
    OperatingCashFlow DECIMAL(18,2),
    CapEx DECIMAL(18,2),
    FreeCashFlow DECIMAL(18,2),
    DividendsPaid DECIMAL(18,2),
    RnDExpense DECIMAL(18,2),
    GrossMargin DECIMAL(8,4),
    OperatingMargin DECIMAL(8,4),
    NetMargin DECIMAL(8,4),
    ROE DECIMAL(8,4),
    ROA DECIMAL(8,4)
);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='FactValuation')
CREATE TABLE FactValuation (
    ValuationKey BIGINT PRIMARY KEY IDENTITY(1,1),
    DateKey INT FOREIGN KEY REFERENCES DimDate(DateKey),
    CompanyKey INT FOREIGN KEY REFERENCES DimCompany(CompanyKey),
    PERatio DECIMAL(10,2),
    ForwardPE DECIMAL(10,2),
    PEGRatio DECIMAL(10,4),
    PriceToSales DECIMAL(10,2),
    PriceToBook DECIMAL(10,2),
    EVToEBITDA DECIMAL(10,2),
    EVToRevenue DECIMAL(10,2),
    DividendYield DECIMAL(8,4),
    Beta DECIMAL(6,4),
    ShortRatio DECIMAL(8,2)
);

-- ============================================
-- SEED DATA
-- ============================================

INSERT INTO DimSector (SectorName, SectorDescription) VALUES
('Technology', 'Hardware, software, semiconductors, cloud computing'),
('Consumer Discretionary', 'Retail, automotive, e-commerce'),
('Communication Services', 'Internet services, media, telecom'),
('Financials', 'Insurance, banking, investments');

INSERT INTO DimExchange (ExchangeCode, ExchangeName, Country, Timezone) VALUES
('NASDAQ', 'NASDAQ Stock Market', 'USA', 'America/New_York'),
('NYSE', 'New York Stock Exchange', 'USA', 'America/New_York');

INSERT INTO DimCompany (Ticker, CompanyName, Sector, Industry, CEO, Founded, Headquarters, Employees, MarketCapTier, EffectiveDate) VALUES
('NVDA', 'NVIDIA Corporation', 'Technology', 'Semiconductors', 'Jensen Huang', 1993, 'Santa Clara, CA', 29600, 'Mega Cap', '2025-01-01'),
('AAPL', 'Apple Inc.', 'Technology', 'Consumer Electronics', 'Tim Cook', 1976, 'Cupertino, CA', 164000, 'Mega Cap', '2025-01-01'),
('GOOGL', 'Alphabet Inc.', 'Communication Services', 'Internet Services', 'Sundar Pichai', 1998, 'Mountain View, CA', 182502, 'Mega Cap', '2025-01-01'),
('MSFT', 'Microsoft Corporation', 'Technology', 'Software/Cloud', 'Satya Nadella', 1975, 'Redmond, WA', 228000, 'Mega Cap', '2025-01-01'),
('AMZN', 'Amazon.com Inc.', 'Consumer Discretionary', 'E-commerce/Cloud', 'Andy Jassy', 1994, 'Seattle, WA', 1540000, 'Mega Cap', '2025-01-01'),
('META', 'Meta Platforms Inc.', 'Communication Services', 'Social Media', 'Mark Zuckerberg', 2004, 'Menlo Park, CA', 72404, 'Mega Cap', '2025-01-01'),
('TSLA', 'Tesla Inc.', 'Consumer Discretionary', 'Electric Vehicles', 'Elon Musk', 2003, 'Austin, TX', 140473, 'Mega Cap', '2025-01-01'),
('BRK.B', 'Berkshire Hathaway', 'Financials', 'Conglomerate', 'Warren Buffett', 1839, 'Omaha, NE', 396500, 'Mega Cap', '2025-01-01'),
('AVGO', 'Broadcom Inc.', 'Technology', 'Semiconductors', 'Hock Tan', 1961, 'Palo Alto, CA', 37000, 'Mega Cap', '2025-01-01'),
('WMT', 'Walmart Inc.', 'Consumer Discretionary', 'Retail', 'Doug McMillon', 1962, 'Bentonville, AR', 2100000, 'Mega Cap', '2025-01-01');

-- ============================================
-- SAMPLE ANALYTICAL QUERIES
-- ============================================

-- Market cap ranking over time
SELECT 
    d.FullDate,
    c.Ticker,
    c.CompanyName,
    f.MarketCap / 1e12 AS MarketCapTrillions,
    RANK() OVER (PARTITION BY d.FullDate ORDER BY f.MarketCap DESC) AS Ranking
FROM FactDailyStock f
JOIN DimDate d ON f.DateKey = d.DateKey
JOIN DimCompany c ON f.CompanyKey = c.CompanyKey
WHERE c.IsCurrent = 1
ORDER BY d.FullDate DESC, Ranking;

-- Sector performance comparison
SELECT 
    c.Sector,
    d.Year,
    d.Quarter,
    SUM(q.Revenue) / 1e9 AS TotalRevenueBillions,
    AVG(v.PERatio) AS AvgPE,
    SUM(q.NetIncome) / 1e9 AS TotalNetIncomeBillions
FROM FactFinancials q
JOIN DimCompany c ON q.CompanyKey = c.CompanyKey
JOIN DimDate d ON q.DateKey = d.DateKey
JOIN FactValuation v ON q.CompanyKey = v.CompanyKey AND q.DateKey = v.DateKey
WHERE c.IsCurrent = 1
GROUP BY c.Sector, d.Year, d.Quarter
ORDER BY d.Year DESC, d.Quarter DESC;

-- YoY revenue growth by company
WITH RevenueGrowth AS (
    SELECT 
        c.Ticker,
        d.Year,
        SUM(q.Revenue) AS AnnualRevenue,
        LAG(SUM(q.Revenue)) OVER (PARTITION BY c.Ticker ORDER BY d.Year) AS PriorYearRevenue
    FROM FactFinancials q
    JOIN DimCompany c ON q.CompanyKey = c.CompanyKey
    JOIN DimDate d ON q.DateKey = d.DateKey
    GROUP BY c.Ticker, d.Year
)
SELECT 
    Ticker,
    Year,
    AnnualRevenue / 1e9 AS RevenueBillions,
    ROUND((AnnualRevenue - PriorYearRevenue) / PriorYearRevenue * 100, 2) AS YoYGrowthPct
FROM RevenueGrowth
WHERE PriorYearRevenue IS NOT NULL
ORDER BY Year DESC, YoYGrowthPct DESC;
