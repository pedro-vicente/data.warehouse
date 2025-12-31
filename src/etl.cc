#include "odbc.hh"
#include "csv.hh"
#include <iostream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <cstring>
#include <cstdlib>

/////////////////////////////////////////////////////////////////////////////////////////////////////
//usage
// same syntax as sqlcmd
/////////////////////////////////////////////////////////////////////////////////////////////////////

void usage(const char* program_name)
{
  std::cout << "Usage: " << program_name << " [OPTIONS]" << std::endl;
  std::cout << std::endl;
  std::cout << "Required options:" << std::endl;
  std::cout << "  -S SERVER     SQL Server hostname or IP address" << std::endl;
  std::cout << "  -d DATABASE   Database name" << std::endl;
  std::cout << std::endl;
  std::cout << "Optional options:" << std::endl;
  std::cout << "  -U USER       SQL Server username (omit for trusted connection)" << std::endl;
  std::cout << "  -P PASSWORD   SQL Server password" << std::endl;
  std::cout << std::endl;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t
//ETL for US Companies Data Warehouse using Kimball star schema
/////////////////////////////////////////////////////////////////////////////////////////////////////

class etl_t
{
public:
  etl_t();
  ~etl_t();
  int connect(const std::string& server, const std::string& database,
    const std::string& user = std::string(), const std::string& password = std::string());
  int disconnect();
  int create_schema();
  int load_date_dimension(int start_year, int end_year);
  int load_companies_from_csv(const std::string& filename);
  int load_stock_data_from_csv(const std::string& filename);
  int load_financials_from_csv(const std::string& filename);
  int update_company_scd2(const std::string& ticker, const std::string& field, const std::string& new_value);
  int run_analytics();

private:
  odbc_t odbc;
  int get_company_key(const std::string& ticker);
  int get_date_key(const std::string& date_str);
  std::string escape_sql(const std::string& str);
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
//main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
  std::string server;
  std::string database;
  std::string user;
  std::string password;

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //parse command line
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  for (int idx = 1; idx < argc; idx++)
  {
    std::string arg = argv[idx];
    if (arg == "-h" || arg == "--help")
    {
      usage(argv[0]);
      return 0;
    }
    else if (arg == "-S" && idx + 1 < argc)
    {
      server = argv[++idx];
    }
    else if (arg == "-d" && idx + 1 < argc)
    {
      database = argv[++idx];
    }
    else if (arg == "-U" && idx + 1 < argc)
    {
      user = argv[++idx];
    }
    else if (arg == "-P" && idx + 1 < argc)
    {
      password = argv[++idx];
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //validate required parameters
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (server.empty())
  {
    usage(argv[0]);
    return 1;
  }

  if (database.empty())
  {
    usage(argv[0]);
    return 1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //display configuration
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::cout << "ETL Configuration:" << std::endl;
  std::cout << "  Server:   " << server << std::endl;
  std::cout << "  Database: " << database << std::endl;
  std::cout << "  User:     " << (user.empty() ? "(trusted connection)" : user) << std::endl;
  std::cout << std::endl;

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //connect
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  etl_t etl;

  if (etl.connect(server, database, user, password) < 0)
  {
    return 1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //create schema
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (etl.create_schema() < 0)
  {
    etl.disconnect();
    return 1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //load date dimension
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (etl.load_date_dimension(2020, 2026) < 0)
  {
    etl.disconnect();
    return 1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //load from CSV files
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string companies_file = "companies.csv";
  std::string stock_file = "stock_data.csv";
  std::string financials_file = "financials.csv";

  if (etl.load_companies_from_csv(companies_file) < 0)
  {
    etl.disconnect();
    return 1;
  }

  if (etl.load_stock_data_from_csv(stock_file) < 0)
  {
    etl.disconnect();
    return 1;
  }

  if (etl.load_financials_from_csv(financials_file) < 0)
  {
    etl.disconnect();
    return 1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //run analytics
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (etl.run_analytics() < 0)
  {
    etl.disconnect();
    return 1;
  }

  etl.disconnect();
  return 0;
}



/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::etl_t
/////////////////////////////////////////////////////////////////////////////////////////////////////

etl_t::etl_t()
{
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::~etl_t
/////////////////////////////////////////////////////////////////////////////////////////////////////

etl_t::~etl_t()
{
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::connect
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::connect(const std::string& server, const std::string& database,
  const std::string& user, const std::string& password)
{
  std::string conn = make_conn(server, database, user, password);
  return odbc.connect(conn);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::disconnect
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::disconnect()
{
  return odbc.disconnect();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::escape_sql
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string etl_t::escape_sql(const std::string& str)
{
  std::string result;
  for (size_t idx = 0; idx < str.size(); idx++)
  {
    if (str[idx] == '\'')
    {
      result += "''";
    }
    else
    {
      result += str[idx];
    }
  }
  return result;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::get_company_key
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::get_company_key(const std::string& ticker)
{
  std::stringstream sql;
  sql << "SELECT CompanyKey FROM DimCompany WHERE Ticker='" << ticker << "' AND IsCurrent=1";

  table_t table;
  if (odbc.fetch(sql.str(), table) < 0)
  {
    return -1;
  }

  if (table.rows.size() > 0)
  {
    std::string key_str = table.get_row_col_value(0, "CompanyKey");
    return atoi(key_str.c_str());
  }
  return -1;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::get_date_key
//converts date string YYYY-MM-DD to date key YYYYMMDD
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::get_date_key(const std::string& date_str)
{
  int year = 0;
  int month = 0;
  int day = 0;

  if (sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day) == 3)
  {
    return year * 10000 + month * 100 + day;
  }
  return -1;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::create_schema
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::create_schema()
{
  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //DimDate
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_dim_date =
    "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimDate') "
    "CREATE TABLE DimDate ("
    "DateKey INT PRIMARY KEY, "
    "FullDate DATE, Year INT, Quarter INT, Month INT, "
    "MonthName VARCHAR(15), Week INT, DayOfWeek VARCHAR(10), "
    "IsWeekend BIT, FiscalYear INT, FiscalQuarter INT)";

  if (odbc.exec_direct(sql_dim_date) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //DimCompany (SCD Type 2)
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_dim_company =
    "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimCompany') "
    "CREATE TABLE DimCompany ("
    "CompanyKey INT PRIMARY KEY IDENTITY(1,1), "
    "Ticker VARCHAR(10), CompanyName VARCHAR(100), "
    "Sector VARCHAR(50), Industry VARCHAR(100), "
    "CEO VARCHAR(100), Founded INT, Headquarters VARCHAR(100), "
    "Employees INT, MarketCapTier VARCHAR(20), "
    "EffectiveDate DATE, ExpiryDate DATE, IsCurrent BIT DEFAULT 1)";

  if (odbc.exec_direct(sql_dim_company) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //DimSector
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_dim_sector =
    "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='DimSector') "
    "CREATE TABLE DimSector ("
    "SectorKey INT PRIMARY KEY IDENTITY(1,1), "
    "SectorName VARCHAR(50), SectorDescription VARCHAR(200))";

  if (odbc.exec_direct(sql_dim_sector) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //FactDailyStock
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_fact_stock =
    "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='FactDailyStock') "
    "CREATE TABLE FactDailyStock ("
    "StockFactKey BIGINT PRIMARY KEY IDENTITY(1,1), "
    "DateKey INT, CompanyKey INT, "
    "OpenPrice DECIMAL(12,2), HighPrice DECIMAL(12,2), "
    "LowPrice DECIMAL(12,2), ClosePrice DECIMAL(12,2), "
    "Volume BIGINT, MarketCap DECIMAL(18,2), "
    "DailyReturn DECIMAL(8,6), MovingAvg50 DECIMAL(12,2), "
    "MovingAvg200 DECIMAL(12,2), RSI DECIMAL(6,2), "
    "FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey), "
    "FOREIGN KEY (CompanyKey) REFERENCES DimCompany(CompanyKey))";

  if (odbc.exec_direct(sql_fact_stock) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //FactFinancials
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_fact_fin =
    "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='FactFinancials') "
    "CREATE TABLE FactFinancials ("
    "FinancialKey BIGINT PRIMARY KEY IDENTITY(1,1), "
    "DateKey INT, CompanyKey INT, "
    "Revenue DECIMAL(18,2), GrossProfit DECIMAL(18,2), "
    "OperatingIncome DECIMAL(18,2), NetIncome DECIMAL(18,2), "
    "EPS DECIMAL(10,4), EBITDA DECIMAL(18,2), "
    "TotalAssets DECIMAL(18,2), TotalLiabilities DECIMAL(18,2), "
    "CashAndEquivalents DECIMAL(18,2), TotalDebt DECIMAL(18,2), "
    "FreeCashFlow DECIMAL(18,2), RnDExpense DECIMAL(18,2), "
    "GrossMargin DECIMAL(8,4), OperatingMargin DECIMAL(8,4), "
    "NetMargin DECIMAL(8,4), ROE DECIMAL(8,4), ROA DECIMAL(8,4), "
    "FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey), "
    "FOREIGN KEY (CompanyKey) REFERENCES DimCompany(CompanyKey))";

  if (odbc.exec_direct(sql_fact_fin) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //FactValuation
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_fact_val =
    "IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='FactValuation') "
    "CREATE TABLE FactValuation ("
    "ValuationKey BIGINT PRIMARY KEY IDENTITY(1,1), "
    "DateKey INT, CompanyKey INT, "
    "PERatio DECIMAL(10,2), ForwardPE DECIMAL(10,2), "
    "PEGRatio DECIMAL(10,4), PriceToSales DECIMAL(10,2), "
    "PriceToBook DECIMAL(10,2), EVToEBITDA DECIMAL(10,2), "
    "EVToRevenue DECIMAL(10,2), DividendYield DECIMAL(8,4), "
    "Beta DECIMAL(6,4), ShortRatio DECIMAL(8,2), "
    "FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey), "
    "FOREIGN KEY (CompanyKey) REFERENCES DimCompany(CompanyKey))";

  if (odbc.exec_direct(sql_fact_val) < 0)
  {
    return -1;
  }

  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::load_date_dimension
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::load_date_dimension(int start_year, int end_year)
{
  const char* months[] = { "", "January", "February", "March", "April", "May", "June",
                           "July", "August", "September", "October", "November", "December" };
  const char* days[] = { "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };

  int count = 0;
  for (int y = start_year; y <= end_year; y++)
  {
    for (int m = 1; m <= 12; m++)
    {
      int days_in_month = (m == 2) ? ((y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 29 : 28) :
        ((m == 4 || m == 6 || m == 9 || m == 11) ? 30 : 31);

      for (int d = 1; d <= days_in_month; d++)
      {
        int date_key = y * 10000 + m * 100 + d;

        struct tm t;
        memset(&t, 0, sizeof(t));
        t.tm_mday = d;
        t.tm_mon = m - 1;
        t.tm_year = y - 1900;
        mktime(&t);
        int dow = t.tm_wday;
        int week = (t.tm_yday / 7) + 1;
        int quarter = (m - 1) / 3 + 1;
        int fiscal_q = ((m + 5) / 3) % 4 + 1;
        int fiscal_y = (m >= 7) ? y + 1 : y;
        int is_weekend = (dow == 0 || dow == 6) ? 1 : 0;

        std::stringstream sql;
        sql << "IF NOT EXISTS (SELECT 1 FROM DimDate WHERE DateKey=" << date_key << ") "
          << "INSERT INTO DimDate (DateKey, FullDate, Year, Quarter, Month, MonthName, Week, DayOfWeek, IsWeekend, FiscalYear, FiscalQuarter) "
          << "VALUES (" << date_key << ", '"
          << std::setfill('0') << std::setw(4) << y << "-"
          << std::setw(2) << m << "-"
          << std::setw(2) << d << "', "
          << y << ", " << quarter << ", " << m << ", '" << months[m] << "', "
          << week << ", '" << days[dow] << "', " << is_weekend << ", "
          << fiscal_y << ", " << fiscal_q << ")";

        if (odbc.exec_direct(sql.str()) < 0)
        {
        }
        else
        {
          count++;
        }
      }
    }
  }

  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::load_companies_from_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::load_companies_from_csv(const std::string& filename)
{
  read_csv_t reader;

  if (reader.open(filename) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //skip header row
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<std::string> row = reader.read_row_by_comma();
  if (row.empty())
  {
    reader.close();
    return -1;
  }

  int count = 0;
  int errors = 0;

  while (true)
  {
    row = reader.read_row_by_comma();
    if (row.empty())
    {
      break;
    }

    if (row.size() < 9)
    {
      continue;
    }

    std::string ticker = row[0];
    std::string company_name = escape_sql(row[1]);
    std::string sector = escape_sql(row[2]);
    std::string industry = escape_sql(row[3]);
    std::string ceo = escape_sql(row[4]);
    std::string founded = row[5];
    std::string headquarters = escape_sql(row[6]);
    std::string employees = row[7];
    std::string market_cap_tier = row[8];

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //check if company already exists
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    std::stringstream check_sql;
    check_sql << "SELECT CompanyKey FROM DimCompany WHERE Ticker='" << ticker << "' AND IsCurrent=1";

    table_t table;
    if (odbc.fetch(check_sql.str(), table) < 0)
    {
      errors++;
      continue;
    }

    if (table.rows.size() > 0)
    {
      continue;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //insert new company
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    std::stringstream sql;
    sql << "INSERT INTO DimCompany (Ticker, CompanyName, Sector, Industry, CEO, Founded, Headquarters, Employees, MarketCapTier, EffectiveDate, IsCurrent) "
      << "VALUES ('" << ticker << "', '" << company_name << "', '" << sector << "', '" << industry << "', '"
      << ceo << "', " << founded << ", '" << headquarters << "', " << employees << ", '" << market_cap_tier << "', GETDATE(), 1)";

    if (odbc.exec_direct(sql.str()) < 0)
    {
      errors++;
    }
    else
    {
      count++;
    }
  }

  reader.close();
  std::cout << "Loaded " << count << " companies (" << errors << " errors)" << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::load_stock_data_from_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::load_stock_data_from_csv(const std::string& filename)
{
  read_csv_t reader;

  if (reader.open(filename) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //skip header row
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<std::string> row = reader.read_row_by_comma();
  if (row.empty())
  {
    reader.close();
    return -1;
  }

  int count = 0;
  int errors = 0;

  while (true)
  {
    row = reader.read_row_by_comma();
    if (row.empty())
    {
      break;
    }

    if (row.size() < 9)
    {
      continue;
    }

    std::string ticker = row[0];
    std::string date_str = row[1];
    std::string open_price = row[2];
    std::string high_price = row[3];
    std::string low_price = row[4];
    std::string close_price = row[5];
    std::string volume = row[6];
    std::string market_cap = row[7];
    std::string daily_return = row[8];

    int company_key = get_company_key(ticker);
    if (company_key < 0)
    {
      errors++;
      continue;
    }

    int date_key = get_date_key(date_str);
    if (date_key < 0)
    {
      errors++;
      continue;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //check for duplicate
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    std::stringstream check_sql;
    check_sql << "SELECT StockFactKey FROM FactDailyStock WHERE DateKey=" << date_key << " AND CompanyKey=" << company_key;

    table_t table;
    if (odbc.fetch(check_sql.str(), table) < 0)
    {
      errors++;
      continue;
    }

    if (table.rows.size() > 0)
    {
      continue;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //insert stock data
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    std::stringstream sql;
    sql << "INSERT INTO FactDailyStock (DateKey, CompanyKey, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, MarketCap, DailyReturn) "
      << "VALUES (" << date_key << ", " << company_key << ", " << open_price << ", " << high_price << ", "
      << low_price << ", " << close_price << ", " << volume << ", " << market_cap << ", " << daily_return << ")";

    if (odbc.exec_direct(sql.str()) < 0)
    {
      errors++;
    }
    else
    {
      count++;
    }
  }

  reader.close();
  std::cout << "Loaded " << count << " stock records (" << errors << " errors)" << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::load_financials_from_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::load_financials_from_csv(const std::string& filename)
{
  read_csv_t reader;

  if (reader.open(filename) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //skip header row
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<std::string> row = reader.read_row_by_comma();
  if (row.empty())
  {
    reader.close();
    return -1;
  }

  int count = 0;
  int errors = 0;

  while (true)
  {
    row = reader.read_row_by_comma();
    if (row.empty())
    {
      break;
    }

    if (row.size() < 19)
    {
      continue;
    }

    std::string ticker = row[0];
    std::string quarter_end = row[1];
    std::string revenue = row[2];
    std::string gross_profit = row[3];
    std::string operating_income = row[4];
    std::string net_income = row[5];
    std::string eps = row[6];
    std::string ebitda = row[7];
    std::string total_assets = row[8];
    std::string total_liabilities = row[9];
    std::string cash_equiv = row[10];
    std::string total_debt = row[11];
    std::string free_cash_flow = row[12];
    std::string rnd_expense = row[13];
    std::string gross_margin = row[14];
    std::string operating_margin = row[15];
    std::string net_margin = row[16];
    std::string roe = row[17];
    std::string roa = row[18];

    int company_key = get_company_key(ticker);
    if (company_key < 0)
    {
      errors++;
      continue;
    }

    int date_key = get_date_key(quarter_end);
    if (date_key < 0)
    {
      errors++;
      continue;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //check for duplicate
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    std::stringstream check_sql;
    check_sql << "SELECT FinancialKey FROM FactFinancials WHERE DateKey=" << date_key << " AND CompanyKey=" << company_key;

    table_t table;
    if (odbc.fetch(check_sql.str(), table) < 0)
    {
      errors++;
      continue;
    }

    if (table.rows.size() > 0)
    {
      continue;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    //insert financials
    /////////////////////////////////////////////////////////////////////////////////////////////////////

    std::stringstream sql;
    sql << "INSERT INTO FactFinancials (DateKey, CompanyKey, Revenue, GrossProfit, OperatingIncome, NetIncome, "
      << "EPS, EBITDA, TotalAssets, TotalLiabilities, CashAndEquivalents, TotalDebt, FreeCashFlow, RnDExpense, "
      << "GrossMargin, OperatingMargin, NetMargin, ROE, ROA) "
      << "VALUES (" << date_key << ", " << company_key << ", "
      << revenue << ", " << gross_profit << ", " << operating_income << ", " << net_income << ", "
      << eps << ", " << ebitda << ", " << total_assets << ", " << total_liabilities << ", "
      << cash_equiv << ", " << total_debt << ", " << free_cash_flow << ", " << rnd_expense << ", "
      << gross_margin << ", " << operating_margin << ", " << net_margin << ", " << roe << ", " << roa << ")";

    if (odbc.exec_direct(sql.str()) < 0)
    {
      errors++;
    }
    else
    {
      count++;
    }
  }

  reader.close();
  std::cout << "Loaded " << count << " financial records (" << errors << " errors)" << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::update_company_scd2
//SCD Type 2: Expire current record and insert new one with updated field
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::update_company_scd2(const std::string& ticker, const std::string& field, const std::string& new_value)
{
  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //expire current record
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::stringstream sql_expire;
  sql_expire << "UPDATE DimCompany SET ExpiryDate=GETDATE(), IsCurrent=0 "
    << "WHERE Ticker='" << ticker << "' AND IsCurrent=1";

  if (odbc.exec_direct(sql_expire.str()) < 0)
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //insert new record with updated field
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::stringstream sql_insert;
  sql_insert << "INSERT INTO DimCompany (Ticker, CompanyName, Sector, Industry, CEO, Founded, Headquarters, Employees, MarketCapTier, EffectiveDate) "
    << "SELECT Ticker, CompanyName, Sector, Industry, "
    << "CASE WHEN '" << field << "'='CEO' THEN '" << new_value << "' ELSE CEO END, "
    << "Founded, Headquarters, Employees, MarketCapTier, GETDATE() "
    << "FROM DimCompany WHERE Ticker='" << ticker << "' AND ExpiryDate=CAST(GETDATE() AS DATE)";

  if (odbc.exec_direct(sql_insert.str()) < 0)
  {
    return -1;
  }

  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
//etl_t::run_analytics
/////////////////////////////////////////////////////////////////////////////////////////////////////

int etl_t::run_analytics()
{
  table_t table;

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //market cap rankings
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_rank =
    "SELECT TOP 50 c.Ticker, c.CompanyName, c.Sector, f.MarketCap/1e12 AS MarketCapT, "
    "RANK() OVER (ORDER BY f.MarketCap DESC) AS Rank "
    "FROM FactDailyStock f "
    "JOIN DimCompany c ON f.CompanyKey = c.CompanyKey "
    "WHERE c.IsCurrent = 1 "
    "AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock) "
    "ORDER BY Rank";

  if (odbc.fetch(sql_rank, table) < 0)
  {
    return -1;
  }

  printf("\n");
  printf("%-4s %-6s %-30s %-20s %12s\n", "Rank", "Ticker", "Company", "Sector", "Market Cap");
  printf("------------------------------------------------------------------------------\n");

  for (size_t idx = 0; idx < table.rows.size(); idx++)
  {
    std::string ticker = table.get_row_col_value(static_cast<int>(idx), "Ticker");
    std::string name = table.get_row_col_value(static_cast<int>(idx), "CompanyName");
    std::string sector = table.get_row_col_value(static_cast<int>(idx), "Sector");
    std::string mcap = table.get_row_col_value(static_cast<int>(idx), "MarketCapT");
    std::string rank = table.get_row_col_value(static_cast<int>(idx), "Rank");

    if (name.length() > 28)
    {
      name = name.substr(0, 28) + "..";
    }
    if (sector.length() > 18)
    {
      sector = sector.substr(0, 18) + "..";
    }

    printf("%-4s %-6s %-30s %-20s $%10sT\n", rank.c_str(), ticker.c_str(), name.c_str(), sector.c_str(), mcap.c_str());
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //sector breakdown
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_sector =
    "SELECT c.Sector, COUNT(DISTINCT c.Ticker) AS Companies, SUM(f.MarketCap)/1e12 AS TotalMarketCapT "
    "FROM FactDailyStock f "
    "JOIN DimCompany c ON f.CompanyKey = c.CompanyKey "
    "WHERE c.IsCurrent = 1 "
    "AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock) "
    "GROUP BY c.Sector "
    "ORDER BY TotalMarketCapT DESC";

  if (odbc.fetch(sql_sector, table) < 0)
  {
    return -1;
  }

  printf("\n");
  printf("%-30s %10s %15s\n", "Sector", "Companies", "Total Cap");
  printf("------------------------------------------------------------------------------\n");

  for (size_t idx = 0; idx < table.rows.size(); idx++)
  {
    std::string sector = table.get_row_col_value(static_cast<int>(idx), "Sector");
    std::string cnt = table.get_row_col_value(static_cast<int>(idx), "Companies");
    std::string total = table.get_row_col_value(static_cast<int>(idx), "TotalMarketCapT");

    printf("%-30s %10s $%13sT\n", sector.c_str(), cnt.c_str(), total.c_str());
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  //financial metrics summary
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql_fin =
    "SELECT TOP 20 c.Ticker, c.CompanyName, "
    "ff.Revenue/1e9 AS RevenueB, ff.NetIncome/1e9 AS NetIncomeB, "
    "ff.GrossMargin, ff.NetMargin, ff.ROE, ff.ROA "
    "FROM FactFinancials ff "
    "JOIN DimCompany c ON ff.CompanyKey = c.CompanyKey "
    "WHERE c.IsCurrent = 1 "
    "AND ff.DateKey = (SELECT MAX(DateKey) FROM FactFinancials) "
    "ORDER BY ff.Revenue DESC";

  if (odbc.fetch(sql_fin, table) < 0)
  {
    std::cout << "No financial data available yet" << std::endl;
  }
  else if (table.rows.size() > 0)
  {
    printf("\n");
    printf("%-6s %-25s %10s %10s %8s %8s %8s %8s\n",
      "Ticker", "Company", "Rev($B)", "Net($B)", "Gross%", "Net%", "ROE%", "ROA%");
    printf("------------------------------------------------------------------------------\n");

    for (size_t idx = 0; idx < table.rows.size(); idx++)
    {
      std::string ticker = table.get_row_col_value(static_cast<int>(idx), "Ticker");
      std::string name = table.get_row_col_value(static_cast<int>(idx), "CompanyName");
      std::string rev = table.get_row_col_value(static_cast<int>(idx), "RevenueB");
      std::string net = table.get_row_col_value(static_cast<int>(idx), "NetIncomeB");
      std::string gm = table.get_row_col_value(static_cast<int>(idx), "GrossMargin");
      std::string nm = table.get_row_col_value(static_cast<int>(idx), "NetMargin");
      std::string roe = table.get_row_col_value(static_cast<int>(idx), "ROE");
      std::string roa = table.get_row_col_value(static_cast<int>(idx), "ROA");

      if (name.length() > 23)
      {
        name = name.substr(0, 23) + "..";
      }

      printf("%-6s %-25s %10s %10s %8s %8s %8s %8s\n",
        ticker.c_str(), name.c_str(), rev.c_str(), net.c_str(), gm.c_str(), nm.c_str(), roe.c_str(), roa.c_str());
    }
  }

  return 0;
}

