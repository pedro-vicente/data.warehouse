#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include <cmath>
#include <cassert>
#include "ssl_read.hh"
#include "stock.hh"

/////////////////////////////////////////////////////////////////////////////////////////////////////
// constants
/////////////////////////////////////////////////////////////////////////////////////////////////////

static const std::string ALPHAVANTAGE_HOST = "www.alphavantage.co";
static const std::string ALPHAVANTAGE_PORT = "443";

/////////////////////////////////////////////////////////////////////////////////////////////////////
// prototype declarations
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> split_csv_line(const std::string& line);
double safe_stod(const std::string& str);
long long safe_stoll(const std::string& str);
std::string get_market_cap_tier(long long market_cap);
static std::string extract_json_string(const std::string& json, const std::string& key);

/////////////////////////////////////////////////////////////////////////////////////////////////////
// fetch_daily_stock
// GET https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo&datatype=csv
// CSV format: timestamp,open,high,low,close,volume
/////////////////////////////////////////////////////////////////////////////////////////////////////

int fetch_daily_stock(const std::string& api_key, const std::string& ticker,
  std::vector<StockQuote>& quotes, int limit)
{
  quotes.clear();

  std::string path = "/query?function=TIME_SERIES_DAILY&symbol=" + ticker +
    "&apikey=" + api_key + "&datatype=csv&outputsize=compact";

  std::stringstream http;
  http << "GET " << path << " HTTP/1.1\r\n";
  http << "Host: " << ALPHAVANTAGE_HOST << "\r\n";
  http << "User-Agent: Mozilla/5.0\r\n";
  http << "Accept: */*\r\n";
  http << "Connection: close\r\n\r\n";

  std::string response;
  std::vector<std::string> headers;

  int result = ssl_read(ALPHAVANTAGE_HOST, ALPHAVANTAGE_PORT, http.str(), response, headers);
  if (result != 0)
  {
    return -1;
  }

  std::cout << response << std::endl;

  std::istringstream iss(response);
  std::string line;
  bool first_line = true;
  int count = 0;

  while (std::getline(iss, line) && count < limit)
  {
    if (line.empty() || line[0] == '\r') continue;

    if (first_line)
    {
      first_line = false;
      continue;
    }

    std::vector<std::string> fields = split_csv_line(line);
    if (fields.size() < 5) continue;

    // CSV format: timestamp,open,high,low,close,volume
    StockQuote quote;
    quote.ticker = ticker;
    quote.date = fields[0];
    quote.open = safe_stod(fields[1]);
    quote.high = safe_stod(fields[2]);
    quote.low = safe_stod(fields[3]);
    quote.close = safe_stod(fields[4]);
    quote.adjusted_close = quote.close;
    quote.volume = fields.size() > 5 ? safe_stoll(fields[5]) : 0;

    if (quote.open > 0)
    {
      quote.daily_return = (quote.close - quote.open) / quote.open;
    }
    else
    {
      quote.daily_return = 0.0;
    }

    quote.market_cap = 0;

    quotes.push_back(quote);
    ++count;
  }

  std::cout << "  " << ticker << ": " << quotes.size() << " days" << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// fetch_company_overview
// GET https://www.alphavantage.co/query?function=OVERVIEW&symbol=IBM&apikey=demo
/////////////////////////////////////////////////////////////////////////////////////////////////////

int fetch_company_overview(const std::string& api_key, const std::string& ticker,
  CompanyInfo& info)
{
  std::string path = "/query?function=OVERVIEW&symbol=" + ticker + "&apikey=" + api_key;

  std::stringstream http;
  http << "GET " << path << " HTTP/1.1\r\n";
  http << "Host: " << ALPHAVANTAGE_HOST << "\r\n";
  http << "User-Agent: Mozilla/5.0\r\n";
  http << "Accept: */*\r\n";
  http << "Connection: close\r\n\r\n";

  std::string response;
  std::vector<std::string> headers;

  int result = ssl_read(ALPHAVANTAGE_HOST, ALPHAVANTAGE_PORT, http.str(), response, headers);
  if (result != 0)
  {
    return -1;
  }

  std::cout << response << std::endl;

  if (response.empty() || response == "{}")
  {
    return -1;
  }

  info.ticker = ticker;
  info.name = extract_json_string(response, "Name");
  info.sector = extract_json_string(response, "Sector");
  info.industry = extract_json_string(response, "Industry");
  info.exchange = extract_json_string(response, "Exchange");
  info.country = extract_json_string(response, "Country");
  info.market_cap = safe_stoll(extract_json_string(response, "MarketCapitalization"));
  info.employees = static_cast<int>(safe_stoll(extract_json_string(response, "FullTimeEmployees")));

  if (info.name.empty())
  {
    info.name = ticker;
  }

  std::cout << "  " << ticker << ": " << info.name << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// fetch_income_statement
// GET https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol=IBM&apikey=demo
/////////////////////////////////////////////////////////////////////////////////////////////////////

int fetch_income_statement(const std::string& api_key, const std::string& ticker,
  std::vector<FinancialStatement>& statements)
{
  statements.clear();

  std::string path = "/query?function=INCOME_STATEMENT&symbol=" + ticker + "&apikey=" + api_key;

  std::stringstream http;
  http << "GET " << path << " HTTP/1.1\r\n";
  http << "Host: " << ALPHAVANTAGE_HOST << "\r\n";
  http << "User-Agent: Mozilla/5.0\r\n";
  http << "Accept: */*\r\n";
  http << "Connection: close\r\n\r\n";

  std::string response;
  std::vector<std::string> headers;

  int result = ssl_read(ALPHAVANTAGE_HOST, ALPHAVANTAGE_PORT, http.str(), response, headers);
  if (result != 0)
  {
    return -1;
  }

  std::cout << response << std::endl;

  size_t pos = response.find("\"quarterlyReports\"");
  if (pos == std::string::npos)
  {
    return -1;
  }

  pos = response.find("[", pos);
  if (pos == std::string::npos) return -1;

  int count = 0;
  while (count < 4)
  {
    size_t start = response.find("{", pos);
    if (start == std::string::npos) break;

    size_t end = response.find("}", start);
    if (end == std::string::npos) break;

    std::string obj = response.substr(start, end - start + 1);

    FinancialStatement stmt;
    stmt.ticker = ticker;
    stmt.fiscal_date = extract_json_string(obj, "fiscalDateEnding");
    stmt.revenue = safe_stod(extract_json_string(obj, "totalRevenue"));
    stmt.gross_profit = safe_stod(extract_json_string(obj, "grossProfit"));
    stmt.operating_income = safe_stod(extract_json_string(obj, "operatingIncome"));
    stmt.net_income = safe_stod(extract_json_string(obj, "netIncome"));
    stmt.ebitda = safe_stod(extract_json_string(obj, "ebitda"));

    // initialize balance sheet fields to zero (will be populated by merge_balance_sheet)
    stmt.total_assets = 0.0;
    stmt.total_liabilities = 0.0;
    stmt.cash = 0.0;
    stmt.total_debt = 0.0;

    if (!stmt.fiscal_date.empty())
    {
      statements.push_back(stmt);
      ++count;
    }

    pos = end + 1;
  }

  std::cout << "  " << ticker << ": " << statements.size() << " quarters (income)" << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// fetch_balance_sheet
// GET https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol=IBM&apikey=demo
/////////////////////////////////////////////////////////////////////////////////////////////////////

int fetch_balance_sheet(const std::string& api_key, const std::string& ticker,
  std::vector<BalanceSheet>& sheets)
{
  sheets.clear();

  std::string path = "/query?function=BALANCE_SHEET&symbol=" + ticker + "&apikey=" + api_key;

  std::stringstream http;
  http << "GET " << path << " HTTP/1.1\r\n";
  http << "Host: " << ALPHAVANTAGE_HOST << "\r\n";
  http << "User-Agent: Mozilla/5.0\r\n";
  http << "Accept: */*\r\n";
  http << "Connection: close\r\n\r\n";

  std::string response;
  std::vector<std::string> headers;

  int result = ssl_read(ALPHAVANTAGE_HOST, ALPHAVANTAGE_PORT, http.str(), response, headers);
  if (result != 0)
  {
    return -1;
  }

  std::cout << response << std::endl;

  size_t pos = response.find("\"quarterlyReports\"");
  if (pos == std::string::npos)
  {
    return -1;
  }

  pos = response.find("[", pos);
  if (pos == std::string::npos) return -1;

  int count = 0;
  while (count < 4)
  {
    size_t start = response.find("{", pos);
    if (start == std::string::npos) break;

    size_t end = response.find("}", start);
    if (end == std::string::npos) break;

    std::string obj = response.substr(start, end - start + 1);

    BalanceSheet sheet;
    sheet.ticker = ticker;
    sheet.fiscal_date = extract_json_string(obj, "fiscalDateEnding");
    sheet.total_assets = safe_stod(extract_json_string(obj, "totalAssets"));
    sheet.total_liabilities = safe_stod(extract_json_string(obj, "totalLiabilities"));
    sheet.cash = safe_stod(extract_json_string(obj, "cashAndCashEquivalentsAtCarryingValue"));
    sheet.total_debt = safe_stod(extract_json_string(obj, "shortLongTermDebtTotal"));

    // try alternate field names if primary ones are empty
    if (sheet.cash == 0.0)
    {
      sheet.cash = safe_stod(extract_json_string(obj, "cashAndShortTermInvestments"));
    }
    if (sheet.total_debt == 0.0)
    {
      double short_term = safe_stod(extract_json_string(obj, "shortTermDebt"));
      double long_term = safe_stod(extract_json_string(obj, "longTermDebt"));
      sheet.total_debt = short_term + long_term;
    }

    if (!sheet.fiscal_date.empty())
    {
      sheets.push_back(sheet);
      ++count;
    }

    pos = end + 1;
  }

  std::cout << "  " << ticker << ": " << sheets.size() << " quarters (balance sheet)" << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// merge_balance_sheet
// merges balance sheet data into financial statements by matching fiscal_date
/////////////////////////////////////////////////////////////////////////////////////////////////////

int merge_balance_sheet(std::vector<FinancialStatement>& statements,
  const std::vector<BalanceSheet>& sheets)
{
  int merged_count = 0;

  for (size_t idx = 0; idx < statements.size(); idx++)
  {
    FinancialStatement& stmt = statements[idx];

    // find matching balance sheet by ticker and fiscal_date
    for (size_t jdx = 0; jdx < sheets.size(); jdx++)
    {
      const BalanceSheet& sheet = sheets[jdx];

      if (stmt.ticker == sheet.ticker && stmt.fiscal_date == sheet.fiscal_date)
      {
        stmt.total_assets = sheet.total_assets;
        stmt.total_liabilities = sheet.total_liabilities;
        stmt.cash = sheet.cash;
        stmt.total_debt = sheet.total_debt;
        merged_count++;
        break;
      }
    }
  }

  std::cout << "  Merged " << merged_count << " balance sheets" << std::endl;
  return merged_count;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// export_companies_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int export_companies_csv(const std::vector<CompanyInfo>& companies, const std::string& filename)
{
  std::ofstream ofs(filename);
  if (!ofs.is_open())
  {
    return -1;
  }

  ofs << "Ticker,CompanyName,Sector,Industry,CEO,Founded,Headquarters,Employees,MarketCapTier\n";

  for (size_t idx = 0; idx < companies.size(); ++idx)
  {
    const CompanyInfo& c = companies[idx];

    std::string name = c.name;
    if (name.find(',') != std::string::npos)
    {
      name = "\"" + name + "\"";
    }

    ofs << c.ticker << ","
      << name << ","
      << c.sector << ","
      << c.industry << ","
      << "Unknown" << ","
      << "Unknown" << ","
      << c.country << ","
      << c.employees << ","
      << get_market_cap_tier(c.market_cap) << "\n";
  }

  ofs.close();
  std::cout << "Exported " << companies.size() << " companies to " << filename << std::endl;
  assert(companies.size());
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// export_stock_data_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int export_stock_data_csv(const std::vector<StockQuote>& quotes, const std::vector<CompanyInfo>& companies, const std::string& filename)
{
  std::ofstream ofs(filename);
  if (!ofs.is_open())
  {
    return -1;
  }

  ofs << "Ticker,Date,OpenPrice,HighPrice,LowPrice,ClosePrice,Volume,MarketCap,DailyReturn\n";
  ofs << std::fixed << std::setprecision(1);

  for (size_t idx = 0; idx < quotes.size(); ++idx)
  {
    const StockQuote& q = quotes[idx];

    // look up market cap from companies by ticker
    long long market_cap = 0;
    for (size_t jdx = 0; jdx < companies.size(); ++jdx)
    {
      if (companies[jdx].ticker == q.ticker)
      {
        market_cap = companies[jdx].market_cap;
        break;
      }
    }

    ofs << q.ticker << ","
      << q.date << ","
      << q.open << ","
      << q.high << ","
      << q.low << ","
      << q.close << ","
      << q.volume << ","
      << market_cap << ","
      << q.daily_return << "\n";
  }

  ofs.close();
  std::cout << "Exported " << quotes.size() << " stock quotes to " << filename << std::endl;
  assert(quotes.size());
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// export_financials_csv
/////////////////////////////////////////////////////////////////////////////////////////////////////

int export_financials_csv(const std::vector<FinancialStatement>& statements, const std::string& filename)
{
  std::ofstream ofs(filename);
  if (!ofs.is_open())
  {
    return -1;
  }

  ofs << "Ticker,QuarterEnd,Revenue,GrossProfit,OperatingIncome,NetIncome,EPS,EBITDA,"
    << "TotalAssets,TotalLiabilities,CashAndEquivalents,TotalDebt,FreeCashFlow,RnDExpense,"
    << "GrossMargin,OperatingMargin,NetMargin,ROE,ROA\n";

  ofs << std::fixed << std::setprecision(1);

  for (size_t idx = 0; idx < statements.size(); ++idx)
  {
    const FinancialStatement& s = statements[idx];

    double gross_margin = s.revenue > 0 ? s.gross_profit / s.revenue : 0;
    double operating_margin = s.revenue > 0 ? s.operating_income / s.revenue : 0;
    double net_margin = s.revenue > 0 ? s.net_income / s.revenue : 0;

    // calculate ROE and ROA from balance sheet data
    double equity = s.total_assets - s.total_liabilities;
    double roe = equity > 0 ? s.net_income / equity : 0;
    double roa = s.total_assets > 0 ? s.net_income / s.total_assets : 0;

    ofs << s.ticker << ","
      << s.fiscal_date << ","
      << s.revenue << ","
      << s.gross_profit << ","
      << s.operating_income << ","
      << s.net_income << ","
      << "0" << ","
      << s.ebitda << ","
      << s.total_assets << ","
      << s.total_liabilities << ","
      << s.cash << ","
      << s.total_debt << ","
      << "0" << ","
      << "0" << ","
      << gross_margin << ","
      << operating_margin << ","
      << net_margin << ","
      << roe << ","
      << roa << "\n";
  }

  ofs.close();
  std::cout << "Exported " << statements.size() << " financial statements to " << filename << std::endl;
  assert(statements.size());  
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// split_csv_line
// splits a CSV line into fields, handling quoted fields with commas.
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::vector<std::string> split_csv_line(const std::string& line)
{
  std::vector<std::string> fields;
  std::string field;
  bool in_quotes = false;

  for (size_t idx = 0; idx < line.size(); ++idx)
  {
    char c = line[idx];
    if (c == '"')
    {
      in_quotes = !in_quotes;
    }
    else if (c == ',' && !in_quotes)
    {
      size_t start = field.find_first_not_of(" \t\r\n");
      size_t end = field.find_last_not_of(" \t\r\n");
      if (start != std::string::npos)
      {
        fields.push_back(field.substr(start, end - start + 1));
      }
      else
      {
        fields.push_back("");
      }
      field.clear();
    }
    else
    {
      field += c;
    }
  }

  size_t start = field.find_first_not_of(" \t\r\n");
  size_t end = field.find_last_not_of(" \t\r\n");
  if (start != std::string::npos)
  {
    fields.push_back(field.substr(start, end - start + 1));
  }
  else
  {
    fields.push_back("");
  }

  return fields;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// safe_stod
/////////////////////////////////////////////////////////////////////////////////////////////////////

double safe_stod(const std::string& str)
{
  if (str.empty() || str == "None" || str == "null" || str == "-")
  {
    return 0.0;
  }
  try
  {
    return std::stod(str);
  }
  catch (...)
  {
    return 0.0;
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// safe_stoll
/////////////////////////////////////////////////////////////////////////////////////////////////////

long long safe_stoll(const std::string& str)
{
  if (str.empty() || str == "None" || str == "null" || str == "-")
  {
    return 0;
  }
  try
  {
    return std::stoll(str);
  }
  catch (...)
  {
    return 0;
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// get_market_cap_tier
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string get_market_cap_tier(long long market_cap)
{
  if (market_cap >= 200000000000LL)
  {
    return "Mega Cap";
  }
  else if (market_cap >= 10000000000LL)
  {
    return "Large Cap";
  }
  else if (market_cap >= 2000000000LL)
  {
    return "Mid Cap";
  }
  return "Small Cap";
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// extract_json_string
/////////////////////////////////////////////////////////////////////////////////////////////////////

static std::string extract_json_string(const std::string& json, const std::string& key)
{
  std::string search = "\"" + key + "\"";
  size_t pos = json.find(search);
  if (pos == std::string::npos) return "";

  pos = json.find(":", pos);
  if (pos == std::string::npos) return "";

  pos = json.find("\"", pos);
  if (pos == std::string::npos) return "";

  size_t end = json.find("\"", pos + 1);
  if (end == std::string::npos) return "";

  return json.substr(pos + 1, end - pos - 1);
}
