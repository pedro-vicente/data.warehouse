/////////////////////////////////////////////////////////////////////////////////////////////////////
// fetch.cc
// fetches real financial data from Alpha Vantage API and generates CSV files
// reads API key from alpha.vantage.txt
// get free API key from: https://www.alphavantage.co/support/#api-key
/////////////////////////////////////////////////////////////////////////////////////////////////////

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <thread>
#include <chrono>
#include "stock.hh"

int test = 1;
int wait = 12;
std::string read_key(const std::string& filename);

/////////////////////////////////////////////////////////////////////////////////////////////////////
// top 100 US companies by market cap
/////////////////////////////////////////////////////////////////////////////////////////////////////

static const char* TOP_100_TICKERS[] =
{
  // Technology (25)
  "AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "AMD", "ADBE", "CSCO", "ACN", "INTC",
  "IBM", "INTU", "NOW", "TXN", "QCOM", "CRM", "AMAT", "MU", "LRCX", "ADI",
  "KLAC", "CDNS", "SNPS", "MRVL", "FTNT",
  // Financials (15)
  "BRK.B", "JPM", "V", "MA", "BAC", "WFC", "GS", "AXP", "SPGI", "MS",
  "BLK", "C", "SCHW", "CB", "MMC",
  // Healthcare (15)
  "LLY", "UNH", "JNJ", "ABBV", "MRK", "TMO", "ABT", "PFE", "DHR", "BMY",
  "AMGN", "GILD", "VRTX", "ISRG", "MDT",
  // Consumer Discretionary (10)
  "AMZN", "TSLA", "HD", "MCD", "NKE", "LOW", "SBUX", "TJX", "BKNG", "CMG",
  // Communication Services (8)
  "META", "GOOGL", "GOOG", "NFLX", "DIS", "VZ", "T", "CMCSA",
  // Consumer Staples (10)
  "WMT", "PG", "COST", "KO", "PEP", "PM", "MO", "CL", "MDLZ", "KHC",
  // Energy (5)
  "XOM", "CVX", "COP", "SLB", "EOG",
  // Industrials (8)
  "GE", "CAT", "RTX", "HON", "UNP", "UPS", "DE", "BA",
  // Materials (2)
  "LIN", "APD",
  // Utilities (2)
  "NEE", "DUK"
};

static const int NUM_TICKERS = sizeof(TOP_100_TICKERS) / sizeof(TOP_100_TICKERS[0]);

/////////////////////////////////////////////////////////////////////////////////////////////////////
// main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main()
{
  std::string api_key = read_key("alpha.vantage.txt");
  if (api_key.empty())
  {
    return -1;
  }

  if (test)
  {
    wait = 2;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // build ticker list
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<std::string> tickers;
  for (int idx = 0; idx < NUM_TICKERS; ++idx)
  {
    tickers.push_back(TOP_100_TICKERS[idx]);
  }

  size_t size = tickers.size();
  if (test)
  {
    size = 1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // fetch stock prices 
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<StockQuote> quotes;
  int days = 30;

  for (size_t idx = 0; idx < size; ++idx)
  {
    std::vector<StockQuote> quote;
    if (fetch_daily_stock(api_key, tickers[idx], quote, days) == 0)
    {
      quotes.insert(quotes.end(), quote.begin(), quote.end());
    }

    if (idx < tickers.size() - 1 && wait > 0)
    {
      std::this_thread::sleep_for(std::chrono::seconds(wait));
    }
  }

  export_stock_data_csv(quotes, "stock_data.csv");
  std::cout << std::endl;

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // fetch company info
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<CompanyInfo> companies;

  for (size_t idx = 0; idx < size; ++idx)
  {
    CompanyInfo info;
    if (fetch_company_overview(api_key, tickers[idx], info) == 0)
    {
      companies.push_back(info);
    }

    if (idx < tickers.size() - 1 && wait > 0)
    {
      std::this_thread::sleep_for(std::chrono::seconds(wait));
    }
  }

  export_companies_csv(companies, "companies.csv");
  std::cout << std::endl;

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // fetch financials
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<FinancialStatement> financials;

  for (size_t idx = 0; idx < size; ++idx)
  {
    std::vector<FinancialStatement> statements;
    if (fetch_income_statement(api_key, tickers[idx], statements) == 0)
    {
      financials.insert(financials.end(), statements.begin(), statements.end());
    }

    if (idx < tickers.size() - 1 && wait > 0)
    {
      std::this_thread::sleep_for(std::chrono::seconds(wait));
    }
  }

  export_financials_csv(financials, "financials.csv");
  std::cout << std::endl;
  return 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// read_key
// read API key from alpha.vantage.txt
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string read_key(const std::string& filename)
{
  std::ifstream ifs(filename);
  if (!ifs.is_open())
  {
    return "";
  }

  std::string key;
  std::getline(ifs, key);
  ifs.close();

  size_t start = key.find_first_not_of(" \t\r\n");
  size_t end = key.find_last_not_of(" \t\r\n");
  if (start != std::string::npos)
  {
    key = key.substr(start, end - start + 1);
  }

  return key;
}

