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
#include <cstring>
#include <cstdlib>
#include "stock.hh"

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
// usage
/////////////////////////////////////////////////////////////////////////////////////////////////////

void usage(const char* program_name)
{
  std::cout << "Usage: " << program_name << " [OPTIONS]" << std::endl;
  std::cout << std::endl;
  std::cout << "Fetch options (if none specified, fetches all):" << std::endl;
  std::cout << "  --stocks        Fetch daily stock prices" << std::endl;
  std::cout << "  --companies     Fetch company overview/info" << std::endl;
  std::cout << "  --income        Fetch income statements" << std::endl;
  std::cout << "  --balance       Fetch balance sheets" << std::endl;
  std::cout << "  --financials    Fetch both income and balance (merged)" << std::endl;
  std::cout << "  --all           Fetch all data types (default)" << std::endl;
  std::cout << std::endl;
  std::cout << "Other options:" << std::endl;
  std::cout << "  -k, --key FILE  API key file (default: alpha.vantage.txt)" << std::endl;
  std::cout << "  -t, --ticker SYM  Fetch single ticker only" << std::endl;
  std::cout << "  -n, --count N   Number of tickers to fetch (default: all)" << std::endl;
  std::cout << "  -d, --days N    Days of stock history (default: 30)" << std::endl;
  std::cout << "  -w, --wait N    Seconds between API calls (default: 12)" << std::endl;
  std::cout << "  --test          Test mode: 1 ticker, 2 sec wait" << std::endl;
  std::cout << "  -h, --help      Display this help message" << std::endl;
  std::cout << std::endl;
  std::cout << "Output files:" << std::endl;
  std::cout << "  stock_data.csv  Daily OHLCV data" << std::endl;
  std::cout << "  companies.csv   Company information" << std::endl;
  std::cout << "  financials.csv  Financial statements" << std::endl;
  std::cout << std::endl;
  std::cout << "Examples:" << std::endl;
  std::cout << "  " << program_name << " --test                  # test with 1 ticker" << std::endl;
  std::cout << "  " << program_name << " --stocks -n 5           # stocks for 5 tickers" << std::endl;
  std::cout << "  " << program_name << " --ticker AAPL --all     # all data for AAPL" << std::endl;
  std::cout << "  " << program_name << " --financials -n 10      # financials for 10 tickers" << std::endl;
  std::cout << std::endl;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // default options
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string key_file = "alpha.vantage.txt";
  std::string single_ticker;
  int ticker_count = -1;
  int days = 30;
  int wait = 12;
  bool test_mode = false;

  bool fetch_stocks = false;
  bool fetch_companies = false;
  bool fetch_income = false;
  bool fetch_balance = false;
  bool any_fetch_specified = false;

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // parse command line
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  for (int idx = 1; idx < argc; idx++)
  {
    std::string arg = argv[idx];

    if (arg == "-h" || arg == "--help")
    {
      usage(argv[0]);
      return 0;
    }
    else if (arg == "--stocks")
    {
      fetch_stocks = true;
      any_fetch_specified = true;
    }
    else if (arg == "--companies")
    {
      fetch_companies = true;
      any_fetch_specified = true;
    }
    else if (arg == "--income")
    {
      fetch_income = true;
      any_fetch_specified = true;
    }
    else if (arg == "--balance")
    {
      fetch_balance = true;
      any_fetch_specified = true;
    }
    else if (arg == "--financials")
    {
      fetch_income = true;
      fetch_balance = true;
      any_fetch_specified = true;
    }
    else if (arg == "--all")
    {
      fetch_stocks = true;
      fetch_companies = true;
      fetch_income = true;
      fetch_balance = true;
      any_fetch_specified = true;
    }
    else if ((arg == "-k" || arg == "--key") && idx + 1 < argc)
    {
      key_file = argv[++idx];
    }
    else if ((arg == "-t" || arg == "--ticker") && idx + 1 < argc)
    {
      single_ticker = argv[++idx];
    }
    else if ((arg == "-n" || arg == "--count") && idx + 1 < argc)
    {
      ticker_count = std::atoi(argv[++idx]);
    }
    else if ((arg == "-d" || arg == "--days") && idx + 1 < argc)
    {
      days = std::atoi(argv[++idx]);
    }
    else if ((arg == "-w" || arg == "--wait") && idx + 1 < argc)
    {
      wait = std::atoi(argv[++idx]);
    }
    else if (arg == "--test")
    {
      test_mode = true;
    }
    else
    {
      std::cerr << "Unknown option: " << arg << std::endl;
      usage(argv[0]);
      return 1;
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // if no fetch type specified, fetch all
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (!any_fetch_specified)
  {
    fetch_stocks = true;
    fetch_companies = true;
    fetch_income = true;
    fetch_balance = true;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // test mode overrides
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (test_mode)
  {
    wait = 3;
    if (ticker_count < 0)
    {
      ticker_count = 1;
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // read API key
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string api_key = read_key(key_file);
  if (api_key.empty())
  {
    return -1;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // build ticker list
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<std::string> tickers;

  if (!single_ticker.empty())
  {
    tickers.push_back(single_ticker);
  }
  else
  {
    for (int idx = 0; idx < NUM_TICKERS; ++idx)
    {
      tickers.push_back(TOP_100_TICKERS[idx]);
    }
  }

  size_t size = tickers.size();
  if (ticker_count > 0 && static_cast<size_t>(ticker_count) < size)
  {
    size = static_cast<size_t>(ticker_count);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // display configuration
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::cout << "Fetch Configuration:" << std::endl;
  std::cout << "  API key file: " << key_file << std::endl;
  std::cout << "  Tickers:      " << size << std::endl;
  std::cout << "  Wait time:    " << wait << " seconds" << std::endl;
  std::cout << "  Fetch types:  ";
  if (fetch_stocks) std::cout << "stocks ";
  if (fetch_companies) std::cout << "companies ";
  if (fetch_income) std::cout << "income ";
  if (fetch_balance) std::cout << "balance ";
  std::cout << std::endl;
  if (fetch_stocks) std::cout << "  Stock days:   " << days << std::endl;
  std::cout << std::endl;

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // fetch stock prices 
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (fetch_stocks)
  {
    std::vector<StockQuote> quotes;

    for (size_t idx = 0; idx < size; ++idx)
    {
      std::vector<StockQuote> quote;
      if (fetch_daily_stock(api_key, tickers[idx], quote, days) == 0)
      {
        quotes.insert(quotes.end(), quote.begin(), quote.end());
      }

      std::this_thread::sleep_for(std::chrono::seconds(wait));
    }

    export_stock_data_csv(quotes, "stock_data.csv");
    std::cout << std::endl;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // fetch company info
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (fetch_companies)
  {
    std::vector<CompanyInfo> companies;

    for (size_t idx = 0; idx < size; ++idx)
    {
      CompanyInfo info;
      if (fetch_company_overview(api_key, tickers[idx], info) == 0)
      {
        companies.push_back(info);
      }

      std::this_thread::sleep_for(std::chrono::seconds(wait));
    }

    export_companies_csv(companies, "companies.csv");
    std::cout << std::endl;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // fetch financials (income statement + balance sheet)
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (fetch_income || fetch_balance)
  {
    std::vector<FinancialStatement> financials;
    std::vector<BalanceSheet> balance_sheet;

    // fetch income statements
    if (fetch_income)
    {
      for (size_t idx = 0; idx < size; ++idx)
      {
        std::vector<FinancialStatement> statements;
        if (fetch_income_statement(api_key, tickers[idx], statements) == 0)
        {
          financials.insert(financials.end(), statements.begin(), statements.end());
        }

        std::this_thread::sleep_for(std::chrono::seconds(wait));
      }
      std::cout << std::endl;
    }

    // fetch balance sheets
    if (fetch_balance)
    {
      for (size_t idx = 0; idx < size; ++idx)
      {
        std::vector<BalanceSheet> sheets;
        if (fetch_balance_sheet(api_key, tickers[idx], sheets) == 0)
        {
          balance_sheet.insert(balance_sheet.end(), sheets.begin(), sheets.end());
        }

        std::this_thread::sleep_for(std::chrono::seconds(wait));
      }
      std::cout << std::endl;
    }

    // merge balance sheet data into financial statements if both were fetched
    if (fetch_income && fetch_balance)
    {
      merge_balance_sheet(financials, balance_sheet);
    }

    // export financials if income was fetched
    if (fetch_income)
    {
      export_financials_csv(financials, "financials.csv");
      std::cout << std::endl;
    }
  }

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
