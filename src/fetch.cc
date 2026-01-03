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
#include <algorithm>
#include "stock.hh"

std::string read_key(const std::string& filename);
int read_tickers_from_csv(const std::string& filename, std::vector<std::string>& tickers);

/////////////////////////////////////////////////////////////////////////////////////////////////////
// ticker_entry_t
// holds ticker symbol and market cap for sorting
/////////////////////////////////////////////////////////////////////////////////////////////////////

struct ticker_entry_t
{
  std::string symbol;
  double market_cap;
};

/////////////////////////////////////////////////////////////////////////////////////////////////////
// compare_by_market_cap
// comparator for sorting tickers by market cap descending
/////////////////////////////////////////////////////////////////////////////////////////////////////

bool compare_by_market_cap(const ticker_entry_t& a, const ticker_entry_t& b)
{
  return a.market_cap > b.market_cap;
}

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
  std::cout << "  -t, --ticker SYM  Fetch single ticker only" << std::endl;
  std::cout << "  -n, --count N     Number of companies to fetch (default: all)" << std::endl;
  std::cout << "  -d, --days N      Days of stock history (default: 2)" << std::endl;
  std::cout << "  -w, --wait N      Seconds between API calls (default: 12)" << std::endl;
  std::cout << "  --test            Test mode: 1 company, 3 sec wait" << std::endl;
  std::cout << "  -h, --help        Display this help message" << std::endl;
  std::cout << std::endl;
  std::cout << "Output files:" << std::endl;
  std::cout << "  stock_data.csv  Daily OHLCV data" << std::endl;
  std::cout << "  companies.csv   Company information" << std::endl;
  std::cout << "  financials.csv  Financial statements" << std::endl;
  std::cout << std::endl;
  std::cout << "Examples:" << std::endl;
  std::cout << "  " << program_name << " --test              # test with 1 company" << std::endl;
  std::cout << "  " << program_name << " -n 50 --stocks      # top 50 by market cap" << std::endl;
  std::cout << "  " << program_name << " --all               # all S&P 500 companies" << std::endl;
  std::cout << "  " << program_name << " --ticker AAPL       # single ticker only" << std::endl;
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
  std::string csv_file = "sp500_financials.csv";
  std::string single_ticker;
  int ticker_count = -1;
  int days = 2;
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
    if (read_tickers_from_csv(csv_file, tickers) < 0)
    {
      return 1;
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
  std::cout << "  CSV file:     " << csv_file << std::endl;
  std::cout << "  Companies:    " << size << std::endl;
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
  // fetch company info (needed for market cap in stock data)
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<CompanyInfo> companies;

  if (fetch_companies || fetch_stocks)
  {
    for (size_t idx = 0; idx < size; ++idx)
    {
      std::cout << "\r[" << (idx + 1) << "/" << size << "] " << tickers[idx] << " - fetching company info...    " << std::flush;

      CompanyInfo info;
      if (fetch_company_overview(api_key, tickers[idx], info) == 0)
      {
        companies.push_back(info);
      }

      std::this_thread::sleep_for(std::chrono::seconds(wait));
    }
    std::cout << std::endl;

    if (fetch_companies)
    {
      export_companies_csv(companies, "companies.csv");
      std::cout << "Exported companies.csv" << std::endl;
    }
    std::cout << std::endl;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // fetch stock prices 
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (fetch_stocks)
  {

    std::vector<StockQuote> quotes;

    for (size_t idx = 0; idx < size; ++idx)
    {
      std::cout << "\r[" << (idx + 1) << "/" << size << "] " << tickers[idx] << " - fetching stock prices...    " << std::flush;

      std::vector<StockQuote> quote;
      if (fetch_daily_stock(api_key, tickers[idx], quote, days) == 0)
      {
        quotes.insert(quotes.end(), quote.begin(), quote.end());
      }

      std::this_thread::sleep_for(std::chrono::seconds(wait));
    }
    std::cout << std::endl;

    export_stock_data_csv(quotes, companies, "stock_data.csv");
    std::cout << "Exported stock_data.csv" << std::endl;
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
        std::cout << "\r[" << (idx + 1) << "/" << size << "] " << tickers[idx] << " - fetching income statement...    " << std::flush;

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
        std::cout << "\r[" << (idx + 1) << "/" << size << "] " << tickers[idx] << " - fetching balance sheet...    " << std::flush;

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
      std::cout << "Exported financials.csv" << std::endl;
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

/////////////////////////////////////////////////////////////////////////////////////////////////////
// read_tickers_from_csv
// reads tickers from CSV file with columns: Symbol,Name,Sector,...,Market Cap,...
// sorts by market cap descending
/////////////////////////////////////////////////////////////////////////////////////////////////////

int read_tickers_from_csv(const std::string& filename, std::vector<std::string>& tickers)
{
  std::ifstream ifs(filename);
  if (!ifs.is_open())
  {
    return -1;
  }

  std::vector<ticker_entry_t> entries;
  std::string line;
  bool first_line = true;
  int symbol_col = -1;
  int market_cap_col = -1;

  while (std::getline(ifs, line))
  {
    // remove carriage return if present
    if (!line.empty() && line[line.size() - 1] == '\r')
    {
      line = line.substr(0, line.size() - 1);
    }

    // parse CSV fields (handle quoted fields)
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
        fields.push_back(field);
        field.clear();
      }
      else
      {
        field += c;
      }
    }
    fields.push_back(field);

    // find column indices from header
    if (first_line)
    {
      first_line = false;
      for (size_t idx = 0; idx < fields.size(); ++idx)
      {
        if (fields[idx] == "Symbol")
        {
          symbol_col = static_cast<int>(idx);
        }
        else if (fields[idx] == "Market Cap")
        {
          market_cap_col = static_cast<int>(idx);
        }
      }

      if (symbol_col < 0)
      {
        ifs.close();
        return -1;
      }
      continue;
    }

    // extract ticker and market cap
    if (symbol_col >= 0 && static_cast<size_t>(symbol_col) < fields.size())
    {
      ticker_entry_t entry;
      entry.symbol = fields[symbol_col];
      entry.market_cap = 0.0;

      if (market_cap_col >= 0 && static_cast<size_t>(market_cap_col) < fields.size())
      {
        std::string mcap_str = fields[market_cap_col];
        if (!mcap_str.empty())
        {
          entry.market_cap = std::stod(mcap_str);
        }
      }

      if (!entry.symbol.empty())
      {
        entries.push_back(entry);
      }
    }
  }

  ifs.close();

  // sort by market cap descending
  std::sort(entries.begin(), entries.end(), compare_by_market_cap);

  // extract tickers
  tickers.clear();
  for (size_t idx = 0; idx < entries.size(); ++idx)
  {
    tickers.push_back(entries[idx].symbol);
  }

  return 0;
}
