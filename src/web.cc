#include "web.hh"
#include <Wt/WBreak.h>
#include <Wt/WHBoxLayout.h>
#include <Wt/WVBoxLayout.h>
#include <Wt/WPanel.h>
#include <Wt/WGroupBox.h>
#include <Wt/WImage.h>
#include <Wt/WCssDecorationStyle.h>
#include <Wt/WColor.h>
#include <Wt/WLength.h>
#include <Wt/WFont.h>
#include <Wt/WBorder.h>
#include <sstream>
#include <iomanip>
#include <cstdlib>
#include <cstring>
#include <vector>

/////////////////////////////////////////////////////////////////////////////////////////////////////
// global configuration (set from command line, used by WApplicationFinmart)
/////////////////////////////////////////////////////////////////////////////////////////////////////

static std::string server;
static std::string database;
static std::string user;
static std::string password;

/////////////////////////////////////////////////////////////////////////////////////////////////////
// usage
/////////////////////////////////////////////////////////////////////////////////////////////////////

void usage(const char* program_name)
{
  std::cout << "Usage: " << program_name << " [OPTIONS]" << std::endl;
  std::cout << std::endl;
  std::cout << "Database options:" << std::endl;
  std::cout << "  -S SERVER     SQL Server hostname or IP address (required)" << std::endl;
  std::cout << "  -d DATABASE   Database name (required)" << std::endl;
  std::cout << "  -U USER       SQL Server username (omit for trusted connection)" << std::endl;
  std::cout << "  -P PASSWORD   SQL Server password" << std::endl;
  std::cout << "  -h, --help    Display this help message and exit" << std::endl;
  std::cout << std::endl;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// create_application
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<Wt::WApplication> create_application(const Wt::WEnvironment& env)
{
  return std::make_unique<WApplicationFinmart>(env);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// main
/////////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[])
{
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

#ifdef _MSC_VER
#else
  if (user.empty())
  {
    usage(argv[0]);
    return 1;
  }
#endif

#ifdef _MSC_VER
#else
  if (password.empty())
  {
    usage(argv[0]);
    return 1;
  }
#endif

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // display configuration
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::cout << "FinMart Web Configuration:" << std::endl;
  std::cout << "  Server:   " << server << std::endl;
  std::cout << "  Database: " << database << std::endl;
  std::cout << "  User:     " << (user.empty() ? "(trusted connection)" : user) << std::endl;
  std::cout << std::endl;

  std::string conn = make_conn(server, database, user, password);
  odbc_t odbc;
  if (odbc.connect(conn) != 0)
  {
    std::cout << "Error: Cannot connect to database" << std::endl;
    return 1;
  }

  odbc.disconnect();

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // run Wt application
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  return Wt::WRun(argc, argv, &create_application);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::WApplicationFinmart
/////////////////////////////////////////////////////////////////////////////////////////////////////

WApplicationFinmart::WApplicationFinmart(const Wt::WEnvironment& env)
  : Wt::WApplication(env)
{
  setTitle("FinMart Data Warehouse");

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // connect to database
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  if (!connect_database())
  {
    root()->addWidget(std::make_unique<Wt::WText>("Error: Cannot connect to database"));
    return;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // setup UI
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  setup_navbar();
  setup_dashboard();
  setup_companies();
  setup_stocks();
  setup_financials();
  setup_sectors();

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // load initial data
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  load_dashboard();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::~WApplicationFinmart
/////////////////////////////////////////////////////////////////////////////////////////////////////

WApplicationFinmart::~WApplicationFinmart()
{
  odbc.disconnect();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::connect_database
/////////////////////////////////////////////////////////////////////////////////////////////////////

bool WApplicationFinmart::connect_database()
{
  std::string conn = make_conn(server, database, user, password);
  return odbc.connect(conn) == 0;
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::setup_navbar
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::setup_navbar()
{
  Wt::WContainerWidget* container = root()->addWidget(std::make_unique<Wt::WContainerWidget>());

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // navbar styling
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  navbar = container->addWidget(std::make_unique<Wt::WNavigationBar>());
  navbar->setResponsive(false);
  Wt::WCssDecorationStyle style;
  style.setBackgroundColor(Wt::WColor(0, 200, 0));
  navbar->setDecorationStyle(style);

  contents = container->addWidget(std::make_unique<Wt::WStackedWidget>());
  contents->setMargin(Wt::WLength(0), Wt::Side::Top);

  menu = navbar->addMenu(std::make_unique<Wt::WMenu>(contents));

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // create views
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  dashboard_view = std::make_unique<Wt::WContainerWidget>().release();
  companies_view = std::make_unique<Wt::WContainerWidget>().release();
  stocks_view = std::make_unique<Wt::WContainerWidget>().release();
  financials_view = std::make_unique<Wt::WContainerWidget>().release();
  sectors_view = std::make_unique<Wt::WContainerWidget>().release();

  Wt::WMenuItem* item_dashboard = menu->addItem("Dashboard", std::unique_ptr<Wt::WContainerWidget>(dashboard_view));
  Wt::WMenuItem* item_companies = menu->addItem("Companies", std::unique_ptr<Wt::WContainerWidget>(companies_view));
  Wt::WMenuItem* item_stocks = menu->addItem("Stock Data", std::unique_ptr<Wt::WContainerWidget>(stocks_view));
  Wt::WMenuItem* item_financials = menu->addItem("Financials", std::unique_ptr<Wt::WContainerWidget>(financials_view));
  Wt::WMenuItem* item_sectors = menu->addItem("Sectors", std::unique_ptr<Wt::WContainerWidget>(sectors_view));

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // menu item styling
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  Wt::WCssDecorationStyle menu_style;
  menu_style.setForegroundColor(Wt::WColor(255, 255, 255));
  menu_style.font().setSize(Wt::FontSize::Medium);

  item_dashboard->setDecorationStyle(menu_style);
  item_companies->setDecorationStyle(menu_style);
  item_stocks->setDecorationStyle(menu_style);
  item_financials->setDecorationStyle(menu_style);
  item_sectors->setDecorationStyle(menu_style);

  menu->itemSelected().connect([this]()
    {
      int idx = menu->currentIndex();
      if (idx == 0) load_dashboard();
      else if (idx == 1) load_companies();
      else if (idx == 2) load_stocks();
      else if (idx == 3) load_financials();
      else if (idx == 4) load_sectors();
    });
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::setup_dashboard
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::setup_dashboard()
{
  dashboard_view->setMargin(Wt::WLength(5, Wt::LengthUnit::Pixel), Wt::Side::Top);
  dashboard_view->setPadding(Wt::WLength(10, Wt::LengthUnit::Pixel), Wt::Side::Left | Wt::Side::Right | Wt::Side::Bottom);

  Wt::WText* title = dashboard_view->addWidget(std::make_unique<Wt::WText>("<h2>Market Dashboard</h2>"));

  Wt::WCssDecorationStyle title_style;
  title_style.setForegroundColor(Wt::WColor(33, 37, 41));
  title->setDecorationStyle(title_style);

  dashboard_view->addWidget(std::make_unique<Wt::WBreak>());

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // summary cards row
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  Wt::WContainerWidget* row = dashboard_view->addWidget(std::make_unique<Wt::WContainerWidget>());

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // top 10 by market cap
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  Wt::WContainerWidget* col = row->addWidget(std::make_unique<Wt::WContainerWidget>());

  Wt::WGroupBox* box = col->addWidget(std::make_unique<Wt::WGroupBox>("Top 10 Companies by Market Cap"));

  Wt::WTable* table = box->addWidget(std::make_unique<Wt::WTable>());
  table->setHeaderCount(1);

  add_header_cell(table, 0, 0, "Rank");
  add_header_cell(table, 0, 1, "Ticker");
  add_header_cell(table, 0, 2, "Company");
  add_header_cell(table, 0, 3, "Market Cap");

  std::string sql =
    "SELECT TOP 10 c.Ticker, c.CompanyName, f.MarketCap/1e12 AS MarketCapT, "
    "RANK() OVER (ORDER BY f.MarketCap DESC) AS Rank "
    "FROM FactDailyStock f "
    "JOIN DimCompany c ON f.CompanyKey = c.CompanyKey "
    "WHERE c.IsCurrent = 1 "
    "AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock) "
    "ORDER BY Rank";

  table_t tbl;
  if (odbc.fetch(sql, tbl) == 0)
  {
    for (size_t idx = 0; idx < tbl.rows.size(); idx++)
    {
      int row = static_cast<int>(idx) + 1;
      std::string rank = tbl.get_row_col_value(static_cast<int>(idx), "Rank");
      std::string ticker = tbl.get_row_col_value(static_cast<int>(idx), "Ticker");
      std::string name = tbl.get_row_col_value(static_cast<int>(idx), "CompanyName");
      std::string mcap = tbl.get_row_col_value(static_cast<int>(idx), "MarketCapT");

      add_cell(table, row, 0, rank);
      add_cell(table, row, 1, ticker);
      add_cell(table, row, 2, name);
      add_currency_cell(table, row, 3, mcap, "T");
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // sector breakdown
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  Wt::WContainerWidget* sector_col = row->addWidget(std::make_unique<Wt::WContainerWidget>());

  Wt::WGroupBox* sector_box = sector_col->addWidget(std::make_unique<Wt::WGroupBox>("Sector Breakdown"));

  Wt::WTable* sector_table = sector_box->addWidget(std::make_unique<Wt::WTable>());
  sector_table->setHeaderCount(1);

  add_header_cell(sector_table, 0, 0, "Sector");
  add_header_cell(sector_table, 0, 1, "Companies");
  add_header_cell(sector_table, 0, 2, "Total Cap");

  std::string sql_sector =
    "SELECT c.Sector, COUNT(DISTINCT c.Ticker) AS Companies, SUM(f.MarketCap)/1e12 AS TotalMarketCapT "
    "FROM FactDailyStock f "
    "JOIN DimCompany c ON f.CompanyKey = c.CompanyKey "
    "WHERE c.IsCurrent = 1 "
    "AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock) "
    "GROUP BY c.Sector "
    "ORDER BY TotalMarketCapT DESC";

  if (odbc.fetch(sql_sector, tbl) == 0)
  {
    for (size_t idx = 0; idx < tbl.rows.size(); idx++)
    {
      int row = static_cast<int>(idx) + 1;
      std::string sector = tbl.get_row_col_value(static_cast<int>(idx), "Sector");
      std::string cnt = tbl.get_row_col_value(static_cast<int>(idx), "Companies");
      std::string total = tbl.get_row_col_value(static_cast<int>(idx), "TotalMarketCapT");

      add_cell(sector_table, row, 0, sector);
      add_cell(sector_table, row, 1, cnt);
      add_currency_cell(sector_table, row, 2, total, "T");
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::setup_companies
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::setup_companies()
{
  companies_view->setMargin(Wt::WLength(5, Wt::LengthUnit::Pixel), Wt::Side::Top);
  companies_view->setPadding(Wt::WLength(10, Wt::LengthUnit::Pixel), Wt::Side::Left | Wt::Side::Right | Wt::Side::Bottom);

  Wt::WText* title = companies_view->addWidget(std::make_unique<Wt::WText>("<h2>Companies</h2>"));

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // filter row
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  Wt::WContainerWidget* filter_row = companies_view->addWidget(std::make_unique<Wt::WContainerWidget>());
  filter_row->setMargin(Wt::WLength(10, Wt::LengthUnit::Pixel), Wt::Side::Top | Wt::Side::Bottom);

  Wt::WContainerWidget* filter_col = filter_row->addWidget(std::make_unique<Wt::WContainerWidget>());

  filter_col->addWidget(std::make_unique<Wt::WText>("Sector: "));
  sector_filter = filter_col->addWidget(std::make_unique<Wt::WComboBox>());
  sector_filter->addItem("All Sectors");
  sector_filter->changed().connect(this, &WApplicationFinmart::on_sector_changed);

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // load sectors into filter
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql = "SELECT DISTINCT Sector FROM DimCompany WHERE IsCurrent=1 ORDER BY Sector";
  table_t table;
  if (odbc.fetch(sql, table) == 0)
  {
    for (size_t idx = 0; idx < table.rows.size(); idx++)
    {
      std::string sector = table.get_row_col_value(static_cast<int>(idx), "Sector");
      sector_filter->addItem(sector);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // companies table
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  companies_table = companies_view->addWidget(std::make_unique<Wt::WTable>());
  companies_table->setHeaderCount(1);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::setup_stocks
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::setup_stocks()
{
  stocks_view->setMargin(Wt::WLength(5, Wt::LengthUnit::Pixel), Wt::Side::Top);
  stocks_view->setPadding(Wt::WLength(10, Wt::LengthUnit::Pixel), Wt::Side::Left | Wt::Side::Right | Wt::Side::Bottom);

  Wt::WText* title = stocks_view->addWidget(std::make_unique<Wt::WText>("<h2>Stock Data</h2>"));

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // filter row
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  Wt::WContainerWidget* filter_row = stocks_view->addWidget(std::make_unique<Wt::WContainerWidget>());
  filter_row->setMargin(Wt::WLength(10, Wt::LengthUnit::Pixel), Wt::Side::Top | Wt::Side::Bottom);

  Wt::WContainerWidget* filter_col = filter_row->addWidget(std::make_unique<Wt::WContainerWidget>());

  filter_col->addWidget(std::make_unique<Wt::WText>("Company: "));
  company_filter = filter_col->addWidget(std::make_unique<Wt::WComboBox>());
  company_filter->addItem("All Companies");
  company_filter->changed().connect(this, &WApplicationFinmart::on_company_changed);

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // load companies into filter
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  std::string sql = "SELECT Ticker, CompanyName FROM DimCompany WHERE IsCurrent=1 ORDER BY Ticker";
  table_t table;
  if (odbc.fetch(sql, table) == 0)
  {
    for (size_t idx = 0; idx < table.rows.size(); idx++)
    {
      std::string ticker = table.get_row_col_value(static_cast<int>(idx), "Ticker");
      company_filter->addItem(ticker);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////
  // stocks table
  /////////////////////////////////////////////////////////////////////////////////////////////////////

  stocks_table = stocks_view->addWidget(std::make_unique<Wt::WTable>());
  stocks_table->setHeaderCount(1);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::setup_financials
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::setup_financials()
{
  financials_view->setMargin(Wt::WLength(5, Wt::LengthUnit::Pixel), Wt::Side::Top);
  financials_view->setPadding(Wt::WLength(10, Wt::LengthUnit::Pixel), Wt::Side::Left | Wt::Side::Right | Wt::Side::Bottom);

  Wt::WText* title = financials_view->addWidget(std::make_unique<Wt::WText>("<h2>Financial Statements</h2>"));

  financials_table = financials_view->addWidget(std::make_unique<Wt::WTable>());
  financials_table->setHeaderCount(1);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::setup_sectors
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::setup_sectors()
{
  sectors_view->setMargin(Wt::WLength(5, Wt::LengthUnit::Pixel), Wt::Side::Top);
  sectors_view->setPadding(Wt::WLength(10, Wt::LengthUnit::Pixel), Wt::Side::Left | Wt::Side::Right | Wt::Side::Bottom);

  Wt::WText* title = sectors_view->addWidget(std::make_unique<Wt::WText>("<h2>Sector Analysis</h2>"));

  sectors_table = sectors_view->addWidget(std::make_unique<Wt::WTable>());
  sectors_table->setHeaderCount(1);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::load_dashboard
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::load_dashboard()
{
  // dashboard is static, loaded once in setup
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::load_companies
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::load_companies()
{
  companies_table->clear();

  add_header_cell(companies_table, 0, 0, "Ticker");
  add_header_cell(companies_table, 0, 1, "Company Name");
  add_header_cell(companies_table, 0, 2, "Sector");
  add_header_cell(companies_table, 0, 3, "Industry");
  add_header_cell(companies_table, 0, 4, "CEO");
  add_header_cell(companies_table, 0, 5, "Headquarters");
  add_header_cell(companies_table, 0, 6, "Employees");
  add_header_cell(companies_table, 0, 7, "Market Cap Tier");

  std::stringstream sql;
  sql << "SELECT Ticker, CompanyName, Sector, Industry, CEO, Headquarters, Employees, MarketCapTier "
    << "FROM DimCompany WHERE IsCurrent=1 ";

  if (sector_filter->currentIndex() > 0)
  {
    sql << "AND Sector='" << sector_filter->currentText().toUTF8() << "' ";
  }

  sql << "ORDER BY Ticker";

  table_t table;
  if (odbc.fetch(sql.str(), table) == 0)
  {
    for (size_t idx = 0; idx < table.rows.size(); idx++)
    {
      int row = static_cast<int>(idx) + 1;

      add_cell(companies_table, row, 0, table.get_row_col_value(static_cast<int>(idx), "Ticker"));
      add_cell(companies_table, row, 1, table.get_row_col_value(static_cast<int>(idx), "CompanyName"));
      add_cell(companies_table, row, 2, table.get_row_col_value(static_cast<int>(idx), "Sector"));
      add_cell(companies_table, row, 3, table.get_row_col_value(static_cast<int>(idx), "Industry"));
      add_cell(companies_table, row, 4, table.get_row_col_value(static_cast<int>(idx), "CEO"));
      add_cell(companies_table, row, 5, table.get_row_col_value(static_cast<int>(idx), "Headquarters"));
      add_number_cell(companies_table, row, 6, table.get_row_col_value(static_cast<int>(idx), "Employees"));
      add_cell(companies_table, row, 7, table.get_row_col_value(static_cast<int>(idx), "MarketCapTier"));
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::load_stocks
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::load_stocks()
{
  stocks_table->clear();

  add_header_cell(stocks_table, 0, 0, "Ticker");
  add_header_cell(stocks_table, 0, 1, "Date");
  add_header_cell(stocks_table, 0, 2, "Open");
  add_header_cell(stocks_table, 0, 3, "High");
  add_header_cell(stocks_table, 0, 4, "Low");
  add_header_cell(stocks_table, 0, 5, "Close");
  add_header_cell(stocks_table, 0, 6, "Volume");
  add_header_cell(stocks_table, 0, 7, "Market Cap");
  add_header_cell(stocks_table, 0, 8, "Daily Return");

  std::stringstream sql;
  sql << "SELECT TOP 100 c.Ticker, d.FullDate, f.OpenPrice, f.HighPrice, f.LowPrice, f.ClosePrice, "
    << "f.Volume, f.MarketCap/1e9 AS MarketCapB, f.DailyReturn "
    << "FROM FactDailyStock f "
    << "JOIN DimCompany c ON f.CompanyKey = c.CompanyKey "
    << "JOIN DimDate d ON f.DateKey = d.DateKey "
    << "WHERE c.IsCurrent = 1 ";

  if (company_filter->currentIndex() > 0)
  {
    sql << "AND c.Ticker='" << company_filter->currentText().toUTF8() << "' ";
  }

  sql << "ORDER BY d.FullDate DESC, c.Ticker";

  table_t table;
  if (odbc.fetch(sql.str(), table) == 0)
  {
    for (size_t idx = 0; idx < table.rows.size(); idx++)
    {
      int row = static_cast<int>(idx) + 1;

      add_cell(stocks_table, row, 0, table.get_row_col_value(static_cast<int>(idx), "Ticker"));
      add_cell(stocks_table, row, 1, table.get_row_col_value(static_cast<int>(idx), "FullDate"));
      add_currency_cell(stocks_table, row, 2, table.get_row_col_value(static_cast<int>(idx), "OpenPrice"));
      add_currency_cell(stocks_table, row, 3, table.get_row_col_value(static_cast<int>(idx), "HighPrice"));
      add_currency_cell(stocks_table, row, 4, table.get_row_col_value(static_cast<int>(idx), "LowPrice"));
      add_currency_cell(stocks_table, row, 5, table.get_row_col_value(static_cast<int>(idx), "ClosePrice"));
      add_number_cell(stocks_table, row, 6, table.get_row_col_value(static_cast<int>(idx), "Volume"));
      add_currency_cell(stocks_table, row, 7, table.get_row_col_value(static_cast<int>(idx), "MarketCapB"), "B");
      add_percent_cell(stocks_table, row, 8, table.get_row_col_value(static_cast<int>(idx), "DailyReturn"));
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::load_financials
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::load_financials()
{
  financials_table->clear();

  add_header_cell(financials_table, 0, 0, "Ticker");
  add_header_cell(financials_table, 0, 1, "Company");
  add_header_cell(financials_table, 0, 2, "Revenue");
  add_header_cell(financials_table, 0, 3, "Net Income");
  add_header_cell(financials_table, 0, 4, "Gross %");
  add_header_cell(financials_table, 0, 5, "Net %");
  add_header_cell(financials_table, 0, 6, "ROE %");
  add_header_cell(financials_table, 0, 7, "ROA %");

  std::string sql =
    "SELECT c.Ticker, c.CompanyName, "
    "ff.Revenue/1e9 AS RevenueB, ff.NetIncome/1e9 AS NetIncomeB, "
    "ff.GrossMargin * 100 AS GrossMargin, ff.NetMargin * 100 AS NetMargin, "
    "ff.ROE * 100 AS ROE, ff.ROA * 100 AS ROA "
    "FROM FactFinancials ff "
    "JOIN DimCompany c ON ff.CompanyKey = c.CompanyKey "
    "WHERE c.IsCurrent = 1 "
    "AND ff.DateKey = (SELECT MAX(DateKey) FROM FactFinancials) "
    "ORDER BY ff.Revenue DESC";

  table_t table;
  if (odbc.fetch(sql, table) == 0)
  {
    for (size_t idx = 0; idx < table.rows.size(); idx++)
    {
      int row = static_cast<int>(idx) + 1;

      add_cell(financials_table, row, 0, table.get_row_col_value(static_cast<int>(idx), "Ticker"));
      add_cell(financials_table, row, 1, table.get_row_col_value(static_cast<int>(idx), "CompanyName"));
      add_currency_cell(financials_table, row, 2, table.get_row_col_value(static_cast<int>(idx), "RevenueB"), "B");
      add_currency_cell(financials_table, row, 3, table.get_row_col_value(static_cast<int>(idx), "NetIncomeB"), "B");
      add_percent_cell(financials_table, row, 4, table.get_row_col_value(static_cast<int>(idx), "GrossMargin"));
      add_percent_cell(financials_table, row, 5, table.get_row_col_value(static_cast<int>(idx), "NetMargin"));
      add_percent_cell(financials_table, row, 6, table.get_row_col_value(static_cast<int>(idx), "ROE"));
      add_percent_cell(financials_table, row, 7, table.get_row_col_value(static_cast<int>(idx), "ROA"));
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::load_sectors
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::load_sectors()
{
  sectors_table->clear();

  add_header_cell(sectors_table, 0, 0, "Sector");
  add_header_cell(sectors_table, 0, 1, "Companies");
  add_header_cell(sectors_table, 0, 2, "Total Market Cap");
  add_header_cell(sectors_table, 0, 3, "Avg Revenue");
  add_header_cell(sectors_table, 0, 4, "Avg Gross %");
  add_header_cell(sectors_table, 0, 5, "Avg Net %");

  std::string sql =
    "SELECT c.Sector, COUNT(DISTINCT c.Ticker) AS Companies, "
    "SUM(f.MarketCap)/1e12 AS TotalMarketCapT, "
    "AVG(ff.Revenue)/1e9 AS AvgRevenueB, "
    "AVG(ff.GrossMargin) * 100 AS AvgGrossMargin, "
    "AVG(ff.NetMargin) * 100 AS AvgNetMargin "
    "FROM FactDailyStock f "
    "JOIN DimCompany c ON f.CompanyKey = c.CompanyKey "
    "LEFT JOIN FactFinancials ff ON c.CompanyKey = ff.CompanyKey "
    "WHERE c.IsCurrent = 1 "
    "AND f.DateKey = (SELECT MAX(DateKey) FROM FactDailyStock) "
    "GROUP BY c.Sector "
    "ORDER BY TotalMarketCapT DESC";

  table_t table;
  if (odbc.fetch(sql, table) == 0)
  {
    for (size_t idx = 0; idx < table.rows.size(); idx++)
    {
      int row = static_cast<int>(idx) + 1;

      add_cell(sectors_table, row, 0, table.get_row_col_value(static_cast<int>(idx), "Sector"));
      add_cell(sectors_table, row, 1, table.get_row_col_value(static_cast<int>(idx), "Companies"));
      add_currency_cell(sectors_table, row, 2, table.get_row_col_value(static_cast<int>(idx), "TotalMarketCapT"), "T");
      add_currency_cell(sectors_table, row, 3, table.get_row_col_value(static_cast<int>(idx), "AvgRevenueB"), "B");
      add_percent_cell(sectors_table, row, 4, table.get_row_col_value(static_cast<int>(idx), "AvgGrossMargin"));
      add_percent_cell(sectors_table, row, 5, table.get_row_col_value(static_cast<int>(idx), "AvgNetMargin"));
    }
  }
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::on_sector_changed
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::on_sector_changed()
{
  load_companies();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::on_company_changed
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::on_company_changed()
{
  load_stocks();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::on_refresh_clicked
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::on_refresh_clicked()
{
  int idx = menu->currentIndex();
  if (idx == 0) load_dashboard();
  else if (idx == 1) load_companies();
  else if (idx == 2) load_stocks();
  else if (idx == 3) load_financials();
  else if (idx == 4) load_sectors();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::add_header_cell
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::add_header_cell(Wt::WTable* table, int row, int col, const std::string& text)
{
  Wt::WTableCell* cell = table->elementAt(row, col);
  Wt::WText* txt = cell->addWidget(std::make_unique<Wt::WText>(text));

  Wt::WCssDecorationStyle header_style;
  header_style.setBackgroundColor(Wt::WColor(52, 58, 64));
  header_style.setForegroundColor(Wt::WColor(255, 255, 255));
  header_style.font().setWeight(Wt::FontWeight::Bold);
  cell->setDecorationStyle(header_style);
  cell->setPadding(Wt::WLength(8, Wt::LengthUnit::Pixel));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::add_cell
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::add_cell(Wt::WTable* table, int row, int col, const std::string& text)
{
  Wt::WTableCell* cell = table->elementAt(row, col);
  cell->addWidget(std::make_unique<Wt::WText>(text));

  Wt::WCssDecorationStyle cell_style;
  if (row % 2 == 0)
  {
    cell_style.setBackgroundColor(Wt::WColor(248, 249, 250));
  }
  else
  {
    cell_style.setBackgroundColor(Wt::WColor(255, 255, 255));
  }
  cell->setDecorationStyle(cell_style);
  cell->setPadding(Wt::WLength(6, Wt::LengthUnit::Pixel));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::add_number_cell
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::add_number_cell(Wt::WTable* table, int row, int col, const std::string& text)
{
  Wt::WTableCell* cell = table->elementAt(row, col);
  Wt::WText* txt = cell->addWidget(std::make_unique<Wt::WText>(text));

  Wt::WCssDecorationStyle cell_style;
  if (row % 2 == 0)
  {
    cell_style.setBackgroundColor(Wt::WColor(248, 249, 250));
  }
  else
  {
    cell_style.setBackgroundColor(Wt::WColor(255, 255, 255));
  }
  cell_style.setForegroundColor(Wt::WColor(0, 123, 255));
  cell->setDecorationStyle(cell_style);
  cell->setPadding(Wt::WLength(6, Wt::LengthUnit::Pixel));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::add_currency_cell
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::add_currency_cell(Wt::WTable* table, int row, int col, const std::string& text, const std::string& suffix)
{
  Wt::WTableCell* cell = table->elementAt(row, col);
  std::string formatted = "$" + text + suffix;
  Wt::WText* txt = cell->addWidget(std::make_unique<Wt::WText>(formatted));

  Wt::WCssDecorationStyle cell_style;
  if (row % 2 == 0)
  {
    cell_style.setBackgroundColor(Wt::WColor(248, 249, 250));
  }
  else
  {
    cell_style.setBackgroundColor(Wt::WColor(255, 255, 255));
  }
  cell_style.setForegroundColor(Wt::WColor(40, 167, 69));
  cell->setDecorationStyle(cell_style);
  cell->setPadding(Wt::WLength(6, Wt::LengthUnit::Pixel));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::add_percent_cell
/////////////////////////////////////////////////////////////////////////////////////////////////////

void WApplicationFinmart::add_percent_cell(Wt::WTable* table, int row, int col, const std::string& text)
{
  Wt::WTableCell* cell = table->elementAt(row, col);
  std::string formatted = text + "%";
  Wt::WText* txt = cell->addWidget(std::make_unique<Wt::WText>(formatted));

  Wt::WCssDecorationStyle cell_style;
  if (row % 2 == 0)
  {
    cell_style.setBackgroundColor(Wt::WColor(248, 249, 250));
  }
  else
  {
    cell_style.setBackgroundColor(Wt::WColor(255, 255, 255));
  }
  cell_style.setForegroundColor(Wt::WColor(111, 66, 193));
  cell->setDecorationStyle(cell_style);
  cell->setPadding(Wt::WLength(6, Wt::LengthUnit::Pixel));
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::format_number
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string WApplicationFinmart::format_number(double value)
{
  std::stringstream ss;
  ss << std::fixed << std::setprecision(0) << value;
  return ss.str();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::format_currency
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string WApplicationFinmart::format_currency(double value, const std::string& suffix)
{
  std::stringstream ss;
  ss << "$" << std::fixed << std::setprecision(2) << value << suffix;
  return ss.str();
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart::format_percent
/////////////////////////////////////////////////////////////////////////////////////////////////////

std::string WApplicationFinmart::format_percent(double value)
{
  std::stringstream ss;
  ss << std::fixed << std::setprecision(2) << value << "%";
  return ss.str();
}

