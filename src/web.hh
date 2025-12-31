#ifndef FINMART_WEB_HH
#define FINMART_WEB_HH

#include <Wt/WApplication.h>
#include <Wt/WContainerWidget.h>
#include <Wt/WNavigationBar.h>
#include <Wt/WStackedWidget.h>
#include <Wt/WMenu.h>
#include <Wt/WTable.h>
#include <Wt/WText.h>
#include <Wt/WComboBox.h>
#include <Wt/WPushButton.h>
#include <Wt/WLineEdit.h>
#include <Wt/WTemplate.h>
#include <string>
#include <vector>
#include <memory>

#include "odbc.hh"

/////////////////////////////////////////////////////////////////////////////////////////////////////
// WApplicationFinmart
/////////////////////////////////////////////////////////////////////////////////////////////////////

class WApplicationFinmart : public Wt::WApplication
{
public:
  WApplicationFinmart(const Wt::WEnvironment& env);
  ~WApplicationFinmart();

private:
  odbc_t odbc;

  Wt::WNavigationBar* navbar;
  Wt::WStackedWidget* contents;
  Wt::WMenu* menu;

  Wt::WContainerWidget* dashboard_view;
  Wt::WContainerWidget* companies_view;
  Wt::WContainerWidget* stocks_view;
  Wt::WContainerWidget* financials_view;
  Wt::WContainerWidget* sectors_view;

  Wt::WTable* companies_table;
  Wt::WTable* stocks_table;
  Wt::WTable* financials_table;
  Wt::WTable* sectors_table;

  Wt::WComboBox* sector_filter;
  Wt::WComboBox* company_filter;

  void setup_navbar();
  void setup_dashboard();
  void setup_companies();
  void setup_stocks();
  void setup_financials();
  void setup_sectors();

  void load_companies();
  void load_stocks();
  void load_financials();
  void load_sectors();
  void load_dashboard();

  void on_sector_changed();
  void on_company_changed();
  void on_refresh_clicked();

  bool connect_database();
  void add_header_cell(Wt::WTable* table, int row, int col, const std::string& text);
  void add_cell(Wt::WTable* table, int row, int col, const std::string& text);
  void add_number_cell(Wt::WTable* table, int row, int col, const std::string& text);
  void add_currency_cell(Wt::WTable* table, int row, int col, const std::string& text, const std::string& suffix = "");
  void add_percent_cell(Wt::WTable* table, int row, int col, const std::string& text);

  std::string format_number(double value);
  std::string format_currency(double value, const std::string& suffix = "");
  std::string format_percent(double value);
};

#endif
