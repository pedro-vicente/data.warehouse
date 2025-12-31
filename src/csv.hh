#ifndef READ_CSV_HH
#define READ_CSV_HH 1

#include <fstream>
#include <string>
#include <vector>

/////////////////////////////////////////////////////////////////////////////////////////////////////
//read_csv_t
/////////////////////////////////////////////////////////////////////////////////////////////////////

class read_csv_t
{
public:
  read_csv_t();
  int open(const std::string& file_name);
  void close();
  std::vector<std::string> read_row_by_comma();
  std::vector<std::string> read_row_by_tab();
private:
  std::ifstream ifs;
};

#endif
