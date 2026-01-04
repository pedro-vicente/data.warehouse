#ifndef SSL_READ_HH
#define SSL_READ_HH

#include <string>
#include <vector>

/////////////////////////////////////////////////////////////////////////////////////////////////////
// ssl_read
// performs HTTPS request using SSL/TLS
//
// parameters:
//   host     - server hostname (e.g., "www.alphavantage.co")
//   port_num - port number (typically "443" for HTTPS)
//   http     - full HTTP request string
//   response - (output) response body
//   headers  - (output) response headers
//   verbose  - if true, print debug output to stdout (default: false)
//
// returns:
//   0 on success, -1 on error
/////////////////////////////////////////////////////////////////////////////////////////////////////

int ssl_read(const std::string& host, const std::string& port_num, const std::string& http,
  std::string& response, std::vector<std::string>& headers, bool verbose = false);

#endif
