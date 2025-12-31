cmake_minimum_required(VERSION 3.28)
project (data.warehouse)

set(CMAKE_CXX_STANDARD 17)

#//////////////////////////
# source files
#//////////////////////////

set(src)
set(src ${src} src/odbc.cc src/odbc.hh src/csv.cc src/csv.hh)

#//////////////////////////
# executable
#//////////////////////////

add_executable(etl src/etl.cc ${src})

#//////////////////////////
# link with libraries
# lib_dep contains a cascade definition of all the libraries needed to link
#//////////////////////////

set(lib_dep ${lib_dep})

#//////////////////////////
# ODBC
#//////////////////////////

if (MSVC)
  set(lib_dep ${lib_dep} odbc32.lib odbccp32.lib)
else()
  find_library(ODBC_LIBRARY NAMES odbc)
  if(ODBC_LIBRARY)
    set(lib_dep ${lib_dep} ${ODBC_LIBRARY})
  endif()
endif()

target_link_libraries(etl ${lib_dep})

if (MSVC)
  set_property(TARGET etl PROPERTY VS_DEBUGGER_COMMAND_ARGUMENTS "-S localhost -d data_warehouse")
endif()


#//////////////////////////
# data files 
#//////////////////////////

set(DATA_FILES
  "data/companies.csv"
  "data/financials.csv"
  "data/stock_data.csv"
)

message(STATUS "Copying data files to: ${CMAKE_BINARY_DIR}")
foreach(DATA_FILE ${DATA_FILES})
  file(COPY "${CMAKE_SOURCE_DIR}/${DATA_FILE}" DESTINATION ${CMAKE_BINARY_DIR})
endforeach()

#//////////////////////////
# Wt web client 
#//////////////////////////

message(STATUS "Source directory is " ${CMAKE_SOURCE_DIR})
message(STATUS "Build directory is " ${CMAKE_CURRENT_BINARY_DIR})
message(STATUS "WT_INCLUDE: " ${WT_INCLUDE})
if (MSVC)
  message(STATUS "BOOST_INCLUDE_DIR: " ${BOOST_INCLUDE_DIR})
  message(STATUS "BOOST_LIB_DIRS: " ${BOOST_LIB_DIRS})
endif()

include_directories(${BOOST_INCLUDE_DIR})
include_directories(${WT_INCLUDE})
add_definitions(-DBOOST_BIND_GLOBAL_PLACEHOLDERS)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/ext/wt-4.12.1/src)

add_executable(web src/web.cc src/web.hh ${src})

if (MSVC)
  set_property(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY VS_STARTUP_PROJECT web)
  set_property(TARGET web PROPERTY VS_DEBUGGER_COMMAND_ARGUMENTS "-S localhost -d data_warehouse --http-address=0.0.0.0 --http-port=8080  --docroot=.")
  set_property(TARGET web PROPERTY VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_BINARY_DIR}")
endif()

#//////////////////////////
# Linux/Mac
#//////////////////////////

if(UNIX AND NOT APPLE)
  set(LINUX TRUE)
endif()

if(UNIX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated -Wno-deprecated-declarations -pthread")
endif()

if(APPLE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated-declarations -Wno-deprecated-copy -Wno-deprecated-copy-dtor")
endif()

message(STATUS "Compiler flags: ${CMAKE_CXX_FLAGS}")

if(UNIX)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

#//////////////////////////
# LINUX
# order of the link libraries matters; pthread dl
#//////////////////////////

if(LINUX)
  set(lib_dep ${lib_dep} pthread dl odbc)
  set(lib_dep ${lib_dep} stdc++fs) 
  find_program(LSB_RELEASE_EXEC lsb_release)
  execute_process(COMMAND ${LSB_RELEASE_EXEC} -is OUTPUT_VARIABLE LSB_RELEASE_ID_SHORT OUTPUT_STRIP_TRAILING_WHITESPACE)
  message(STATUS "Building in " ${LSB_RELEASE_ID_SHORT})
endif()

if (MSVC)
  set(lib_dep ${lib_dep} ${CMAKE_CURRENT_SOURCE_DIR}/build/wt-4.12.1/src/Debug/wtd.lib)
  set(lib_dep ${lib_dep} ${CMAKE_CURRENT_SOURCE_DIR}/build/wt-4.12.1/src/http/Debug/wthttpd.lib)
  set(lib_dep ${lib_dep} ${BOOST_LIB_DIRS}/libboost_filesystem-vc143-mt-gd-x64-1_88.lib)
  set(lib_dep ${lib_dep} ${BOOST_LIB_DIRS}/libboost_thread-vc143-mt-gd-x64-1_88.lib)
  set(lib_dep ${lib_dep} ${BOOST_LIB_DIRS}/libboost_program_options-vc143-mt-gd-x64-1_88.lib)
  set(lib_dep ${lib_dep} ${BOOST_LIB_DIRS}/libboost_chrono-vc143-mt-gd-x64-1_88.lib)
endif()

if(APPLE)
  set(lib_dep ${lib_dep} ${CMAKE_CURRENT_SOURCE_DIR}/install/wt/lib/libwt.dylib)
  set(lib_dep ${lib_dep} ${CMAKE_CURRENT_SOURCE_DIR}/install/wt/lib/libwthttp.dylib)
  set(lib_dep ${lib_dep} ${BOOST_LIB_DIRS}/libboost_filesystem-clang-darwin17-mt-x64-1_88.a)
  set(lib_dep ${lib_dep} ${BOOST_LIB_DIRS}/libboost_thread-clang-darwin17-mt-x64-1_88.a)
  set(lib_dep ${lib_dep} ${BOOST_LIB_DIRS}/libboost_program_options-clang-darwin17-mt-x64-1_88.a)
  set(lib_dep ${lib_dep} odbc)
endif()

if (LINUX)
  find_package(Boost REQUIRED COMPONENTS filesystem thread program_options)
  set(lib_dep ${lib_dep} ${CMAKE_CURRENT_SOURCE_DIR}/install/wt/lib/libwt.so)
  set(lib_dep ${lib_dep} ${CMAKE_CURRENT_SOURCE_DIR}/install/wt/lib/libwthttp.so)
  set(lib_dep ${lib_dep} Boost::filesystem Boost::thread Boost::program_options)
endif()

#//////////////////////////
#  link
#//////////////////////////

message(STATUS "lib_dep: " ${lib_dep})
target_link_libraries(web ${lib_dep})

