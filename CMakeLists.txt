####################################################################################################

cmake_minimum_required      (VERSION 3.0)
project                     (backtest)

set                         (CMAKE_CXX_STANDARD 14)
set                         (CMAKE_BUILD_TYPE Release)

set                         (Boost_USE_STATIC_LIBS ON)
find_package                (Boost 1.54 REQUIRED COMPONENTS date_time program_options system)

add_executable              (backtest backtest.cpp)
include_directories         (${Boost_INCLUDE})
target_link_libraries       (backtest ${Boost_LIBRARIES})

####################################################################################################