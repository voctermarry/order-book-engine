cmake_minimum_required(VERSION 3.22)
project(order-book-engine VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

add_library(order_book_engine_core src/version.cpp)
target_include_directories(order_book_engine_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_features(order_book_engine_core PUBLIC cxx_std_20)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(order_book_engine_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_executable(order-book-engine src/main.cpp)
target_link_libraries(order-book-engine PRIVATE order_book_engine_core)

include(CTest)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()
