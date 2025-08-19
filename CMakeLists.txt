cmake_minimum_required(VERSION 3.15)
project(lib_pg_cpp VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)

# Use local libpg_query library
set(LIBPQ_QUERY_LIBRARIES "${CMAKE_SOURCE_DIR}/libpg_query.a;pthread")
set(LIBPQ_QUERY_LIBRARY_DIRS "")
set(LIBPQ_QUERY_INCLUDE_DIRS ${CMAKE_SOURCE_DIR}/include)
set(LIBPQ_QUERY_CFLAGS_OTHER "")

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${LIBPQ_QUERY_INCLUDE_DIRS})

add_compile_options(${LIBPQ_QUERY_CFLAGS_OTHER})

file(GLOB_RECURSE SOURCES "src/*.cpp")
file(GLOB_RECURSE HEADERS "include/*.hpp")

add_library(${PROJECT_NAME} ${SOURCES} ${HEADERS})

target_link_libraries(${PROJECT_NAME} ${LIBPQ_QUERY_LIBRARIES})
target_link_directories(${PROJECT_NAME} PRIVATE ${LIBPQ_QUERY_LIBRARY_DIRS})

add_executable(example examples/main.cpp)
target_link_libraries(example ${PROJECT_NAME})

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(${PROJECT_NAME} PRIVATE -g -O0 -Wall -Wextra)
else()
    target_compile_options(${PROJECT_NAME} PRIVATE -O3 -DNDEBUG)
endif()