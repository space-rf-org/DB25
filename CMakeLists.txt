cmake_minimum_required(VERSION 3.15)
project(lib_pg_cpp VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
#find_package(nlohmann_json 3.12.0 REQUIRED

include(FetchContent)

FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz)
FetchContent_MakeAvailable(json)

# Use local libpg_query library
set(LIBPQ_QUERY_LIBRARIES "${CMAKE_SOURCE_DIR}/libpg_query.a;pthread;nlohmann_json::nlohmann_json")
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

# Add warning flags for all builds to match Makefile
target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Wextra)

# Add executables matching Makefile
add_executable(example examples/main.cpp)
target_link_libraries(example ${PROJECT_NAME})

add_executable(planning_demo examples/planning_demo.cpp)
target_link_libraries(planning_demo ${PROJECT_NAME})

add_executable(physical_demo examples/physical_demo.cpp)
target_link_libraries(physical_demo ${PROJECT_NAME})

# Test executables
file(GLOB TEST_SOURCES "tests/test_*.cpp")
foreach(TEST_SOURCE ${TEST_SOURCES})
    get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)
    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME} ${PROJECT_NAME})
endforeach()

# Set optimization flags to match Makefile
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(${PROJECT_NAME} PRIVATE -g -O0)
else()
    # Use -O2 to match Makefile instead of -O3
    target_compile_options(${PROJECT_NAME} PRIVATE -O2 -DNDEBUG)
endif()

# Custom test targets
add_custom_target(run-tests
    COMMAND test_database
    COMMAND test_query_parser
    COMMAND test_logical_planner
    COMMAND test_physical_planner
    COMMAND test_physical_execution
    COMMAND test_query_executor
    DEPENDS test_database test_query_parser test_logical_planner test_physical_planner test_physical_execution test_query_executor
    COMMENT "Running all individual test suites"
)

add_custom_target(test-all
    COMMAND test_all
    DEPENDS test_all
    COMMENT "Running comprehensive test manager"
)

# Add debugging executables
add_executable(debug_ast debug_ast.cpp)
target_link_libraries(debug_ast ${PROJECT_NAME})

add_executable(test_where_ast test_where_ast.cpp)
target_link_libraries(test_where_ast ${PROJECT_NAME})

add_executable(debug_where_parsing debug_where_parsing.cpp)
target_link_libraries(debug_where_parsing ${PROJECT_NAME})

add_executable(debug_and_conditions debug_and_conditions.cpp)
target_link_libraries(debug_and_conditions ${PROJECT_NAME})

add_executable(debug_boolean_expr debug_boolean_expr.cpp)
target_link_libraries(debug_boolean_expr ${PROJECT_NAME})

add_executable(debug_parse_trace debug_parse_trace.cpp)
target_link_libraries(debug_parse_trace ${PROJECT_NAME})

add_executable(debug_bool_only debug_bool_only.cpp)
target_link_libraries(debug_bool_only ${PROJECT_NAME})