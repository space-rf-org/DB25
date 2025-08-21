#include <iostream>
#include <cstdlib>
#include <vector>
#include <string>

// Function declarations for individual test suites
extern int run_database_tests();
extern int run_query_parser_tests();
extern int run_logical_planner_tests();
extern int run_physical_planner_tests();
extern int run_physical_execution_tests();
extern int run_query_executor_tests();

struct TestSuite {
    std::string name;
    int (*run_tests)();
};

int run_database_tests() {
    std::cout << "Running database tests..." << std::endl;
    // Try relative path first (for Make), then current directory (for CMake)
    int result = system("build/test_database") >> 8;
    if (result == 127) {  // Command not found
        result = system("./test_database") >> 8;
    }
    return result;
}

int run_query_parser_tests() {
    std::cout << "Running query parser tests..." << std::endl;
    int result = system("build/test_query_parser") >> 8;
    if (result == 127) {
        result = system("./test_query_parser") >> 8;
    }
    return result;
}

int run_logical_planner_tests() {
    std::cout << "Running logical planner tests..." << std::endl;
    int result = system("build/test_logical_planner") >> 8;
    if (result == 127) {
        result = system("./test_logical_planner") >> 8;
    }
    return result;
}

int run_physical_planner_tests() {
    std::cout << "Running physical planner tests..." << std::endl;
    int result = system("build/test_physical_planner") >> 8;
    if (result == 127) {
        result = system("./test_physical_planner") >> 8;
    }
    return result;
}

int run_physical_execution_tests() {
    std::cout << "Running physical execution tests..." << std::endl;
    int result = system("build/test_physical_execution") >> 8;
    if (result == 127) {
        result = system("./test_physical_execution") >> 8;
    }
    return result;
}

int run_query_executor_tests() {
    std::cout << "Running query executor tests..." << std::endl;
    int result = system("build/test_query_executor") >> 8;
    if (result == 127) {
        result = system("./test_query_executor") >> 8;
    }
    return result;
}

int main(int argc, char* argv[]) {
    std::cout << "========================================" << std::endl;
    std::cout << "DB25 Comprehensive Test Suite" << std::endl;
    std::cout << "========================================" << std::endl;
    
    std::vector<TestSuite> test_suites = {
        {"Database Schema", run_database_tests},
        {"Query Parser", run_query_parser_tests},
        {"Logical Planner", run_logical_planner_tests},
        {"Physical Planner", run_physical_planner_tests},
        {"Physical Execution", run_physical_execution_tests},
        {"Query Executor", run_query_executor_tests}
    };
    
    int total_tests = test_suites.size();
    int passed_tests = 0;
    int failed_tests = 0;
    
    // Check if specific test is requested
    std::string specific_test;
    if (argc > 1) {
        specific_test = argv[1];
    }
    
    for (const auto& suite : test_suites) {
        // Skip if specific test requested and this isn't it
        if (!specific_test.empty() && suite.name.find(specific_test) == std::string::npos) {
            continue;
        }
        
        std::cout << "\n" << std::string(60, '=') << std::endl;
        std::cout << "Running " << suite.name << " Tests" << std::endl;
        std::cout << std::string(60, '=') << std::endl;
        
        int result = suite.run_tests();
        
        if (result == 0) {
            std::cout << "✅ " << suite.name << " tests PASSED" << std::endl;
            passed_tests++;
        } else {
            std::cout << "❌ " << suite.name << " tests FAILED (exit code: " << result << ")" << std::endl;
            failed_tests++;
        }
    }
    
    // Print summary
    std::cout << "\n" << std::string(60, '=') << std::endl;
    std::cout << "TEST SUITE SUMMARY" << std::endl;
    std::cout << std::string(60, '=') << std::endl;
    std::cout << "Total test suites: " << (passed_tests + failed_tests) << std::endl;
    std::cout << "Passed: " << passed_tests << std::endl;
    std::cout << "Failed: " << failed_tests << std::endl;
    
    if (failed_tests == 0) {
        std::cout << "\n🎉 ALL TESTS PASSED! 🎉" << std::endl;
        std::cout << "DB25 database system is working correctly!" << std::endl;
        return 0;
    } else {
        std::cout << "\n💥 " << failed_tests << " TEST SUITE(S) FAILED" << std::endl;
        std::cout << "Please check the individual test outputs above." << std::endl;
        return 1;
    }
}

// Alternative main for direct execution
// This allows running specific test categories
void print_usage(const char* program_name) {
    std::cout << "Usage: " << program_name << " [test_category]" << std::endl;
    std::cout << "Available test categories:" << std::endl;
    std::cout << "  database     - Database schema tests" << std::endl;
    std::cout << "  parser       - Query parser tests" << std::endl;
    std::cout << "  logical      - Logical planner tests" << std::endl;
    std::cout << "  physical     - Physical planner tests" << std::endl;
    std::cout << "  execution    - Physical execution tests" << std::endl;
    std::cout << "  executor     - Query executor tests" << std::endl;
    std::cout << "  (no args)    - Run all tests" << std::endl;
}