#include "logical_plan.hpp"
#include <iostream>

using namespace db25;

int main() {
    try {
        std::cout << "=== Debug TableScanNode Constructor ===" << std::endl;
        
        // Test basic constructor
        std::cout << "Testing basic constructor..." << std::endl;
        auto basic_scan = std::make_shared<TableScanNode>("test_table");
        std::cout << "Basic scan table_id has_value: " << basic_scan->table_id.has_value() << std::endl;
        
        // Test enhanced constructor
        std::cout << "Testing enhanced constructor..." << std::endl;
        std::vector<ColumnId> columns = {1, 2, 3};
        auto enhanced_scan = std::make_shared<TableScanNode>("test_table", 42, columns);
        std::cout << "Enhanced scan table_id has_value: " << enhanced_scan->table_id.has_value() << std::endl;
        if (enhanced_scan->table_id.has_value()) {
            std::cout << "Enhanced scan table_id value: " << enhanced_scan->table_id.value() << std::endl;
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}