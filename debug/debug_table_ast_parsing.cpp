#include "pg_query_wrapper.hpp"
#include <iostream>
#include <nlohmann/json.hpp>

using namespace db25;

int main() {
    try {
        std::cout << "=== Debug Table AST Parsing ===" << std::endl;
        
        const std::string query = "SELECT id FROM user";
        std::cout << "Query: " << query << std::endl;
        
        // Parse the query
        const auto parse_result = pg_query_parse(query.c_str());
        if (parse_result.error) {
            std::cerr << "Parse error: " << parse_result.error->message << std::endl;
            pg_query_free_parse_result(parse_result);
            return 1;
        }
        
        const auto ast = nlohmann::json::parse(parse_result.parse_tree);
        pg_query_free_parse_result(parse_result);
        
        // Navigate to the FROM clause
        const auto& select_stmt = ast["stmts"][0]["stmt"]["SelectStmt"];
        
        std::cout << "Has fromClause: " << select_stmt.contains("fromClause") << std::endl;
        
        if (select_stmt.contains("fromClause")) {
            const auto& from_clause = select_stmt["fromClause"];
            std::cout << "fromClause is array: " << from_clause.is_array() << std::endl;
            std::cout << "fromClause size: " << from_clause.size() << std::endl;
            
            if (!from_clause.empty()) {
                const auto& from_node = from_clause[0];
                std::cout << "\nFROM node structure:" << std::endl;
                std::cout << from_node.dump(2) << std::endl;
                
                // Test the parsing logic that's failing
                std::cout << "\n=== Testing Table Name Extraction ===" << std::endl;
                std::cout << "Has RangeVar: " << from_node.contains("RangeVar") << std::endl;
                if (from_node.contains("RangeVar")) {
                    const auto& range_var = from_node["RangeVar"];
                    std::cout << "RangeVar structure:" << std::endl;
                    std::cout << range_var.dump(2) << std::endl;
                    std::cout << "Has relname: " << range_var.contains("relname") << std::endl;
                    if (range_var.contains("relname")) {
                        std::cout << "relname value: " << range_var["relname"] << std::endl;
                    }
                }
                
                std::cout << "Has relname (direct): " << from_node.contains("relname") << std::endl;
                if (from_node.contains("relname")) {
                    std::cout << "relname value (direct): " << from_node["relname"] << std::endl;
                }
            }
        }
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Exception: " << e.what() << std::endl;
        return 1;
    }
}