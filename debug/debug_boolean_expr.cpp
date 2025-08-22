#include <iostream>
#include "query_planner.hpp"
#include "database.hpp" 
#include "simple_schema.hpp"

using namespace db25;

int main() {
    std::string query = "SELECT * FROM users WHERE id = 123 AND status = 'active'";
    
    std::cout << "Testing boolean expression extraction: " << query << std::endl;
    
    auto schema = std::make_shared<DatabaseSchema>(create_simple_schema());
    QueryPlanner planner(schema);
    
    // Test the method directly
    auto conditions = planner.extract_where_conditions_from_ast(query);
    
    std::cout << "Extracted " << conditions.size() << " conditions:" << std::endl;
    
    for (size_t i = 0; i < conditions.size(); ++i) {
        std::cout << "Condition " << (i + 1) << ": ";
        if (conditions[i]) {
            std::cout << "Type=" << static_cast<int>(conditions[i]->type) << ", Value='" << conditions[i]->value << "'";
            std::cout << ", Children=" << conditions[i]->children.size() << std::endl;
            
            // Print children
            for (size_t j = 0; j < conditions[i]->children.size(); ++j) {
                std::cout << "  Child " << (j + 1) << ": ";
                if (conditions[i]->children[j]) {
                    std::cout << "Type=" << static_cast<int>(conditions[i]->children[j]->type) 
                              << ", Value='" << conditions[i]->children[j]->value << "'" << std::endl;
                } else {
                    std::cout << "NULL" << std::endl;
                }
            }
        } else {
            std::cout << "NULL" << std::endl;
        }
    }
    
    return 0;
}