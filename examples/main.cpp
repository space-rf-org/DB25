#include <iostream>
#include <memory>
#include "pg_query_wrapper.hpp"
#include "database.hpp"
#include "simple_schema.hpp"
#include "query_executor.hpp"

void print_separator(const std::string& title) {
    std::cout << "\n" << std::string(50, '=') << std::endl;
    std::cout << title << std::endl;
    std::cout << std::string(50, '=') << std::endl;
}

void demonstrate_query_parsing() {
    print_separator("Query Parsing with libpg_query");
    
    pg::QueryParser parser;
    
    std::vector<std::string> test_queries = {
        "SELECT * FROM users WHERE email = 'test@example.com'",
        "INSERT INTO products (name, price) VALUES ('Test Product', 29.99)",
        "UPDATE users SET first_name = 'John' WHERE id = '123'",
        "SELECT INVALID SYNTAX", // Invalid query
        "SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id"
    };
    
    for (const auto& query : test_queries) {
        std::cout << "\nQuery: " << query << std::endl;
        
        auto result = parser.parse(query);
        if (result.is_valid) {
            std::cout << "✓ Valid SQL" << std::endl;
            
            auto fingerprint = parser.get_query_fingerprint(query);
            if (fingerprint) {
                std::cout << "Fingerprint: " << *fingerprint << std::endl;
            }
            
            auto normalized = parser.normalize(query);
            if (normalized.is_valid) {
                std::cout << "Normalized: " << normalized.normalized_query << std::endl;
            }
        } else {
            std::cout << "✗ Invalid SQL" << std::endl;
            for (const auto& error : result.errors) {
                std::cout << "Error: " << error << std::endl;
            }
        }
        std::cout << std::string(30, '-') << std::endl;
    }
}

void demonstrate_schema_generation() {
    print_separator("Database Schema Generation");
    
    auto schema = pg::create_simple_schema();
    
    std::cout << "Schema Name: " << "simple_db" << std::endl;
    std::cout << "Tables: " << schema.get_table_names().size() << std::endl;
    
    std::cout << "\nTable Names:" << std::endl;
    for (const auto& table_name : schema.get_table_names()) {
        std::cout << "- " << table_name << std::endl;
    }
    
    std::cout << "\nGenerated SQL (first 1000 chars):" << std::endl;
    auto sql = schema.generate_create_sql();
    std::cout << sql.substr(0, 1000);
    if (sql.length() > 1000) {
        std::cout << "... (truncated)";
    }
    std::cout << std::endl;
}

void demonstrate_query_validation() {
    print_separator("Query Validation and Analysis");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryExecutor executor(schema);
    
    std::vector<std::string> test_queries = {
        "SELECT * FROM users WHERE email = 'test@example.com'",
        "SELECT * FROM non_existent_table",
        "SELECT name, price FROM products WHERE category_id = 1",
        "INSERT INTO orders (user_id, total_amount) VALUES ('123', 99.99)",
        "SELECT p.name, c.name FROM products p JOIN categories c ON p.category_id = c.id"
    };
    
    for (const auto& query : test_queries) {
        std::cout << "\nQuery: " << query << std::endl;
        
        auto validation = executor.validate_query(query);
        if (validation.is_valid) {
            std::cout << "✓ Valid against schema" << std::endl;
        } else {
            std::cout << "✗ Invalid against schema" << std::endl;
            for (const auto& error : validation.errors) {
                std::cout << "Error: " << error << std::endl;
            }
        }
        
        for (const auto& warning : validation.warnings) {
            std::cout << "Warning: " << warning << std::endl;
        }
        
        if (!validation.fingerprint.empty()) {
            std::cout << "Fingerprint: " << validation.fingerprint << std::endl;
        }
        
        auto analysis = executor.analyze_query(query);
        std::cout << "Query Type: " << analysis.query_type << std::endl;
        std::cout << "Modifies Data: " << (analysis.modifies_data ? "Yes" : "No") << std::endl;
        std::cout << "Requires Transaction: " << (analysis.requires_transaction ? "Yes" : "No") << std::endl;
        
        if (!analysis.referenced_tables.empty()) {
            std::cout << "Referenced Tables: ";
            for (size_t i = 0; i < analysis.referenced_tables.size(); ++i) {
                std::cout << analysis.referenced_tables[i];
                if (i < analysis.referenced_tables.size() - 1) std::cout << ", ";
            }
            std::cout << std::endl;
        }
        
        std::cout << std::string(30, '-') << std::endl;
    }
}

void demonstrate_schema_inspection() {
    print_separator("Schema Inspection");
    
    auto schema = pg::create_simple_schema();
    
    auto users_table = schema.get_table("users");
    if (users_table) {
        std::cout << "Users table columns:" << std::endl;
        for (const auto& column : users_table->columns) {
            std::cout << "- " << column.name;
            if (column.primary_key) std::cout << " (PK)";
            if (column.unique) std::cout << " (UNIQUE)";
            if (!column.nullable) std::cout << " (NOT NULL)";
            if (!column.references_table.empty()) {
                std::cout << " -> " << column.references_table << "." << column.references_column;
            }
            std::cout << std::endl;
        }
    }
    
    auto products_table = schema.get_table("products");
    if (products_table) {
        std::cout << "\nProducts table indexes:" << std::endl;
        for (const auto& index : products_table->indexes) {
            std::cout << "- " << index.name << " (" << index.type << ")";
            if (index.unique) std::cout << " UNIQUE";
            std::cout << " on columns: ";
            for (size_t i = 0; i < index.columns.size(); ++i) {
                std::cout << index.columns[i];
                if (i < index.columns.size() - 1) std::cout << ", ";
            }
            std::cout << std::endl;
        }
    }
}

void demonstrate_query_optimization() {
    print_separator("Query Optimization Suggestions");
    
    auto schema = std::make_shared<pg::DatabaseSchema>(pg::create_simple_schema());
    pg::QueryExecutor executor(schema);
    
    std::vector<std::string> queries = {
        "SELECT * FROM products WHERE category_id = 1 AND price > 50",
        "SELECT p.name FROM products p JOIN categories c ON p.category_id = c.id WHERE c.name = 'Electronics'",
        "SELECT * FROM orders WHERE user_id = '123' AND status = 'pending'"
    };
    
    for (const auto& query : queries) {
        std::cout << "\nOriginal Query: " << query << std::endl;
        
        auto optimized = executor.optimize_query(query);
        if (optimized != query) {
            std::cout << "Optimized: " << optimized << std::endl;
        }
        
        auto suggestions = executor.suggest_indexes(query);
        if (!suggestions.empty()) {
            std::cout << "Index Suggestions:" << std::endl;
            for (const auto& suggestion : suggestions) {
                std::cout << "- " << suggestion << std::endl;
            }
        }
        
        std::cout << std::string(30, '-') << std::endl;
    }
}

int main() {
    try {
        std::cout << "PostgreSQL Query Library Demo" << std::endl;
        std::cout << "C++17 Project with libpg_query Integration" << std::endl;
        
        demonstrate_query_parsing();
        demonstrate_schema_generation();
        demonstrate_query_validation();
        demonstrate_schema_inspection();
        demonstrate_query_optimization();
        
        std::cout << "\nDemo completed successfully!" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}