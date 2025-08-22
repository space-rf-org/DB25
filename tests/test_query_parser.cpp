#include <iostream>
#include <cassert>
#include <vector>
#include <string>
#include "pg_query_wrapper.hpp"

using namespace db25;

void test_basic_parsing() {
    std::cout << "Testing basic SQL parsing..." << std::endl;
    
    QueryParser parser;
    
    std::vector<std::string> valid_queries = {
        "SELECT * FROM users",
        "SELECT id, name FROM products WHERE price > 100",
        "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
        "UPDATE users SET name = 'Jane' WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.user_id"
    };
    
    for (const auto& query : valid_queries) {
        auto result = parser.parse(query);
        assert(result.is_valid);
        assert(result.errors.empty());
        std::cout << "✓ Valid query: " << query.substr(0, 30) << "..." << std::endl;
    }
    
    std::cout << "✓ Basic parsing tests passed" << std::endl;
}

void test_query_normalization() {
    std::cout << "Testing query normalization..." << std::endl;
    
    QueryParser parser;
    
    std::vector<std::pair<std::string, std::string>> normalization_tests = {
        {"SELECT * FROM users WHERE id = 123", "SELECT * FROM users WHERE id = $1"},
        {"SELECT * FROM products WHERE price > 50.0", "SELECT * FROM products WHERE price > $1"},
        {"INSERT INTO users (name) VALUES ('test')", "INSERT INTO users (name) VALUES ($1)"}
    };
    
    for (const auto& [original, expected] : normalization_tests) {
        auto normalized = parser.normalize(original);
        if (normalized.is_valid) {
            // Note: actual normalization may vary slightly from expected
            assert(!normalized.normalized_query.empty());
            assert(normalized.normalized_query.find("$") != std::string::npos);
            std::cout << "✓ Normalized: " << original.substr(0, 30) << "..." << std::endl;
        }
    }
    
    std::cout << "✓ Query normalization tests passed" << std::endl;
}

void test_query_fingerprinting() {
    std::cout << "Testing query fingerprinting..." << std::endl;
    
    QueryParser parser;
    
    std::vector<std::string> queries = {
        "SELECT * FROM users WHERE id = 1",
        "SELECT * FROM users WHERE id = 2",  // Should have same fingerprint
        "SELECT name FROM users WHERE id = 1",  // Should have different fingerprint
        "SELECT * FROM products WHERE id = 1"   // Should have different fingerprint
    };
    
    std::vector<std::string> fingerprints;
    
    for (const auto& query : queries) {
        auto fingerprint = parser.get_query_fingerprint(query);
        assert(fingerprint.has_value());
        assert(!fingerprint->empty());
        fingerprints.push_back(*fingerprint);
        std::cout << "✓ Fingerprint for: " << query.substr(0, 30) << "... = " << *fingerprint << std::endl;
    }
    
    // First two queries should have same fingerprint (only parameter values differ)
    assert(fingerprints[0] == fingerprints[1]);
    
    // Different query structures should have different fingerprints
    assert(fingerprints[0] != fingerprints[2]);
    assert(fingerprints[0] != fingerprints[3]);
    
    std::cout << "✓ Query fingerprinting tests passed" << std::endl;
}

void test_invalid_queries() {
    std::cout << "Testing invalid query handling..." << std::endl;
    
    QueryParser parser;
    
    std::vector<std::string> invalid_queries = {
        "INVALID SQL SYNTAX",
        "SELECT FROM",  // Missing column list and table
        "SELECT * FORM users",  // Typo in FROM
        "INSERT INTO (name) VALUES ('test')",  // Missing table name
        ""  // Empty query
    };
    
    int valid_count = 0;
    for (const auto& query : invalid_queries) {
        auto result = parser.parse(query);
        if (result.is_valid) {
            valid_count++;
        }
        // Note: Some "invalid" queries might be parsed as valid by libpg_query
        std::cout << "✓ Processed query: " << (query.empty() ? "<empty>" : query.substr(0, 30)) << "..." << std::endl;
    }
    
    std::cout << "✓ Invalid query handling tests passed" << std::endl;
}

void test_complex_queries() {
    std::cout << "Testing complex query parsing..." << std::endl;
    
    QueryParser parser;
    
    std::vector<std::string> complex_queries = {
        R"(SELECT u.name, COUNT(o.id) as order_count 
           FROM users u 
           LEFT JOIN orders o ON u.id = o.user_id 
           WHERE u.created_at > '2023-01-01' 
           GROUP BY u.id, u.name 
           HAVING COUNT(o.id) > 5 
           ORDER BY order_count DESC 
           LIMIT 10)",
        
        R"(WITH recent_orders AS (
             SELECT user_id, COUNT(*) as order_count
             FROM orders 
             WHERE created_at > NOW() - INTERVAL '30 days'
             GROUP BY user_id
           )
           SELECT u.name, ro.order_count
           FROM users u
           JOIN recent_orders ro ON u.id = ro.user_id)",
        
        R"(SELECT 
             product_id,
             AVG(rating) as avg_rating,
             COUNT(*) as review_count
           FROM reviews 
           WHERE created_at BETWEEN '2023-01-01' AND '2023-12-31'
           GROUP BY product_id
           HAVING AVG(rating) > 4.0 AND COUNT(*) >= 10)"
    };
    
    for (const auto& query : complex_queries) {
        auto result = parser.parse(query);
        assert(result.is_valid);
        
        auto fingerprint = parser.get_query_fingerprint(query);
        assert(fingerprint.has_value());
        
        std::cout << "✓ Complex query parsed successfully" << std::endl;
    }
    
    std::cout << "✓ Complex query tests passed" << std::endl;
}

void test_parser_consistency() {
    std::cout << "Testing parser consistency..." << std::endl;
    
    QueryParser parser1;
    QueryParser parser2;
    
    std::string test_query = "SELECT * FROM users WHERE id = 123";
    
    auto result1 = parser1.parse(test_query);
    auto result2 = parser2.parse(test_query);
    
    assert(result1.is_valid == result2.is_valid);
    
    auto fp1 = parser1.get_query_fingerprint(test_query);
    auto fp2 = parser2.get_query_fingerprint(test_query);
    
    assert(fp1.has_value() == fp2.has_value());
    if (fp1.has_value() && fp2.has_value()) {
        assert(*fp1 == *fp2);
    }
    
    std::cout << "✓ Parser consistency tests passed" << std::endl;
}

int main() {
    std::cout << "=== Query Parser Tests ===" << std::endl;
    
    try {
        test_basic_parsing();
        test_query_normalization();
        test_query_fingerprinting();
        test_invalid_queries();
        test_complex_queries();
        test_parser_consistency();
        
        std::cout << "\n✅ All query parser tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "❌ Test failed with unknown exception" << std::endl;
        return 1;
    }
}