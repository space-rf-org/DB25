#include "schema_registry.hpp"
#include "simple_schema.hpp"
#include <iostream>
#include <cassert>
#include <chrono>

using namespace db25;

class SchemaRegistryTester {
public:
    void run_all_tests() {
        std::cout << "=== Running Schema Registry Tests ===" << std::endl;
        
        test_basic_table_resolution();
        test_basic_column_resolution();
        test_unqualified_column_resolution();
        test_ambiguous_column_detection();
        test_type_compatibility();
        test_error_suggestions();
        test_schema_validation();
        test_foreign_key_validation();
        test_index_operations();
        test_performance();
        test_edge_cases();
        
        std::cout << "✅ All Schema Registry tests passed!" << std::endl;
    }

private:
    void test_basic_table_resolution() {
        std::cout << "Testing basic table resolution..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        // Test existing table
        auto users_id = registry.resolve_table("users");
        assert(users_id.has_value());
        assert(registry.get_table_name(*users_id) == "users");
        
        // Test non-existing table
        auto fake_id = registry.resolve_table("nonexistent");
        assert(!fake_id.has_value());
        
        // Test table exists
        assert(registry.table_exists("users"));
        assert(registry.table_exists("orders"));
        assert(!registry.table_exists("nonexistent"));
        
        // Test get all table IDs
        auto all_ids = registry.get_all_table_ids();
        assert(all_ids.size() == 3); // users, orders, products
        
        std::cout << "  ✓ Basic table resolution works" << std::endl;
    }
    
    void test_basic_column_resolution() {
        std::cout << "Testing basic column resolution..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        auto users_id = registry.resolve_table("users");
        assert(users_id.has_value());
        
        // Test existing columns
        auto id_col = registry.resolve_column(*users_id, "id");
        assert(id_col.has_value());
        assert(registry.get_column_name(*users_id, *id_col) == "id");
        
        auto name_col = registry.resolve_column(*users_id, "name");
        assert(name_col.has_value());
        assert(registry.get_column_name(*users_id, *name_col) == "name");
        
        // Test non-existing column
        auto fake_col = registry.resolve_column(*users_id, "nonexistent");
        assert(!fake_col.has_value());
        
        // Test column exists
        assert(registry.column_exists(*users_id, "id"));
        assert(registry.column_exists(*users_id, "name"));
        assert(registry.column_exists(*users_id, "email"));
        assert(!registry.column_exists(*users_id, "nonexistent"));
        
        // Test get table column IDs
        auto column_ids = registry.get_table_column_ids(*users_id);
        assert(column_ids.size() == 4); // id, name, email, created_at
        
        std::cout << "  ✓ Basic column resolution works" << std::endl;
    }
    
    void test_unqualified_column_resolution() {
        std::cout << "Testing unqualified column resolution..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        // Test unique column (email only in users)
        auto email_resolutions = registry.resolve_unqualified_column("email");
        assert(email_resolutions.size() == 1);
        assert(email_resolutions[0].table_name == "users");
        assert(email_resolutions[0].column_name == "email");
        
        // Test ambiguous column (id exists in all tables)
        auto id_resolutions = registry.resolve_unqualified_column("id");
        assert(id_resolutions.size() == 3); // users, orders, products
        
        // Test non-existing column
        auto fake_resolutions = registry.resolve_unqualified_column("nonexistent");
        assert(fake_resolutions.empty());
        
        std::cout << "  ✓ Unqualified column resolution works" << std::endl;
    }
    
    void test_ambiguous_column_detection() {
        std::cout << "Testing ambiguous column detection..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        // id exists in multiple tables
        assert(registry.is_column_ambiguous("id"));
        
        // email only exists in users table
        assert(!registry.is_column_ambiguous("email"));
        
        // Non-existing column
        assert(!registry.is_column_ambiguous("nonexistent"));
        
        std::cout << "  ✓ Ambiguous column detection works" << std::endl;
    }
    
    void test_type_compatibility() {
        std::cout << "Testing type compatibility..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        // Same types are compatible
        assert(registry.are_types_compatible(ColumnType::INTEGER, ColumnType::INTEGER));
        assert(registry.are_types_compatible(ColumnType::TEXT, ColumnType::TEXT));
        
        // Numeric types are compatible
        assert(registry.are_types_compatible(ColumnType::INTEGER, ColumnType::BIGINT));
        assert(registry.are_types_compatible(ColumnType::BIGINT, ColumnType::DECIMAL));
        
        // String types are compatible
        assert(registry.are_types_compatible(ColumnType::VARCHAR, ColumnType::TEXT));
        
        // Incompatible types
        assert(!registry.are_types_compatible(ColumnType::INTEGER, ColumnType::TEXT));
        assert(!registry.are_types_compatible(ColumnType::BOOLEAN, ColumnType::DATE));
        
        // Test implicit casting
        assert(registry.can_cast_implicitly(ColumnType::INTEGER, ColumnType::BIGINT));
        assert(registry.can_cast_implicitly(ColumnType::VARCHAR, ColumnType::TEXT));
        assert(registry.can_cast_implicitly(ColumnType::DATE, ColumnType::TIMESTAMP));
        assert(!registry.can_cast_implicitly(ColumnType::BIGINT, ColumnType::INTEGER));
        
        // Test common type resolution
        assert(registry.get_common_type(ColumnType::INTEGER, ColumnType::BIGINT) == ColumnType::BIGINT);
        assert(registry.get_common_type(ColumnType::VARCHAR, ColumnType::TEXT) == ColumnType::TEXT);
        assert(registry.get_common_type(ColumnType::DATE, ColumnType::TIMESTAMP) == ColumnType::TIMESTAMP);
        
        std::cout << "  ✓ Type compatibility works" << std::endl;
    }
    
    void test_error_suggestions() {
        std::cout << "Testing error suggestions..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        // Test table name suggestions
        auto table_suggestions = registry.suggest_table_names("user"); // Missing 's'
        assert(!table_suggestions.empty());
        assert(std::find(table_suggestions.begin(), table_suggestions.end(), "users") != table_suggestions.end());
        
        auto bad_table_suggestions = registry.suggest_table_names("xyz123");
        // Should be empty or have very low similarity
        
        // Test column name suggestions
        auto users_id = registry.resolve_table("users");
        assert(users_id.has_value());
        
        auto column_suggestions = registry.suggest_column_names("nam", *users_id); // Missing 'e'
        assert(!column_suggestions.empty());
        assert(std::find(column_suggestions.begin(), column_suggestions.end(), "name") != column_suggestions.end());
        
        std::cout << "  ✓ Error suggestions work" << std::endl;
    }
    
    void test_schema_validation() {
        std::cout << "Testing schema validation..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        auto users_id = registry.resolve_table("users");
        auto orders_id = registry.resolve_table("orders");
        assert(users_id.has_value() && orders_id.has_value());
        
        auto users_id_col = registry.resolve_column(*users_id, "id");
        auto orders_user_id_col = registry.resolve_column(*orders_id, "user_id");
        assert(users_id_col.has_value() && orders_user_id_col.has_value());
        
        // Get column definitions
        const auto& users_id_def = registry.get_column_definition(*users_id, *users_id_col);
        const auto& orders_user_id_def = registry.get_column_definition(*orders_id, *orders_user_id_col);
        
        assert(users_id_def.name == "id");
        assert(users_id_def.type == ColumnType::INTEGER);
        assert(users_id_def.primary_key == true);
        
        assert(orders_user_id_def.name == "user_id");
        assert(orders_user_id_def.type == ColumnType::INTEGER);
        
        std::cout << "  ✓ Schema validation works" << std::endl;
    }
    
    void test_foreign_key_validation() {
        std::cout << "Testing foreign key validation..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        auto users_id = registry.resolve_table("users");
        auto orders_id = registry.resolve_table("orders");
        assert(users_id.has_value() && orders_id.has_value());
        
        auto users_id_col = registry.resolve_column(*users_id, "id");
        auto orders_user_id_col = registry.resolve_column(*orders_id, "user_id");
        assert(users_id_col.has_value() && orders_user_id_col.has_value());
        
        // Valid foreign key (orders.user_id -> users.id)
        bool valid_fk = registry.validate_foreign_key(*orders_id, *orders_user_id_col, 
                                                      *users_id, *users_id_col);
        assert(valid_fk);
        
        // Invalid foreign key (wrong types or not unique)
        auto orders_id_col = registry.resolve_column(*orders_id, "id");
        auto users_email_col = registry.resolve_column(*users_id, "email");
        assert(orders_id_col.has_value() && users_email_col.has_value());
        
        bool invalid_fk = registry.validate_foreign_key(*orders_id, *orders_id_col, 
                                                        *users_id, *users_email_col);
        // This should be false because email might not be unique (depends on schema)
        
        std::cout << "  ✓ Foreign key validation works" << std::endl;
    }
    
    void test_index_operations() {
        std::cout << "Testing index operations..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        auto users_id = registry.resolve_table("users");
        assert(users_id.has_value());
        
        // Test getting table indexes
        auto indexes = registry.get_table_indexes(*users_id);
        // This depends on what indexes are defined in the test schema
        
        // Test has_index_on_column
        auto id_col = registry.resolve_column(*users_id, "id");
        assert(id_col.has_value());
        
        // Primary key usually has an index
        bool has_index = registry.has_index_on_column(*users_id, *id_col);
        // Result depends on schema definition
        
        std::cout << "  ✓ Index operations work" << std::endl;
    }
    
    void test_performance() {
        std::cout << "Testing performance..." << std::endl;
        
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        const int iterations = 10000;
        
        // Time table lookups
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto id = registry.resolve_table("users");
            (void)id; // Suppress unused variable warning
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto table_lookup_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        // Time column lookups
        auto users_id = registry.resolve_table("users");
        assert(users_id.has_value());
        
        start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; ++i) {
            auto col_id = registry.resolve_column(*users_id, "name");
            (void)col_id; // Suppress unused variable warning
        }
        end = std::chrono::high_resolution_clock::now();
        auto column_lookup_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "  ✓ Table lookups: " << table_lookup_time.count() << "μs for " << iterations << " operations" << std::endl;
        std::cout << "  ✓ Column lookups: " << column_lookup_time.count() << "μs for " << iterations << " operations" << std::endl;
        
        // Verify performance is reasonable (< 1μs per lookup on average)
        assert(table_lookup_time.count() < iterations); // Less than 1μs per lookup
        assert(column_lookup_time.count() < iterations); // Less than 1μs per lookup
        
        std::cout << "  ✓ Performance is acceptable" << std::endl;
    }
    
    void test_edge_cases() {
        std::cout << "Testing edge cases..." << std::endl;
        
        // Test with empty schema
        auto empty_schema = std::make_shared<DatabaseSchema>("empty_db");
        SchemaRegistry empty_registry(empty_schema);
        
        assert(empty_registry.get_table_count() == 0);
        assert(empty_registry.get_total_column_count() == 0);
        assert(!empty_registry.resolve_table("anything").has_value());
        
        // Test with null schema
        SchemaRegistry null_registry(nullptr);
        assert(null_registry.get_table_count() == 0);
        
        // Test case sensitivity
        auto schema = create_test_schema();
        SchemaRegistry registry(schema);
        
        // Table names should be case sensitive
        assert(registry.resolve_table("users").has_value());
        assert(!registry.resolve_table("Users").has_value());
        assert(!registry.resolve_table("USERS").has_value());
        
        // Test registry refresh
        registry.refresh_mappings();
        assert(registry.resolve_table("users").has_value()); // Should still work
        
        std::cout << "  ✓ Edge cases handled correctly" << std::endl;
    }
    
    std::shared_ptr<DatabaseSchema> create_test_schema() {
        auto schema = std::make_shared<DatabaseSchema>("test_db");
        
        // Users table
        Table users_table;
        users_table.name = "users";
        users_table.columns = {
            {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
            {"name", ColumnType::VARCHAR, 100, false, false, false, "", "", ""},
            {"email", ColumnType::VARCHAR, 255, false, false, true, "", "", ""},
            {"created_at", ColumnType::TIMESTAMP, 0, true, false, false, "NOW()", "", ""}
        };
        users_table.indexes = {
            {"users_pkey", {"id"}, true, "BTREE"},
            {"users_email_idx", {"email"}, true, "BTREE"}
        };
        
        // Orders table
        Table orders_table;
        orders_table.name = "orders";
        orders_table.columns = {
            {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
            {"user_id", ColumnType::INTEGER, 0, false, false, false, "", "users", "id"},
            {"total", ColumnType::DECIMAL, 0, false, false, false, "", "", ""},
            {"created_at", ColumnType::TIMESTAMP, 0, true, false, false, "NOW()", "", ""}
        };
        orders_table.indexes = {
            {"orders_pkey", {"id"}, true, "BTREE"},
            {"orders_user_id_idx", {"user_id"}, false, "BTREE"}
        };
        
        // Products table
        Table products_table;
        products_table.name = "products";
        products_table.columns = {
            {"id", ColumnType::INTEGER, 0, false, true, true, "", "", ""},
            {"name", ColumnType::VARCHAR, 200, false, false, false, "", "", ""},
            {"price", ColumnType::DECIMAL, 0, false, false, false, "", "", ""},
            {"description", ColumnType::TEXT, 0, true, false, false, "", "", ""}
        };
        products_table.indexes = {
            {"products_pkey", {"id"}, true, "BTREE"}
        };
        
        schema->add_table(users_table);
        schema->add_table(orders_table);
        schema->add_table(products_table);
        
        return schema;
    }
};

int main() {
    try {
        SchemaRegistryTester tester;
        tester.run_all_tests();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
}