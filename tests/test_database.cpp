#include <iostream>
#include <cassert>
#include <vector>
#include <string>
#include "database.hpp"
#include "simple_schema.hpp"

using namespace db25;

void test_database_creation() {
    std::cout << "Testing database creation..." << std::endl;
    
    DatabaseSchema schema("test_db");
    assert(schema.get_table_names().empty());
    std::cout << "✓ Empty database creation passed" << std::endl;
}

void test_table_creation() {
    std::cout << "Testing table creation..." << std::endl;
    
    DatabaseSchema schema("test_db");
    
    // Create a test table
    Table users;
    users.name = "users";
    users.comment = "Test users table";
    
    Column id_col;
    id_col.name = "id";
    id_col.type = ColumnType::INTEGER;
    id_col.nullable = false;
    id_col.primary_key = true;
    
    Column name_col;
    name_col.name = "name";
    name_col.type = ColumnType::VARCHAR;
    name_col.max_length = 255;
    name_col.nullable = false;
    
    users.columns = {id_col, name_col};
    schema.add_table(users);
    
    auto table_names = schema.get_table_names();
    assert(table_names.size() == 1);
    assert(table_names[0] == "users");
    
    auto retrieved_table = schema.get_table("users");
    assert(retrieved_table.has_value());
    assert(retrieved_table->name == "users");
    assert(retrieved_table->columns.size() == 2);
    
    std::cout << "✓ Table creation and retrieval passed" << std::endl;
}

void test_index_creation() {
    std::cout << "Testing index creation..." << std::endl;
    
    DatabaseSchema schema("test_db");
    
    Table users;
    users.name = "users";
    
    Column id_col;
    id_col.name = "id";
    id_col.type = ColumnType::INTEGER;
    id_col.primary_key = true;
    users.columns = {id_col};
    
    schema.add_table(users);
    
    Index name_idx;
    name_idx.name = "idx_users_name";
    name_idx.columns = {"name"};
    name_idx.unique = false;
    
    schema.add_index("users", name_idx);
    
    auto table = schema.get_table("users");
    assert(table.has_value());
    assert(table->indexes.size() == 1);
    assert(table->indexes[0].name == "idx_users_name");
    
    std::cout << "✓ Index creation passed" << std::endl;
}

void test_foreign_key() {
    std::cout << "Testing foreign key creation..." << std::endl;
    
    DatabaseSchema schema("test_db");
    
    // Create parent table
    Table users;
    users.name = "users";
    Column user_id;
    user_id.name = "id";
    user_id.type = ColumnType::INTEGER;
    user_id.primary_key = true;
    users.columns = {user_id};
    schema.add_table(users);
    
    // Create child table
    Table orders;
    orders.name = "orders";
    Column order_id;
    order_id.name = "id";
    order_id.type = ColumnType::INTEGER;
    order_id.primary_key = true;
    
    Column user_id_fk;
    user_id_fk.name = "user_id";
    user_id_fk.type = ColumnType::INTEGER;
    user_id_fk.nullable = false;
    
    orders.columns = {order_id, user_id_fk};
    schema.add_table(orders);
    
    schema.add_foreign_key("orders", "user_id", "users", "id");
    
    auto orders_table = schema.get_table("orders");
    assert(orders_table.has_value());
    
    bool found_fk = false;
    for (const auto& col : orders_table->columns) {
        if (col.name == "user_id") {
            assert(col.references_table == "users");
            assert(col.references_column == "id");
            found_fk = true;
            break;
        }
    }
    assert(found_fk);
    
    std::cout << "✓ Foreign key creation passed" << std::endl;
}

void test_sql_generation() {
    std::cout << "Testing SQL generation..." << std::endl;
    
    auto schema = create_simple_schema();
    
    std::string create_sql = schema.generate_create_sql();
    assert(!create_sql.empty());
    assert(create_sql.find("CREATE TABLE") != std::string::npos);
    assert(create_sql.find("users") != std::string::npos);
    assert(create_sql.find("products") != std::string::npos);
    
    std::string drop_sql = schema.generate_drop_sql();
    assert(!drop_sql.empty());
    assert(drop_sql.find("DROP TABLE") != std::string::npos);
    
    std::cout << "✓ SQL generation passed" << std::endl;
}

void test_column_types() {
    std::cout << "Testing column type handling..." << std::endl;
    
    DatabaseSchema schema("test_db");
    Table test_table;
    test_table.name = "test_types";
    
    // Test all column types
    std::vector<std::pair<std::string, ColumnType>> type_tests = {
        {"int_col", ColumnType::INTEGER},
        {"bigint_col", ColumnType::BIGINT},
        {"varchar_col", ColumnType::VARCHAR},
        {"text_col", ColumnType::TEXT},
        {"bool_col", ColumnType::BOOLEAN},
        {"timestamp_col", ColumnType::TIMESTAMP},
        {"date_col", ColumnType::DATE},
        {"decimal_col", ColumnType::DECIMAL},
        {"json_col", ColumnType::JSON},
        {"uuid_col", ColumnType::UUID}
    };
    
    for (const auto& [name, type] : type_tests) {
        Column col;
        col.name = name;
        col.type = type;
        if (type == ColumnType::VARCHAR) {
            col.max_length = 100;
        }
        test_table.columns.push_back(col);
    }
    
    schema.add_table(test_table);
    
    std::string sql = schema.generate_create_sql();
    assert(sql.find("INTEGER") != std::string::npos);
    assert(sql.find("VARCHAR(100)") != std::string::npos);
    assert(sql.find("BOOLEAN") != std::string::npos);
    
    std::cout << "✓ Column type handling passed" << std::endl;
}

int main() {
    std::cout << "=== Database Schema Tests ===" << std::endl;
    
    try {
        test_database_creation();
        test_table_creation();
        test_index_creation();
        test_foreign_key();
        test_sql_generation();
        test_column_types();
        
        std::cout << "\n✅ All database tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "❌ Test failed with unknown exception" << std::endl;
        return 1;
    }
}