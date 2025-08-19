#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <functional>
#include <optional>

namespace pg {

enum class ColumnType {
    INTEGER,
    BIGINT,
    VARCHAR,
    TEXT,
    BOOLEAN,
    TIMESTAMP,
    DATE,
    DECIMAL,
    JSON,
    UUID
};

struct Column {
    std::string name;
    ColumnType type;
    size_t max_length = 0;
    bool nullable = true;
    bool primary_key = false;
    bool unique = false;
    std::string default_value;
    std::string references_table;
    std::string references_column;
};

struct Index {
    std::string name;
    std::vector<std::string> columns;
    bool unique = false;
    std::string type = "BTREE";
};

struct Table {
    std::string name;
    std::vector<Column> columns;
    std::vector<Index> indexes;
    std::string comment;
};

class DatabaseSchema {
public:
    DatabaseSchema(const std::string& name);

    void add_table(const Table& table);
    void add_index(const std::string& table_name, const Index& index);
    void add_foreign_key(const std::string& table_name, 
                        const std::string& column_name,
                        const std::string& ref_table, 
                        const std::string& ref_column);

    std::string generate_create_sql() const;
    std::string generate_drop_sql() const;
    std::vector<std::string> get_table_names() const;
    std::optional<Table> get_table(const std::string& name) const;

private:
    std::string name_;
    std::unordered_map<std::string, Table> tables_;
    
    std::string column_type_to_sql(const Column& column) const;
    std::string generate_table_sql(const Table& table) const;
    std::string generate_index_sql(const std::string& table_name, const Index& index) const;
};

}