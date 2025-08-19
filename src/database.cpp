#include "database.hpp"
#include <sstream>
#include <algorithm>

namespace pg {

DatabaseSchema::DatabaseSchema(const std::string& name) : name_(name) {}

void DatabaseSchema::add_table(const Table& table) {
    tables_[table.name] = table;
}

void DatabaseSchema::add_index(const std::string& table_name, const Index& index) {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        it->second.indexes.push_back(index);
    }
}

void DatabaseSchema::add_foreign_key(const std::string& table_name, 
                                    const std::string& column_name,
                                    const std::string& ref_table, 
                                    const std::string& ref_column) {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        for (auto& column : it->second.columns) {
            if (column.name == column_name) {
                column.references_table = ref_table;
                column.references_column = ref_column;
                break;
            }
        }
    }
}

std::string DatabaseSchema::generate_create_sql() const {
    std::stringstream sql;
    
    sql << "-- Database: " << name_ << "\n";
    sql << "-- Generated Schema\n\n";
    
    for (const auto& [table_name, table] : tables_) {
        sql << generate_table_sql(table) << "\n\n";
        
        for (const auto& index : table.indexes) {
            sql << generate_index_sql(table_name, index) << "\n";
        }
        
        if (!table.indexes.empty()) {
            sql << "\n";
        }
    }
    
    return sql.str();
}

std::string DatabaseSchema::generate_drop_sql() const {
    std::stringstream sql;
    
    sql << "-- Drop Database: " << name_ << "\n\n";
    
    for (const auto& [table_name, table] : tables_) {
        sql << "DROP TABLE IF EXISTS " << table_name << " CASCADE;\n";
    }
    
    return sql.str();
}

std::vector<std::string> DatabaseSchema::get_table_names() const {
    std::vector<std::string> names;
    for (const auto& [table_name, table] : tables_) {
        names.push_back(table_name);
    }
    return names;
}

std::optional<Table> DatabaseSchema::get_table(const std::string& name) const {
    auto it = tables_.find(name);
    if (it != tables_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::string DatabaseSchema::column_type_to_sql(const Column& column) const {
    std::string sql;
    
    switch (column.type) {
        case ColumnType::INTEGER:
            sql = "INTEGER";
            break;
        case ColumnType::BIGINT:
            sql = "BIGINT";
            break;
        case ColumnType::VARCHAR:
            sql = "VARCHAR(" + std::to_string(column.max_length > 0 ? column.max_length : 255) + ")";
            break;
        case ColumnType::TEXT:
            sql = "TEXT";
            break;
        case ColumnType::BOOLEAN:
            sql = "BOOLEAN";
            break;
        case ColumnType::TIMESTAMP:
            sql = "TIMESTAMP";
            break;
        case ColumnType::DATE:
            sql = "DATE";
            break;
        case ColumnType::DECIMAL:
            sql = "DECIMAL";
            break;
        case ColumnType::JSON:
            sql = "JSON";
            break;
        case ColumnType::UUID:
            sql = "UUID";
            break;
    }
    
    return sql;
}

std::string DatabaseSchema::generate_table_sql(const Table& table) const {
    std::stringstream sql;
    
    sql << "CREATE TABLE " << table.name << " (\n";
    
    std::vector<std::string> column_defs;
    std::vector<std::string> constraints;
    
    for (const auto& column : table.columns) {
        std::string column_def = "    " + column.name + " " + column_type_to_sql(column);
        
        if (!column.nullable) {
            column_def += " NOT NULL";
        }
        
        if (!column.default_value.empty()) {
            column_def += " DEFAULT " + column.default_value;
        }
        
        if (column.primary_key) {
            column_def += " PRIMARY KEY";
        }
        
        if (column.unique && !column.primary_key) {
            column_def += " UNIQUE";
        }
        
        column_defs.push_back(column_def);
        
        if (!column.references_table.empty() && !column.references_column.empty()) {
            std::string fk_constraint = "    FOREIGN KEY (" + column.name + 
                                      ") REFERENCES " + column.references_table + 
                                      "(" + column.references_column + ")";
            constraints.push_back(fk_constraint);
        }
    }
    
    for (size_t i = 0; i < column_defs.size(); ++i) {
        sql << column_defs[i];
        if (i < column_defs.size() - 1 || !constraints.empty()) {
            sql << ",";
        }
        sql << "\n";
    }
    
    for (size_t i = 0; i < constraints.size(); ++i) {
        sql << constraints[i];
        if (i < constraints.size() - 1) {
            sql << ",";
        }
        sql << "\n";
    }
    
    sql << ")";
    
    if (!table.comment.empty()) {
        sql << ";\nCOMMENT ON TABLE " << table.name << " IS '" << table.comment << "'";
    }
    
    sql << ";";
    
    return sql.str();
}

std::string DatabaseSchema::generate_index_sql(const std::string& table_name, const Index& index) const {
    std::stringstream sql;
    
    if (index.unique) {
        sql << "CREATE UNIQUE INDEX ";
    } else {
        sql << "CREATE INDEX ";
    }
    
    sql << index.name << " ON " << table_name;
    
    if (index.type != "BTREE") {
        sql << " USING " << index.type;
    }
    
    sql << " (";
    for (size_t i = 0; i < index.columns.size(); ++i) {
        sql << index.columns[i];
        if (i < index.columns.size() - 1) {
            sql << ", ";
        }
    }
    sql << ");";
    
    return sql.str();
}

}