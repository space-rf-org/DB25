#include "database.hpp"

namespace pg {

DatabaseSchema create_simple_schema() {
    DatabaseSchema schema("simple_db");
    
    // Users table
    Table users;
    users.name = "users";
    users.comment = "User accounts";
    
    Column id_col;
    id_col.name = "id";
    id_col.type = ColumnType::INTEGER;
    id_col.nullable = false;
    id_col.primary_key = true;
    
    Column email_col;
    email_col.name = "email";
    email_col.type = ColumnType::VARCHAR;
    email_col.max_length = 255;
    email_col.nullable = false;
    email_col.unique = true;
    
    Column name_col;
    name_col.name = "name";
    name_col.type = ColumnType::VARCHAR;
    name_col.max_length = 100;
    name_col.nullable = false;
    
    users.columns = {id_col, email_col, name_col};
    schema.add_table(users);
    
    // Products table
    Table products;
    products.name = "products";
    products.comment = "Product catalog";
    
    Column prod_id_col;
    prod_id_col.name = "id";
    prod_id_col.type = ColumnType::INTEGER;
    prod_id_col.nullable = false;
    prod_id_col.primary_key = true;
    
    Column prod_name_col;
    prod_name_col.name = "name";
    prod_name_col.type = ColumnType::VARCHAR;
    prod_name_col.max_length = 255;
    prod_name_col.nullable = false;
    
    Column price_col;
    price_col.name = "price";
    price_col.type = ColumnType::DECIMAL;
    price_col.nullable = false;
    
    products.columns = {prod_id_col, prod_name_col, price_col};
    schema.add_table(products);
    
    return schema;
}

}