#include "database.hpp"

namespace db25 {

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
    
    Column status_col;
    status_col.name = "status";
    status_col.type = ColumnType::VARCHAR;
    status_col.max_length = 20;
    status_col.nullable = false;
    
    Column created_date_col;
    created_date_col.name = "created_date";
    created_date_col.type = ColumnType::DATE;
    created_date_col.nullable = false;
    
    Column company_id_col;
    company_id_col.name = "company_id";
    company_id_col.type = ColumnType::INTEGER;
    company_id_col.nullable = true;
    
    Column manager_id_col;
    manager_id_col.name = "manager_id";
    manager_id_col.type = ColumnType::INTEGER;
    manager_id_col.nullable = true;
    
    Column type_col;
    type_col.name = "type";
    type_col.type = ColumnType::VARCHAR;
    type_col.max_length = 20;
    type_col.nullable = false;
    
    users.columns = {id_col, email_col, name_col, status_col, created_date_col, company_id_col, manager_id_col, type_col};
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
    
    Column user_id_col;
    user_id_col.name = "user_id";
    user_id_col.type = ColumnType::INTEGER;
    user_id_col.nullable = true;
    
    Column category_id_col;
    category_id_col.name = "category_id";
    category_id_col.type = ColumnType::INTEGER;
    category_id_col.nullable = true;
    
    Column featured_col;
    featured_col.name = "featured";
    featured_col.type = ColumnType::BOOLEAN;
    featured_col.nullable = false;
    
    Column public_col;
    public_col.name = "public";
    public_col.type = ColumnType::BOOLEAN;
    public_col.nullable = false;
    
    Column company_id_prod_col;
    company_id_prod_col.name = "company_id";
    company_id_prod_col.type = ColumnType::INTEGER;
    company_id_prod_col.nullable = true;
    
    products.columns = {prod_id_col, prod_name_col, price_col, user_id_col, category_id_col, featured_col, public_col, company_id_prod_col};
    schema.add_table(products);
    
    // Categories table
    Table categories;
    categories.name = "categories";
    categories.comment = "Product categories";
    
    Column cat_id_col;
    cat_id_col.name = "id";
    cat_id_col.type = ColumnType::INTEGER;
    cat_id_col.nullable = false;
    cat_id_col.primary_key = true;
    
    Column cat_name_col;
    cat_name_col.name = "name";
    cat_name_col.type = ColumnType::VARCHAR;
    cat_name_col.max_length = 100;
    cat_name_col.nullable = false;
    
    categories.columns = {cat_id_col, cat_name_col};
    schema.add_table(categories);
    
    // Orders table
    Table orders;
    orders.name = "orders";
    orders.comment = "Customer orders";
    
    Column order_id_col;
    order_id_col.name = "id";
    order_id_col.type = ColumnType::INTEGER;
    order_id_col.nullable = false;
    order_id_col.primary_key = true;
    
    Column order_user_id_col;
    order_user_id_col.name = "user_id";
    order_user_id_col.type = ColumnType::INTEGER;
    order_user_id_col.nullable = false;
    
    Column product_id_col;
    product_id_col.name = "product_id";
    product_id_col.type = ColumnType::INTEGER;
    product_id_col.nullable = true;
    
    Column total_col;
    total_col.name = "total";
    total_col.type = ColumnType::DECIMAL;
    total_col.nullable = false;
    
    Column order_created_date_col;
    order_created_date_col.name = "created_date";
    order_created_date_col.type = ColumnType::DATE;
    order_created_date_col.nullable = false;
    
    orders.columns = {order_id_col, order_user_id_col, product_id_col, total_col, order_created_date_col};
    schema.add_table(orders);
    
    // Featured products table (for subquery tests)
    Table featured_products;
    featured_products.name = "featured_products";
    featured_products.comment = "Featured product references";
    
    Column fp_id_col;
    fp_id_col.name = "id";
    fp_id_col.type = ColumnType::INTEGER;
    fp_id_col.nullable = false;
    fp_id_col.primary_key = true;
    
    Column fp_product_id_col;
    fp_product_id_col.name = "product_id";
    fp_product_id_col.type = ColumnType::INTEGER;
    fp_product_id_col.nullable = false;
    
    featured_products.columns = {fp_id_col, fp_product_id_col};
    schema.add_table(featured_products);
    
    return schema;
}

}