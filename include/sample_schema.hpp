#pragma once

#include "database.hpp"

namespace db25 {

class ECommerceSchema {
public:
    static DatabaseSchema create_schema();
    
private:
    static Table create_users_table();
    static Table create_categories_table();
    static Table create_products_table();
    static Table create_orders_table();
    static Table create_order_items_table();
    static Table create_shopping_cart_table();
    static Table create_reviews_table();
    static Table create_addresses_table();
    static Table create_payments_table();
    static Table create_inventory_table();
};

}