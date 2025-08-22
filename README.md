# DB25: HTAP Database System (Uses PostgreSQL Query Library Adapted for C++17)

DB25 is a modern Hybrid Transactional/Analytical Processing (HTAP) database system built on the PostgreSQL Query Library foundation. It provides a comprehensive C++17 implementation that unifies OLTP and OLAP workloads, featuring SQL query parsing, logical/physical planning, vectorized execution, and computational storage integration.

## Features

### Core Database Engine
- **SQL Query Parsing**: Parse and validate SQL queries using libpg_query
- **Query Normalization**: Normalize queries and generate fingerprints
- **Schema Management**: Define and manage complete database schemas
- **Query Validation**: Validate queries against defined schemas
- **Query Analysis**: Analyze query structure and dependencies
- **Index Suggestions**: Get optimization recommendations

### HTAP Query Processing Pipeline
- **Logical Planning**: Advanced query planning with cost-based optimization
- **Physical Planning**: Convert logical plans to executable physical plans
- **Physical Execution Engine**: Iterator-based execution with vectorized processing
- **Workload Classification**: Intelligent OLTP/OLAP query routing
- **Dual Storage Support**: Row-oriented (OLTP) and column-oriented (OLAP) processing

### Physical Operators
- **Scan Operators**: Sequential scan, index scan with filter pushdown
- **Join Algorithms**: Nested loop join, hash join with build/probe phases
- **Sort Operations**: In-memory and external sort algorithms
- **Aggregation**: Hash-based GROUP BY processing
- **Parallel Execution**: Multi-threaded operator execution
- **Memory Management**: Work memory limits and spill-to-disk strategies

### Advanced Features
- **Plan Visualization**: PostgreSQL-style execution plan display
- **Cost Estimation**: Sophisticated cost modeling for query optimization
- **Plan Optimization**: Predicate pushdown, join reordering, and other optimizations
- **Vectorized Execution**: Batch-based tuple processing for analytical workloads
- **Computational Storage**: Framework for near-data processing integration
- **Complete E-commerce Schema**: Ready-to-use database schema example

## Prerequisites

### Dependencies

- C++17 compatible compiler (GCC 7+, Clang 6+, MSVC 2017+)
- CMake 3.15+
- libpg_query development library
- pkg-config

### Installing libpg_query

#### Ubuntu/Debian
```bash
# Install dependencies
sudo apt-get update
sudo apt-get install build-essential cmake pkg-config

# Build and install libpg_query from source
git clone https://github.com/pganalyze/libpg_query.git
cd libpg_query
make
sudo make install
```

#### macOS
```bash
# Using Homebrew
brew install libpq
brew install pkg-config

# Build libpg_query from source
git clone https://github.com/pganalyze/libpg_query.git
cd libpg_query
make
sudo make install
```

#### CentOS/RHEL/Fedora
```bash
# Install dependencies
sudo yum install gcc gcc-c++ cmake pkg-config
# or for newer versions:
sudo dnf install gcc gcc-c++ cmake pkg-config

# Build libpg_query from source
git clone https://github.com/pganalyze/libpg_query.git
cd libpg_query
make
sudo make install
```

## Building the Project

### Using Make
```bash
# Build library and example
make all

# Build only the library
make library

# Build only the example
make example

# Run the example
make test

# Clean build files
make clean
```

### Using CMake
```bash
# Create build directory
mkdir build && cd build

# Configure and build
cmake ..
make

# Run the example
./example
```

### Alternative CMake approach
```bash
# Use the make target for CMake
make cmake
```

## Project Structure

```
lib_pg_cpp/
├── include/                 # Header files
│   ├── pg_query_wrapper.hpp    # libpg_query wrapper
│   ├── database.hpp            # Database schema classes
│   ├── simple_schema.hpp       # Simple schema example
│   ├── query_executor.hpp      # Query validation and analysis
│   ├── logical_plan.hpp        # Logical plan node definitions
│   ├── query_planner.hpp       # Advanced query planning
│   ├── physical_plan.hpp       # Physical plan node definitions
│   ├── physical_planner.hpp    # Logical to physical plan conversion
│   └── execution_engine.hpp    # Physical execution engine
├── src/                     # Source files
│   ├── pg_query_wrapper.cpp
│   ├── database.cpp
│   ├── simple_schema.cpp
│   ├── query_executor.cpp
│   ├── logical_plan.cpp
│   ├── query_planner.cpp
│   ├── physical_plan.cpp       # Physical plan implementation
│   ├── physical_planner.cpp    # Physical plan generation
│   └── execution_engine.cpp    # Execution engine implementation
├── examples/               # Example applications
│   ├── main.cpp               # Basic functionality demo
│   ├── planning_demo.cpp      # Logical planning demo
│   └── execution_demo.cpp     # Physical execution demo
├── docs/                   # Documentation
│   ├── query_engine_architecture.tex  # Comprehensive technical documentation
│   ├── build_pdf.sh          # Documentation build script
│   └── README.md             # Documentation guide
├── CMakeLists.txt         # CMake configuration
├── Makefile              # Make configuration
└── README.md            # This file
```

## Usage Examples

### Basic Query Parsing

```cpp
#include "pg_query_wrapper.hpp"

db25::QueryParser parser;

// Parse a query
auto result = parser.parse("SELECT * FROM users WHERE id = 1");
if (result.is_valid) {
    std::cout << "Valid SQL query" << std::endl;
}

// Get query fingerprint
auto fingerprint = parser.get_query_fingerprint("SELECT * FROM users WHERE id = ?");
if (fingerprint) {
    std::cout << "Fingerprint: " << *fingerprint << std::endl;
}

// Normalize query
auto normalized = parser.normalize("SELECT * FROM users WHERE id = 123");
if (normalized.is_valid) {
    std::cout << "Normalized: " << normalized.normalized_query << std::endl;
}
```

### Schema Definition and Management

```cpp
#include "database.hpp"
#include "sample_schema.hpp"

// Create a complete e-commerce schema
auto schema = db25::ECommerceSchema::create_schema();

// Generate SQL for creating tables
std::string create_sql = schema.generate_create_sql();
std::cout << create_sql << std::endl;

// Check if table exists
bool has_users = schema.get_table("users").has_value();

// Get table details
auto users_table = schema.get_table("users");
if (users_table) {
    for (const auto& column : users_table->columns) {
        std::cout << column.name << " - ";
        if (column.primary_key) std::cout << "Primary Key";
        std::cout << std::endl;
    }
}
```

### Query Validation and Analysis

```cpp
#include "query_executor.hpp"

auto schema = std::make_shared<db25::DatabaseSchema>(db25::ECommerceSchema::create_schema());
db25::QueryExecutor executor(schema);

// Validate query against schema
std::string query = "SELECT name, email FROM users WHERE active = true";
auto validation = executor.validate_query(query);

if (validation.is_valid) {
    std::cout << "Query is valid!" << std::endl;
} else {
    for (const auto& error : validation.errors) {
        std::cout << "Error: " << error << std::endl;
    }
}

// Analyze query structure
auto analysis = executor.analyze_query(query);
std::cout << "Query type: " << analysis.query_type << std::endl;
std::cout << "Modifies data: " << (analysis.modifies_data ? "Yes" : "No") << std::endl;

// Get optimization suggestions
auto suggestions = executor.suggest_indexes(query);
for (const auto& suggestion : suggestions) {
    std::cout << "Suggested index: " << suggestion << std::endl;
}
```

### Logical Query Planning

```cpp
#include "query_planner.hpp"

auto schema = std::make_shared<db25::DatabaseSchema>(db25::create_simple_schema());
db25::QueryPlanner planner(schema);

// Configure table statistics for better cost estimation
db25::TableStats user_stats;
user_stats.row_count = 10000;
user_stats.avg_row_size = 120.0;
user_stats.column_selectivity["email"] = 0.8;
planner.set_table_stats("users", user_stats);

// Generate logical plan
std::string query = "SELECT u.name, p.name FROM users u JOIN products p ON u.id = p.id WHERE u.name LIKE 'John%'";
auto plan = planner.create_plan(query);

// Display execution plan
std::cout << plan.to_string() << std::endl;

// Optimize the plan
auto optimized_plan = planner.optimize_plan(plan);
std::cout << "Optimized Plan:" << std::endl;
std::cout << optimized_plan.to_string() << std::endl;

// Generate alternative plans
auto alternatives = planner.generate_alternative_plans(query);
for (size_t i = 0; i < alternatives.size(); ++i) {
    std::cout << "Alternative " << i << ":" << std::endl;
    std::cout << alternatives[i].to_string() << std::endl;
}

// Configure planner options
db25::PlannerConfig config;
config.enable_hash_joins = true;
config.enable_merge_joins = true;
config.work_mem = 1024 * 1024; // 1MB
planner.set_config(config);
```

### Physical Planning and Execution

```cpp
#include "physical_planner.hpp"
#include "execution_engine.hpp"

auto schema = std::make_shared<db25::DatabaseSchema>(db25::create_simple_schema());
db25::QueryPlanner logical_planner(schema);
db25::PhysicalPlanner physical_planner(schema);

// Create logical plan
std::string query = "SELECT u.name, p.price FROM users u JOIN products p ON u.id = p.user_id WHERE p.price > 100";
auto logical_plan = logical_planner.create_plan(query);

// Convert to physical plan
auto physical_plan = physical_planner.create_physical_plan(logical_plan);

// Display physical execution plan
std::cout << "Physical Execution Plan:" << std::endl;
std::cout << physical_plan.to_string() << std::endl;

// Execute the plan
db25::ExecutionEngine executor(schema);
auto execution_context = executor.create_context();

// Execute and get results
auto result_iterator = executor.execute(physical_plan, execution_context);
while (auto batch = result_iterator.next()) {
    for (const auto& row : batch->rows) {
        for (const auto& value : row) {
            std::cout << value << "\t";
        }
        std::cout << std::endl;
    }
}

// Get execution statistics
auto stats = execution_context.get_statistics();
std::cout << "Execution Statistics:" << std::endl;
std::cout << "Rows processed: " << stats.rows_processed << std::endl;
std::cout << "Memory used: " << stats.memory_used_mb << " MB" << std::endl;
std::cout << "Execution time: " << stats.execution_time_ms << " ms" << std::endl;
```

### HTAP Workload Processing

```cpp
#include "htap_engine.hpp"

// Create HTAP engine with workload classification
db25::HTAPEngine htap_engine(schema);

// Configure for mixed workloads
db25::HTAPConfig config;
config.oltp_priority = 0.7;  // Prioritize transactional workloads
config.analytical_timeout_ms = 5000;
config.enable_real_time_analytics = true;
htap_engine.set_config(config);

// Execute transactional query (high priority)
std::string oltp_query = "INSERT INTO orders (user_id, total) VALUES (123, 99.99)";
auto oltp_result = htap_engine.execute_transactional(oltp_query);

// Execute analytical query (processed in parallel)
std::string olap_query = "SELECT category, AVG(price) FROM products GROUP BY category";
auto olap_result = htap_engine.execute_analytical(olap_query);

// Real-time analytics on live data
auto analytics_stream = htap_engine.create_analytics_stream("orders");
analytics_stream.add_aggregation("total_revenue", "SUM(total)");
analytics_stream.add_aggregation("order_count", "COUNT(*)");
auto live_metrics = analytics_stream.get_current_metrics();
```

## Database Schema

The library includes a complete e-commerce database schema with the following tables:

- **users**: User accounts and profiles
- **categories**: Product categories
- **products**: Product catalog with JSON attributes
- **orders**: Customer orders
- **order_items**: Items within orders
- **shopping_cart**: User shopping cart items
- **reviews**: Product reviews and ratings
- **addresses**: User addresses for shipping and billing
- **payments**: Order payment records
- **inventory**: Product inventory tracking

Each table includes appropriate:
- Primary keys (UUID or auto-increment)
- Foreign key relationships
- Indexes for performance
- Constraints and defaults
- JSON fields for flexible data

## API Reference

### QueryParser Class

- `QueryResult parse(const std::string& query)`: Parse SQL query
- `NormalizedQuery normalize(const std::string& query)`: Normalize query
- `std::optional<std::string> get_query_fingerprint(const std::string& query)`: Get query fingerprint
- `bool is_valid_sql(const std::string& query)`: Check if SQL is valid

### DatabaseSchema Class

- `void add_table(const Table& table)`: Add table to schema
- `void add_index(const std::string& table_name, const Index& index)`: Add index
- `void add_foreign_key(...)`: Add foreign key relationship
- `std::string generate_create_sql()`: Generate CREATE SQL
- `std::vector<std::string> get_table_names()`: Get all table names
- `std::optional<Table> get_table(const std::string& name)`: Get table by name

### QueryExecutor Class

- `QueryValidationResult validate_query(const std::string& query)`: Validate query
- `QueryAnalysis analyze_query(const std::string& query)`: Analyze query structure
- `std::string optimize_query(const std::string& query)`: Get optimized query
- `std::vector<std::string> suggest_indexes(const std::string& query)`: Get index suggestions
- `bool check_table_exists(const std::string& table_name)`: Check table existence
- `bool check_column_exists(...)`: Check column existence

### QueryPlanner Class

- `LogicalPlan create_plan(const std::string& query)`: Generate logical execution plan
- `LogicalPlan optimize_plan(const LogicalPlan& plan)`: Apply optimization rules
- `std::vector<LogicalPlan> generate_alternative_plans(const std::string& query)`: Generate plan alternatives
- `void estimate_costs(LogicalPlanNodePtr node)`: Estimate execution costs
- `void set_table_stats(const std::string& table_name, const TableStats& stats)`: Set table statistics
- `void set_config(const PlannerConfig& config)`: Configure planner options

### LogicalPlan Class

- `std::string to_string()`: Display plan in PostgreSQL-style format
- `void calculate_costs()`: Calculate total plan cost
- `LogicalPlan copy()`: Create deep copy of plan

### Plan Node Types

- **TableScanNode**: Sequential table scan
- **IndexScanNode**: Index-based scan with conditions
- **NestedLoopJoinNode**: Nested loop join algorithm
- **HashJoinNode**: Hash join algorithm with build/probe phases
- **ProjectionNode**: Column projection and expression evaluation
- **SelectionNode**: Filter conditions (WHERE clause)
- **AggregationNode**: GROUP BY and aggregate functions
- **SortNode**: ORDER BY sorting
- **LimitNode**: LIMIT and OFFSET operations
- **InsertNode**: INSERT operations
- **UpdateNode**: UPDATE operations
- **DeleteNode**: DELETE operations

### PhysicalPlanner Class

- `PhysicalPlan create_physical_plan(const LogicalPlan& logical_plan)`: Convert logical to physical plan
- `void set_execution_config(const ExecutionConfig& config)`: Configure execution parameters
- `std::vector<PhysicalPlan> generate_alternatives(const LogicalPlan& plan)`: Generate alternative physical plans
- `void estimate_physical_costs(PhysicalPlanNodePtr node)`: Estimate physical execution costs

### ExecutionEngine Class

- `ExecutionContext create_context()`: Create execution context
- `ResultIterator execute(const PhysicalPlan& plan, ExecutionContext& context)`: Execute physical plan
- `ExecutionStatistics get_statistics(const ExecutionContext& context)`: Get execution statistics
- `void set_memory_limit(size_t memory_mb)`: Set memory limits for execution
- `void enable_parallel_execution(int worker_threads)`: Configure parallel execution

### Physical Operator Types

- **PhysicalSequentialScanNode**: Table scan with filter pushdown
- **PhysicalIndexScanNode**: B+ tree index scan
- **PhysicalNestedLoopJoinNode**: Iterator-based nested loop join
- **PhysicalHashJoinNode**: Hash join with build/probe phases
- **PhysicalSortNode**: In-memory and external sort
- **PhysicalAggregationNode**: Hash-based GROUP BY aggregation
- **PhysicalParallelScanNode**: Multi-threaded table scan
- **PhysicalMaterializeNode**: Materialize intermediate results

### HTAP Engine Classes

- **HTAPEngine**: Main HTAP query processing engine
- **WorkloadClassifier**: Automatic OLTP/OLAP query classification
- **ResourceManager**: Dynamic resource allocation between workloads
- **ComputationalStorageInterface**: Near-data processing integration

## Running the Examples

The project includes three comprehensive example programs:

### Basic Example
```bash
# Build and run basic example
make example
./build/example
```

The basic example demonstrates:
1. SQL query parsing and validation
2. Database schema generation  
3. Query validation against schema
4. Schema inspection capabilities
5. Query optimization suggestions

### Logical Planning Demo
```bash
# Build and run planning demo
make planning_demo
./build/planning_demo
```

The planning demo demonstrates:
1. **Basic Logical Planning**: Generate execution plans for simple queries
2. **Plan Optimization**: Apply predicate pushdown and other optimization rules
3. **Alternative Plans**: Generate multiple execution strategies
4. **DML Planning**: Plan INSERT, UPDATE, and DELETE operations
5. **Cost Estimation**: Analyze costs for different table sizes and join algorithms
6. **Join Algorithm Selection**: Compare nested loop vs hash joins

### Physical Execution Demo
```bash
# Build and run execution demo
make execution_demo
./build/execution_demo
```

The execution demo demonstrates:
1. **Physical Plan Generation**: Convert logical plans to executable physical plans
2. **Iterator-Based Execution**: Volcano-style execution model with physical operators
3. **Vectorized Processing**: Batch-based tuple processing for analytical workloads
4. **Parallel Execution**: Multi-threaded query execution with worker coordination
5. **Memory Management**: Work memory limits and spill-to-disk strategies
6. **HTAP Workload Processing**: Mixed transactional and analytical query execution
7. **Performance Monitoring**: Execution statistics and resource utilization tracking

### Run All Examples
```bash
# Build and run all examples
make test
```

## Installation

To install the library system-wide:

```bash
make install
```

This will copy the static library to `/usr/local/lib/` and headers to `/usr/local/include/`.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License. See LICENSE file for details.

## Troubleshooting

### libpg_query not found
- Ensure libpg_query is properly installed
- Check that pkg-config can find libpg_query: `pkg-config --cflags --libs libpg_query`
- You may need to update your `PKG_CONFIG_PATH`

### Compilation errors
- Ensure you're using a C++17 compatible compiler
- Check that all required headers are included
- Verify libpg_query development headers are installed

### Runtime errors
- Ensure libpg_query shared library is in your library path
- You may need to run `ldconfig` after installing libpg_query