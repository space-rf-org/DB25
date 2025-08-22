CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
INCLUDES = -Iinclude
LIBS = ./libpg_query.a -lpthread
BUILD_DIR = build
SRC_DIR = src
INCLUDE_DIR = include
EXAMPLE_DIR = examples
TEST_DIR = tests

SOURCES = $(wildcard $(SRC_DIR)/*.cpp)
OBJECTS = $(SOURCES:$(SRC_DIR)/%.cpp=$(BUILD_DIR)/%.o)
LIBRARY = $(BUILD_DIR)/libpg_cpp.a
EXAMPLE = $(BUILD_DIR)/example
PLANNING_DEMO = $(BUILD_DIR)/planning_demo
PHYSICAL_DEMO = $(BUILD_DIR)/physical_demo

# Test executables
TEST_DATABASE = $(BUILD_DIR)/test_database
TEST_QUERY_PARSER = $(BUILD_DIR)/test_query_parser
TEST_LOGICAL_PLANNER = $(BUILD_DIR)/test_logical_planner
TEST_PHYSICAL_PLANNER = $(BUILD_DIR)/test_physical_planner
TEST_PHYSICAL_EXECUTION = $(BUILD_DIR)/test_physical_execution
TEST_QUERY_EXECUTOR = $(BUILD_DIR)/test_query_executor
TEST_ALL = $(BUILD_DIR)/test_all

ALL_TESTS = $(TEST_DATABASE) $(TEST_QUERY_PARSER) $(TEST_LOGICAL_PLANNER) $(TEST_PHYSICAL_PLANNER) $(TEST_PHYSICAL_EXECUTION) $(TEST_QUERY_EXECUTOR) $(TEST_ALL)

.PHONY: all clean library example planning_demo physical_demo install test tests run-tests

all: library example planning_demo physical_demo

tests: $(ALL_TESTS)

library: $(LIBRARY)

example: $(EXAMPLE)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(LIBRARY): $(OBJECTS)
	ar rcs $@ $^

$(EXAMPLE): $(LIBRARY) $(EXAMPLE_DIR)/main.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(EXAMPLE_DIR)/main.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

planning_demo: $(PLANNING_DEMO)

$(PLANNING_DEMO): $(LIBRARY) $(EXAMPLE_DIR)/planning_demo.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(EXAMPLE_DIR)/planning_demo.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

physical_demo: $(PHYSICAL_DEMO)

$(PHYSICAL_DEMO): $(LIBRARY) $(EXAMPLE_DIR)/physical_demo.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(EXAMPLE_DIR)/physical_demo.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

# Test targets
$(TEST_DATABASE): $(LIBRARY) $(TEST_DIR)/test_database.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_DIR)/test_database.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

$(TEST_QUERY_PARSER): $(LIBRARY) $(TEST_DIR)/test_query_parser.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_DIR)/test_query_parser.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

$(TEST_LOGICAL_PLANNER): $(LIBRARY) $(TEST_DIR)/test_logical_planner.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_DIR)/test_logical_planner.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

$(TEST_PHYSICAL_PLANNER): $(LIBRARY) $(TEST_DIR)/test_physical_planner.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_DIR)/test_physical_planner.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

$(TEST_PHYSICAL_EXECUTION): $(LIBRARY) $(TEST_DIR)/test_physical_execution.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_DIR)/test_physical_execution.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

$(TEST_QUERY_EXECUTOR): $(LIBRARY) $(TEST_DIR)/test_query_executor.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_DIR)/test_query_executor.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

$(TEST_ALL): $(LIBRARY) $(TEST_DIR)/test_all.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(TEST_DIR)/test_all.cpp -L$(BUILD_DIR) -lpg_cpp $(LIBS) -o $@

install: $(LIBRARY)
	@echo "Installing library..."
	cp $(LIBRARY) /usr/local/lib/ || sudo cp $(LIBRARY) /usr/local/lib/
	cp -r $(INCLUDE_DIR)/* /usr/local/include/ || sudo cp -r $(INCLUDE_DIR)/* /usr/local/include/

test: $(EXAMPLE) $(PLANNING_DEMO) $(PHYSICAL_DEMO)
	@echo "Running example..."
	./$(EXAMPLE)
	@echo "Running planning demo..."
	./$(PLANNING_DEMO)
	@echo "Running physical demo..."
	./$(PHYSICAL_DEMO)

run-tests: $(ALL_TESTS)
	@echo "Running comprehensive test suite..."
	./$(TEST_DATABASE)
	./$(TEST_QUERY_PARSER)
	./$(TEST_LOGICAL_PLANNER)
	./$(TEST_PHYSICAL_PLANNER)
	./$(TEST_PHYSICAL_EXECUTION)
	./$(TEST_QUERY_EXECUTOR)
	@echo "\n🎉 All individual tests completed!"

test-all: $(TEST_ALL)
	@echo "Running test suite manager..."
	./$(TEST_ALL)

clean:
	rm -rf $(BUILD_DIR)

cmake: clean
	mkdir -p $(BUILD_DIR) && cd $(BUILD_DIR) && cmake .. && make

help:
	@echo "Available targets:"
	@echo "  all        - Build library and examples"
	@echo "  library    - Build static library"
	@echo "  example    - Build example program"
	@echo "  tests      - Build all test executables"
	@echo "  run-tests  - Build and run individual test suites"
	@echo "  test-all   - Build and run comprehensive test manager"
	@echo "  test       - Run examples (legacy)"
	@echo "  cmake      - Build using CMake"
	@echo "  install    - Install library system-wide"
	@echo "  clean      - Remove build files"
	@echo "  help       - Show this help"