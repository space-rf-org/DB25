CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
INCLUDES = -Iinclude
LIBS = ./libpg_query.a -lpthread
BUILD_DIR = build
SRC_DIR = src
INCLUDE_DIR = include
EXAMPLE_DIR = examples

SOURCES = $(wildcard $(SRC_DIR)/*.cpp)
OBJECTS = $(SOURCES:$(SRC_DIR)/%.cpp=$(BUILD_DIR)/%.o)
LIBRARY = $(BUILD_DIR)/libpg_cpp.a
EXAMPLE = $(BUILD_DIR)/example
PLANNING_DEMO = $(BUILD_DIR)/planning_demo
PHYSICAL_DEMO = $(BUILD_DIR)/physical_demo

.PHONY: all clean library example planning_demo physical_demo install test

all: library example planning_demo physical_demo

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

clean:
	rm -rf $(BUILD_DIR)

cmake: clean
	mkdir -p $(BUILD_DIR) && cd $(BUILD_DIR) && cmake .. && make

help:
	@echo "Available targets:"
	@echo "  all      - Build library and example"
	@echo "  library  - Build static library"
	@echo "  example  - Build example program"
	@echo "  cmake    - Build using CMake"
	@echo "  install  - Install library system-wide"
	@echo "  test     - Run example program"
	@echo "  clean    - Remove build files"
	@echo "  help     - Show this help"