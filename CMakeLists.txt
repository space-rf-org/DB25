cmake_minimum_required(VERSION 3.20)
project(DB25Harness VERSION 0.1.0 LANGUAGES CXX)

# Match the rest of the DB25 stack.
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# ---------------------------------------------------------------------------
# The system under test: parser + analyzer + binder + logical optimizer.
# db25logicalplan's include dirs are PUBLIC and it links db25parser, so linking
# it alone brings in db25/plan, db25/semantic, db25/ast, db25/parser and the
# tokenizer headers.
# ---------------------------------------------------------------------------
if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/external/db25-logical-plan/CMakeLists.txt)
    message(FATAL_ERROR
        "external/db25-logical-plan is empty. Run:\n"
        "  git submodule update --init --recursive external/db25-logical-plan")
endif()
add_subdirectory(external/db25-logical-plan)

# ---------------------------------------------------------------------------
# Sources. The harness foundation plus every registered test suite. Sibling
# agents drop suites under tests/**; they register via db25::harness::test() and
# define no main(), so they GLOB straight into both executables.
# ---------------------------------------------------------------------------
file(GLOB_RECURSE HARNESS_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/harness/*.cpp)
file(GLOB_RECURSE TEST_SOURCES CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp)

set(ALL_SOURCES ${HARNESS_SOURCES} ${TEST_SOURCES})

# Include roots so every include style used by the suites resolves:
#   "harness/harness.hpp"        (corpus)      -> repo root
#   "harness.hpp"                (property)    -> harness/
#   "sql_generator.hpp"          (property)    -> tests/property/
#   "../../harness/harness.hpp"  (metamorphic) -> relative, always works
set(HARNESS_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/harness
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/property)

# The whole stack is -fno-exceptions; keep the harness consistent (the reference
# evaluator signals "unsupported" via std::optional, never by throwing).
set(HARNESS_COMPILE_OPTS -fno-exceptions -Wall -Wextra)

# db25_harness: run the full registered suite once (clean unless DB25_MUTANT set).
add_executable(db25_harness ${ALL_SOURCES})
target_include_directories(db25_harness PRIVATE ${HARNESS_INCLUDE_DIRS})
target_link_libraries(db25_harness PRIVATE db25logicalplan)
target_compile_options(db25_harness PRIVATE ${HARNESS_COMPILE_OPTS})

# db25_gate: same sources, defaults to the falsifiability gate (clean-pass +
# every-mutant-caught + no-vacuous-test). --suite / --gate override at runtime.
add_executable(db25_gate ${ALL_SOURCES})
target_include_directories(db25_gate PRIVATE ${HARNESS_INCLUDE_DIRS})
target_link_libraries(db25_gate PRIVATE db25logicalplan)
target_compile_options(db25_gate PRIVATE ${HARNESS_COMPILE_OPTS})
target_compile_definitions(db25_gate PRIVATE DB25_GATE_MAIN)

enable_testing()
add_test(NAME db25_harness COMMAND db25_harness)
add_test(NAME db25_gate COMMAND db25_gate)

# Tutorial examples (docs/tutorial.md): runnable end-to-end walkthroughs.
add_subdirectory(examples)
