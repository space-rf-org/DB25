cmake_minimum_required(VERSION 3.20)
project(DB25Harness VERSION 0.1.0 LANGUAGES CXX)

# Match the rest of the DB25 stack.
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Supported toolchain: GCC 14+ (the CI baseline). Surface an older GCC as a
# clear, actionable message rather than a deep C++23 template error. A soft
# warning, not a hard error, so other C++23 compilers can still be tried at the
# user's own risk (they are not exercised in CI).
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
    message(WARNING
        "DB25 targets GCC 14+ (the supported, CI-tested toolchain); "
        "detected GCC ${CMAKE_CXX_COMPILER_VERSION}, which is older and unsupported.")
endif()

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
# The physical planner (db25-physical-plan). It vendors db25-logical-plan too,
# but its CMake guards the add_subdirectory on `NOT TARGET db25logicalplan`, so
# here it REUSES the target added just above instead of adding a second copy.
# Its own tests are off in this super-build (they run in its own CI). Linking
# db25physicalplan brings db25/physical on top of the logical-plan stack.
# ---------------------------------------------------------------------------
if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/external/db25-physical-plan/CMakeLists.txt)
    message(FATAL_ERROR
        "external/db25-physical-plan is empty. Run:\n"
        "  git submodule update --init --recursive external/db25-physical-plan")
endif()
set(DB25_PHYSICAL_BUILD_TESTS OFF CACHE BOOL "" FORCE)
add_subdirectory(external/db25-physical-plan)

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

# ---------------------------------------------------------------------------
# db25_integration: a standalone full-stack integration test covering DDL, DML
# and queries end to end, asserting the artifact each stage produces (AST,
# catalog + durable snapshot, diagnostics, transaction state). It is NOT part of
# the mutation gate, so broad structural assertions are welcome. It drives
# execute_ddl / the CatalogManager / snapshot persistence / CHECK evaluation /
# the transaction manager, which the logical-plan library does not compile in,
# so those analyzer translation units are compiled straight into this target.
# ---------------------------------------------------------------------------
set(ANALYZER_SRC_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/external/db25-logical-plan/external/analyzer/src)
add_executable(db25_integration
    integration/full_stack_integration.cpp
    ${ANALYZER_SRC_DIR}/ddl.cpp
    ${ANALYZER_SRC_DIR}/catalog_snapshot.cpp
    ${ANALYZER_SRC_DIR}/check_eval.cpp
    ${ANALYZER_SRC_DIR}/transaction.cpp)
target_link_libraries(db25_integration PRIVATE db25logicalplan)
# -Wall/-Wextra as elsewhere, but not the harness -fno-exceptions/-Werror set:
# these analyzer TUs are already -fno-exceptions-clean in their own build.
target_compile_options(db25_integration PRIVATE -fno-exceptions -Wall -Wextra)
add_test(NAME db25_integration COMMAND db25_integration)

# ---------------------------------------------------------------------------
# corpus_runner: replays a committed corpus of real SQL (harvested from
# sqllogictest, dialect-corrected to DB25's Postgres-leaning canon; see
# corpus/README.md) through the frontend. Two tiers: robustness (parse ->
# analyze without crash/UB - the safety net under the sanitizer job) and
# expectation (each statement's committed db25_parse / db25_analyze columns must
# match what the frontend produces now, so behaviour cannot drift silently).
# Statements replay per session so DDL builds the catalog later statements bind
# against. Uses the same analyzer TUs as db25_integration.
# ---------------------------------------------------------------------------
add_executable(corpus_runner
    integration/corpus_runner.cpp
    ${ANALYZER_SRC_DIR}/ddl.cpp
    ${ANALYZER_SRC_DIR}/catalog_snapshot.cpp
    ${ANALYZER_SRC_DIR}/check_eval.cpp
    ${ANALYZER_SRC_DIR}/transaction.cpp)
target_link_libraries(corpus_runner PRIVATE db25logicalplan)
target_compile_options(corpus_runner PRIVATE -fno-exceptions -Wall -Wextra)
add_test(NAME corpus_runner
         COMMAND corpus_runner ${CMAKE_CURRENT_SOURCE_DIR}/corpus/corpus.tsv)

# ---------------------------------------------------------------------------
# staged_runner: the staged-artifact s-expr harness (Phase A: plan layers). For
# each fixture in corpus/staged/, drives parse -> analyze -> bind -> optimize and
# pins the canonical s-expr of each stage artifact against the committed golden,
# reporting the FIRST diverging stage on a mismatch (per-module localization).
# See docs/layer-contracts.html. Shares the analyzer TUs with corpus_report.
# Built WITH exceptions (unlike the -fno-exceptions frontend) only because it
# uses std::filesystem to walk the fixture directory; the linked frontend TUs
# stay -fno-exceptions.
# ---------------------------------------------------------------------------
add_executable(staged_runner
    integration/staged_runner.cpp
    integration/staged_sexpr.cpp
    integration/staged_sexpr_read.cpp
    ${ANALYZER_SRC_DIR}/ddl.cpp
    ${ANALYZER_SRC_DIR}/catalog_snapshot.cpp
    ${ANALYZER_SRC_DIR}/check_eval.cpp
    ${ANALYZER_SRC_DIR}/transaction.cpp)
target_link_libraries(staged_runner PRIVATE db25physicalplan)
target_compile_options(staged_runner PRIVATE -Wall -Wextra)
target_compile_definitions(staged_runner PRIVATE
    DB25_PHYSICAL_SPEC_DIR="${CMAKE_CURRENT_SOURCE_DIR}/external/db25-physical-plan/spec")
add_test(NAME staged_runner
         COMMAND staged_runner ${CMAKE_CURRENT_SOURCE_DIR}/corpus/staged)
# Falsifiability gate: every committed plan golden must be caught (changed) by
# at least one plan mutation, and must still match the produced plan. A vacuous
# or stale golden fails this test.
add_test(NAME staged_gate
         COMMAND staged_runner --gate ${CMAKE_CURRENT_SOURCE_DIR}/corpus/staged)
# Round-trip (Phase B): read each plan golden back into a LogicalNode and
# re-serialize; write(read(golden)) must equal the golden, proving the plan
# s-expr is lossless w.r.t. the rendered fields and the reader stays in sync.
add_test(NAME staged_roundtrip
         COMMAND staged_runner --roundtrip ${CMAKE_CURRENT_SOURCE_DIR}/corpus/staged)
# Per-module injection (Phase B): read the LOGICAL golden back into a plan and
# optimize it in isolation (no binder); optimize(read(logical)) must equal the
# OPTIMIZED golden. Tests the optimizer alone on a committed input.
add_test(NAME staged_inject
         COMMAND staged_runner --inject ${CMAKE_CURRENT_SOURCE_DIR}/corpus/staged)

# ---------------------------------------------------------------------------
# corpus_report: replays corpus/showcase.sql and emits a human-readable Markdown
# report of the artifact each frontend stage produces per statement (AST,
# diagnostics, catalog effect, logical + optimized plan) - see corpus/REPORT.md.
# Run as a test it also exercises the full parse -> analyze -> bind -> optimize
# chain on every showcase statement (a crash / UB fails under the sanitizer job).
# ---------------------------------------------------------------------------
add_executable(corpus_report
    integration/corpus_report.cpp
    ${ANALYZER_SRC_DIR}/ddl.cpp
    ${ANALYZER_SRC_DIR}/catalog_snapshot.cpp
    ${ANALYZER_SRC_DIR}/check_eval.cpp
    ${ANALYZER_SRC_DIR}/transaction.cpp)
target_link_libraries(corpus_report PRIVATE db25logicalplan)
target_compile_options(corpus_report PRIVATE -fno-exceptions -Wall -Wextra)
add_test(NAME corpus_report
         COMMAND corpus_report ${CMAKE_CURRENT_SOURCE_DIR}/corpus/showcase.sql)
# The HTML emitter (AST tree + stage artifacts) shares the same replay; run it
# too so its rendering path is exercised (a crash / UB fails under sanitizers).
add_test(NAME corpus_report_html
         COMMAND corpus_report --html ${CMAKE_CURRENT_SOURCE_DIR}/corpus/showcase.sql)

# ---------------------------------------------------------------------------
# physical_integration: CI-gated cross-repo seam - drives the full pipeline
# (parse -> analyze -> bind -> optimize) and lowers the result through the pinned
# db25-physical-plan, asserting a physical plan of the expected shape. Proves the
# physical planner is wired into the umbrella; its own unit tests run in its repo.
# ---------------------------------------------------------------------------
add_executable(physical_integration integration/physical_integration.cpp)
target_link_libraries(physical_integration PRIVATE db25physicalplan)
target_compile_options(physical_integration PRIVATE -fno-exceptions -Wall -Wextra)
# Lower through the SHIPPED spec, not the built-in fallback: the goldens and this
# check should pin what the real configuration produces.
target_compile_definitions(physical_integration PRIVATE
    DB25_PHYSICAL_SPEC_DIR="${CMAKE_CURRENT_SOURCE_DIR}/external/db25-physical-plan/spec")
add_test(NAME physical_integration COMMAND physical_integration)

# ---------------------------------------------------------------------------
# stage_timer: a lab tool (not a gate) - times each frontend stage in isolation
# on the simple join query and dumps each stage artifact, now including the
# physical stage (T6). Built so it cannot rot; run by hand for the numbers.
# ---------------------------------------------------------------------------
add_executable(stage_timer
    integration/stage_timer.cpp
    integration/staged_sexpr.cpp)
target_include_directories(stage_timer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/integration)
target_link_libraries(stage_timer PRIVATE db25physicalplan)
target_compile_options(stage_timer PRIVATE -fno-exceptions -Wall -Wextra)

# Tutorial examples (docs/tutorial.md): runnable end-to-end walkthroughs.
add_subdirectory(examples)
