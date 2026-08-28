# Tutorial examples (see docs/tutorial.md).
#
# Runnable programs that drive the whole DB25 pipeline. They reuse the harness
# translation unit - which owns run() / eval() / catalog() / data() - WITHOUT its
# main via DB25_HARNESS_NO_MAIN, and link the system under test (db25logicalplan
# brings in binder + analyzer + parser + tokenizer). The whole stack is
# -fno-exceptions, so the examples match.

# db25_tour also renders the canonical plan s-expression via the staged-artifact
# writer (integration/staged_sexpr.cpp), so it shares that translation unit.
add_executable(db25_tour
    tour.cpp
    ${CMAKE_SOURCE_DIR}/harness/harness.cpp
    ${CMAKE_SOURCE_DIR}/integration/staged_sexpr.cpp)

target_include_directories(db25_tour PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/harness
    ${CMAKE_SOURCE_DIR}/integration)

target_link_libraries(db25_tour PRIVATE db25logicalplan)
target_compile_options(db25_tour PRIVATE -fno-exceptions -Wall -Wextra)
target_compile_definitions(db25_tour PRIVATE DB25_HARNESS_NO_MAIN)

add_executable(db25_dialect_features
    dialect_features.cpp
    ${CMAKE_SOURCE_DIR}/harness/harness.cpp)

target_include_directories(db25_dialect_features PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/harness)

target_link_libraries(db25_dialect_features PRIVATE db25logicalplan)
target_compile_options(db25_dialect_features PRIVATE -fno-exceptions -Wall -Wextra)
target_compile_definitions(db25_dialect_features PRIVATE DB25_HARNESS_NO_MAIN)
