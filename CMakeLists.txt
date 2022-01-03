cmake_minimum_required(VERSION 3.0)
project(SchemaJSONConversion)

find_package(Arrow REQUIRED)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

##################################
# LIBRARY
##################################

set(SCHEMA_JSON_LIB "schema-json-lib")

set(PUBLIC_HEADERS 
    src/Schema_JSON_Conversion.h
)

include_directories(include)

set(SOURCES
    src/Schema_To_Json.cpp
    src/Json_To_Schema.cpp
    src/IDataType.h
    src/DataTypes.h
)

set(LIBRARIES arrow_shared)

add_library(${SCHEMA_JSON_LIB} SHARED ${SOURCES} ${PUBLIC_HEADERS})

set_target_properties(
    ${SCHEMA_JSON_LIB} 
        PROPERTIES 
            PUBLIC_HEADER "${PUBLIC_HEADERS}"
            SOVERSION "0"
            POSITION_INDEPENDENT_CODE ON
)

target_link_libraries(${SCHEMA_JSON_LIB} ${LIBRARIES})

install(TARGETS ${SCHEMA_JSON_LIB} LIBRARY DESTINATION lib)

##################################
# UNIT TEST
##################################
enable_testing()
find_package(GTest REQUIRED)
add_executable(Test
    test/test.cpp
)
target_include_directories(Test SYSTEM PUBLIC src/ test/)
target_link_libraries(Test GTest::GTest GTest::Main arrow_shared ${SCHEMA_JSON_LIB})
