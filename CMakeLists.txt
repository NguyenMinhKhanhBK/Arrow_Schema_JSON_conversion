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

set(SOURCES
    src/Schema_JSON_Conversion.cpp
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
#enable_testing()
#find_package(GTest REQUIRED)
#add_executable(test test/test.cpp)
#target_link_libraries(test GTest::GTest GTest::Main)
