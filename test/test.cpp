#include "Schema_JSON_Conversion.h"
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <iomanip>
#include <nlohmann/json.hpp>
#include "helper.h"


using json = nlohmann::json;

TEST(SchemaJSON, AllTestCases) {
    auto testData = helper::GetTestData();
    for (const auto& data : testData) {
        std::cout << "Test item: " << data.first << std::endl;
        auto schema = data.second;

        // convert schema to JSON
        auto schemaJson = converter::SchemaToJSON(schema);
        ASSERT_TRUE(schemaJson.ok());

        // convert the newly created JSON to new schema
        auto newSchema = converter::JSONToSchema(schemaJson.ValueOrDie());
        ASSERT_TRUE(newSchema.ok());

        // check if both schemas are the same
        schema->Equals(newSchema.ValueOrDie());

        // convert new schema to JSON
        auto newJson = converter::SchemaToJSON(newSchema.ValueOrDie());
        ASSERT_TRUE(newJson.ok());

        // check if both jsons are the same
        ASSERT_TRUE(schemaJson.ValueOrDie() == newJson.ValueOrDie());
    }
};
