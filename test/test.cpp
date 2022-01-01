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
        std::cout << "Running test " << data.first << std::endl;
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

        ASSERT_TRUE(schemaJson.ValueOrDie() == newJson.ValueOrDie());
    }
};

/*
TEST(SchemaToJSON, BasicTypes)
{
    GTEST_SKIP();
    auto schema =
        arrow::schema({ arrow::field("IntField", arrow::int32()),
                        arrow::field("ExtensionField", arrow::uuid())
                            ->WithMetadata(arrow::KeyValueMetadata::Make(
                                { "k1", "k2" }, { "v1", "v2" })),
                        arrow::field("FloatField", arrow::float32()),
                        arrow::field("DurationField",
                                     arrow::duration(arrow::TimeUnit::SECOND))
                            ->WithMetadata(arrow::KeyValueMetadata::Make(
                                { "k1", "k2" }, { "v1", "v2" })) })
            ->WithMetadata(arrow::KeyValueMetadata::Make(
                { "key1", "key2", "key3" }, { "value1", "value2", "value3" }));

    json expected{
        "schema",
        {
            "fields",
            {
                { { "name", "IntField" },
                  {
                      "type",
                      {
                          { "name", "int" },
                          { "isSigned", true },
                          { "bitWidth", 8 },
                      },
                  },
                  { "nullable", true } },
                { { "name", "FloatField" },
                  {
                      "type",
                      {
                          { "name", "float" },
                          { "precision", "SINGLE" },
                      },
                  },
                  { "nullable", true } },
            },
        },
    };

    auto actual = converter::SchemaToJSON(schema);

    // std::cout << std::setw(4) << expected << std::endl;
    std::cout << std::setw(4) << std::move(actual).ValueOrDie() << std::endl;
}

TEST(JSONToSchema, BasicTest) {
    GTEST_SKIP();
    auto schema =
        arrow::schema({ arrow::field("IntField", arrow::int32()),
                        arrow::field("FloatField", arrow::float32()),
                        arrow::field("DurationField",
                                     arrow::duration(arrow::TimeUnit::SECOND))
                            ->WithMetadata(arrow::KeyValueMetadata::Make(
                                { "k1", "k2" }, { "v1", "v2" })) })
            ->WithMetadata(arrow::KeyValueMetadata::Make(
                { "key1", "key2", "key3" }, { "value1", "value2", "value3" }));

    auto js = converter::SchemaToJSON(schema).ValueOrDie();
    std::cout << "Origin JSON\n";
    std::cout << std::setw(4) << js << std::endl;
    auto newSchema = converter::JSONToSchema(js).ValueOrDie();
    auto newJs = converter::SchemaToJSON(newSchema).ValueOrDie();
    std::cout << "New JSON\n";
    std::cout << std::setw(4) << js << std::endl;
    ASSERT_EQ(js, newJs);
    ASSERT_TRUE(schema->Equals(newSchema));
}

TEST(SchemaToJSON, ListType) {
    GTEST_SKIP();
    auto listField1 = arrow::field("ListField1", arrow::list(arrow::int32()));
    auto listField2 = arrow::field("ListField1", arrow::list(arrow::field("ChildField", arrow::utf8())));
    auto schema = arrow::schema({listField1, listField2});

    auto js = converter::SchemaToJSON(schema).ValueOrDie();
    std::cout << std::setw(4) << js << std::endl;
}

TEST(SchemaToJSON, StructType)
{
    GTEST_SKIP();
    auto schema = arrow::schema({ arrow::field(
        "StructField",
        arrow::struct_(
            { arrow::field("ChildInt", arrow::int32()),
              arrow::field("ChildFloat", arrow::float32()),
              arrow::field("ChildList", arrow::list(arrow::boolean())),
              arrow::field("ChildStruct",
                           arrow::struct_({
                               arrow::field("ChildOfChild", arrow::int64()),
                           })) })) });

    auto js = converter::SchemaToJSON(schema).ValueOrDie();
    std::cout << "Original: \n" << std::setw(4) << js << std::endl;
}

TEST(SchemaToJSON, MapType) {
    GTEST_SKIP();
    auto schema = arrow::schema({
        arrow::field("MapField_Int_Utf8",
                     arrow::map(arrow::int32(), arrow::utf8())),
        arrow::field(
            "MapField_Int_Struct",
            arrow::map(arrow::int32(),
                       arrow::struct_({
                           arrow::field("ChildInt", arrow::int32()),
                           arrow::field("ChildFloat", arrow::float64()),
                       }))),
    });

    auto js = converter::SchemaToJSON(schema).ValueOrDie();
    std::cout << std::setw(4) << js << std::endl;
}

TEST(JSONToSchema, StructTest)
{
    GTEST_SKIP();
    auto schema = arrow::schema({ arrow::field(
        "StructField",
        arrow::struct_(
            { arrow::field("IntField", arrow::int32()),
              arrow::field("FloatField", arrow::float16()),
              arrow::field("ListField", arrow::list(arrow::uint8())),
              arrow::field(
                  "StructChildField",
                  arrow::struct_(
                      { arrow::field("ChildInt", arrow::uint16()),
                        arrow::field("ChildFloat", arrow::float64()) })) })) });
    auto originalJson = converter::SchemaToJSON(schema).ValueOrDie();
    auto newSchema = converter::JSONToSchema(originalJson).ValueOrDie();
    std::cout << "New Json\n" << std::setw(4) << converter::SchemaToJSON(newSchema).ValueOrDie() << std::endl;
    ASSERT_TRUE(newSchema->Equals(schema));
    ASSERT_TRUE(schema->Equals(newSchema));
}

TEST(JSONToSchema, MapTest)
{
    GTEST_SKIP();
    auto schema = arrow::schema({
        arrow::field("MapField", arrow::map(arrow::int16(), arrow::utf8())),
    });
    auto originalJson = converter::SchemaToJSON(schema).ValueOrDie();
    auto newSchema = converter::JSONToSchema(originalJson).ValueOrDie();
    std::cout << "New Json\n" << std::setw(4) << converter::SchemaToJSON(newSchema).ValueOrDie() << std::endl;
    ASSERT_TRUE(newSchema->Equals(schema));
    ASSERT_TRUE(schema->Equals(newSchema));
    // auto newJson = convertor.ToJson(newSchema);
}
*/
