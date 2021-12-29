#include <arrow/type.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <iomanip>
#include "Schema_JSON_Conversion.h"
#include <arrow/util/key_value_metadata.h>

using json = nlohmann::json;

TEST(SchemaToJSON, BasicTypes)
{
    auto schema =
        arrow::schema({ arrow::field("IntField", arrow::int32()),
                        arrow::field("FloatField", arrow::float32()),
                        arrow::field("DurationField",
                                     arrow::duration(arrow::TimeUnit::SECOND))
                            ->WithMetadata(arrow::KeyValueMetadata::Make(
                                { "k1", "k2" }, { "v1", "v2" })) })
            ->WithMetadata(arrow::KeyValueMetadata::Make(
                { "key1", "key2", "key3" }, { "value1", "value2", "value3" }));

    json expected {
        "schema", {
            "fields", {
                {
                    {"name", "IntField"},
                    {"type", {
                            {"name", "int"},
                            {"isSigned", true},
                            {"bitWidth", 8},
                        },
                    },
                    {"nullable", true}
                },
                {
                    {"name", "FloatField"},
                    {"type", {
                            {"name", "float"},
                            {"precision", "SINGLE"},
                        },
                    },
                    {"nullable", true}
                },
            },
        },
    };

    SchemaJSONConversion convertor{};
    auto actual = convertor.ToJson(schema);


    //std::cout << std::setw(4) << expected << std::endl;
    std::cout << std::setw(4) << actual << std::endl;
}

