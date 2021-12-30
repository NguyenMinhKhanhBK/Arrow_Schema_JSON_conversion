#include "Schema_JSON_Conversion.h"
#include <arrow/extension_type.h>
#include <arrow/testing/extension_type.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/util/logging.h>
#include <gtest/gtest.h>
#include <iomanip>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

std::shared_ptr<arrow::DataType> arrow::uuid()
{
    return std::make_shared<arrow::UuidType>();
}

bool arrow::UuidType::ExtensionEquals(const ExtensionType& other) const
{
    return (other.extension_name() == this->extension_name());
}

std::shared_ptr<arrow::Array> arrow::UuidType::MakeArray(
    std::shared_ptr<ArrayData> data) const
{
    DCHECK_EQ(data->type->id(), Type::EXTENSION);
    DCHECK_EQ("uuid",
              static_cast<const ExtensionType&>(*data->type).extension_name());
    return std::make_shared<UuidArray>(data);
}

arrow::Result<std::shared_ptr<arrow::DataType>> arrow::UuidType::Deserialize(
    std::shared_ptr<DataType> storage_type,
    const std::string& serialized) const
{
    if (serialized != "uuid-serialized") {
        return Status::Invalid(
            "Type identifier did not match: '", serialized, "'");
    }
    if (!storage_type->Equals(*fixed_size_binary(16))) {
        return Status::Invalid("Invalid storage type for UuidType: ",
                               storage_type->ToString());
    }
    return std::make_shared<UuidType>();
}

TEST(SchemaToJSON, BasicTypes)
{
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

    SchemaJSONConversion convertor{};
    auto actual = convertor.ToJson(schema);

    // std::cout << std::setw(4) << expected << std::endl;
    std::cout << std::setw(4) << actual << std::endl;
}
