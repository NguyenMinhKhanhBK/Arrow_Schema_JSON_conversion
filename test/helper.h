#ifndef _TEST_HELPER_H_
#define _TEST_HELPER_H_

#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/testing/extension_type.h>
#include <arrow/util/logging.h>

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

namespace helper {
std::shared_ptr<arrow::Schema> makeNullSchema()
{
    return arrow::schema({ arrow::field("nulls", arrow::null()) })
        ->WithMetadata(arrow::KeyValueMetadata::Make({ "k1", "k2", "k3" },
                                                     { "v1", "v2", "v3" }));
}

std::shared_ptr<arrow::Schema> makePrimitiveSchema()
{
    return arrow::schema({
                             arrow::field("bools", arrow::boolean()),
                             arrow::field("int8s", arrow::int8()),
                             arrow::field("int16s", arrow::int16()),
                             arrow::field("int32s", arrow::int32()),
                             arrow::field("int64s", arrow::int64()),
                             arrow::field("uint8s", arrow::uint8()),
                             arrow::field("uint16s", arrow::uint16()),
                             arrow::field("uint32s", arrow::uint32()),
                             arrow::field("uint64s", arrow::uint64()),
                             arrow::field("float16s", arrow::float16()),
                             arrow::field("float32s", arrow::float32()),
                             arrow::field("float64s", arrow::float64()),
                             arrow::field("dec128s", arrow::decimal(10, 1)),
                         })
        ->WithMetadata(arrow::KeyValueMetadata::Make({ "k1", "k2", "k3" },
                                                     { "v1", "v2", "v3" }));
}

std::shared_ptr<arrow::Schema> makeStructSchema()
{
    return arrow::schema({ arrow::field(
        "struct_nullable",
        arrow::struct_({
            arrow::field("f1", arrow::int32()), // nullable = false
            arrow::field("f2", arrow::utf8()),  // nullable = false
        })) });
}

std::shared_ptr<arrow::Schema> makeListSchema()
{
    return arrow::schema(
        { arrow::field("list_nullable", arrow::list(arrow::int32())) });
}

std::shared_ptr<arrow::Schema> makeStringSchema()
{
    return arrow::schema({
        arrow::field("strings", arrow::utf8()),
        arrow::field("bytes", arrow::binary()),
    });
}

std::shared_ptr<arrow::Schema> makeDateTimeSchema()
{
    return arrow::schema({
        arrow::field("time32ms", arrow::time32(arrow::TimeUnit::MILLI)),
        arrow::field("time32s", arrow::time32(arrow::TimeUnit::SECOND)),
        arrow::field("time64ns", arrow::time64(arrow::TimeUnit::NANO)),
        arrow::field("time64us", arrow::time64(arrow::TimeUnit::MICRO)),
        arrow::field("timestamp_s", arrow::timestamp(arrow::TimeUnit::SECOND)),
        arrow::field("timestamp_ms", arrow::timestamp(arrow::TimeUnit::MILLI)),
        arrow::field("timestamp_us", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("timestamp_ns", arrow::timestamp(arrow::TimeUnit::NANO)),
        arrow::field("date32s", arrow::date32()),
        arrow::field("date64s", arrow::date64()),
    });
}

std::shared_ptr<arrow::Schema> makeIntervalSchema()
{
    return arrow::schema({
        arrow::field("months", arrow::month_interval()),
        arrow::field("days", arrow::day_time_interval()),
        arrow::field("nanos", arrow::month_day_nano_interval()),
    });
}

std::shared_ptr<arrow::Schema> makeDurationSchema()
{
    return arrow::schema({
        arrow::field("durations_s", arrow::duration(arrow::TimeUnit::SECOND)),
        arrow::field("durations_ms", arrow::duration(arrow::TimeUnit::MILLI)),
        arrow::field("durations_us", arrow::duration(arrow::TimeUnit::MICRO)),
        arrow::field("durations_ns", arrow::duration(arrow::TimeUnit::NANO)),
    });
}

std::shared_ptr<arrow::Schema> makeMapSchema()
{
    return arrow::schema({
        arrow::field("map_int_utf8",
                     arrow::map(arrow::int32(), arrow::utf8(), true)),
    });
}

std::shared_ptr<arrow::Schema> makeExtensionSchema()
{
    return arrow::schema({arrow::field("uuid", arrow::uuid())});
}

std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> GetTestData() {
    static std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> hashMap{};
    hashMap["nulls"] = makeNullSchema();
    hashMap["primitives"] = makePrimitiveSchema();
    hashMap["structs"] = makeStructSchema();
    hashMap["lists"] = makeListSchema();
    hashMap["strings"] = makeStringSchema();
    hashMap["datetimes"] = makeDateTimeSchema();
    hashMap["intervals"] = makeIntervalSchema();
    hashMap["durations"] = makeDurationSchema();
    hashMap["maps"] = makeMapSchema();
    hashMap["extensions"] = makeExtensionSchema();
    return hashMap;
}

} // namespace helper

#endif // _TEST_HELPER_H_
