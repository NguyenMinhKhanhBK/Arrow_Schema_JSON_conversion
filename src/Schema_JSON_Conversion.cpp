#include "Schema_JSON_Conversion.h"
#include <iostream>
#include "DataTypes.h"
#include <arrow/util/key_value_metadata.h>
#include <arrow/extension_type.h>

const json marshalJSON(const std::shared_ptr<arrow::Field> field) {
    json result;
    std::shared_ptr<IDataType> type;
    auto fieldType = field->type();

    if (field->HasMetadata()) {
        auto metadata = field->metadata();
        for (int i = 0; i < metadata->size(); i++) {
            result["metadata"].push_back({
                { "key", metadata->key(i) },
                { "value", metadata->value(i) },
            });
        }
    }

    if (fieldType->id() == arrow::Type::EXTENSION) {
        auto extType = static_cast<const arrow::ExtensionType*>(fieldType.get());
        result["metadata"].push_back(
            { { "key", "ARROW:extension:name" },
              { "value", extType->extension_name() } });

        auto serializedData = extType->Serialize();
        if (serializedData.size() > 0) {
            result["metadata"].push_back(
                { { "key", "ARROW:extension:metadata" },
                  { "value", serializedData } });
        }
        fieldType = extType->storage_type();
    }

    switch(fieldType->id()) {
        case arrow::Type::NA:
            type = std::make_shared<NameJSON>("nullptr");
            break;
        case arrow::Type::BOOL:
            type = std::make_shared<NameJSON>("bool");
            break;
        case arrow::Type::UINT8:
            type = std::make_shared<BitWidthJSON>("int", false, 8);
            break;
        case arrow::Type::INT8:
            type = std::make_shared<BitWidthJSON>("int", true, 8);
            break;
        case arrow::Type::UINT16:
            type = std::make_shared<BitWidthJSON>("int", false, 16);
            break;
        case arrow::Type::INT16:
            type = std::make_shared<BitWidthJSON>("int", true, 16);
            break;
        case arrow::Type::UINT32:
            type = std::make_shared<BitWidthJSON>("int", false, 32);
            break;
        case arrow::Type::INT32:
            type = std::make_shared<BitWidthJSON>("int", true, 32);
            break;
        case arrow::Type::UINT64:
            type = std::make_shared<BitWidthJSON>("int", false, 64);
            break;
        case arrow::Type::INT64:
            type = std::make_shared<BitWidthJSON>("int", true, 64);
            break;
        case arrow::Type::HALF_FLOAT:
            type = std::make_shared<FloatJSON>("floatingpoint", "HALF");
            break;
        case arrow::Type::FLOAT:
            type = std::make_shared<FloatJSON>("floatingpoint", "SINGLE");
            break;
        case arrow::Type::DOUBLE:
            type = std::make_shared<FloatJSON>("floatingpoint", "DOUBLE");
            break;
        case arrow::Type::STRING:
            type = std::make_shared<NameJSON>("utf8");
            break;
        case arrow::Type::BINARY:
            type = std::make_shared<NameJSON>("binary");
            break;
        case arrow::Type::FIXED_SIZE_BINARY: 
        {
            auto byte_width = static_cast<const arrow::FixedSizeBinaryType*>(fieldType.get())->byte_width();
            type = std::make_shared<ByteWidthJSON>("fixedsizebinary", byte_width);
            break;
         }
        case arrow::Type::DATE32:
            type = std::make_shared<UnitZoneJSON>("date", "DAY");
            break;
        case arrow::Type::DATE64:
            type = std::make_shared<UnitZoneJSON>("date", "MILLISECOND");
            break;
        case arrow::Type::TIMESTAMP:
        {
            auto unit = static_cast<const arrow::TimestampType*>(fieldType.get())->unit();
            auto timezone = static_cast<const arrow::TimestampType*>(fieldType.get())->timezone();
            
            switch (unit) {
                case arrow::TimeUnit::SECOND:
                    type = std::make_shared<UnitZoneJSON>("timestamp", "SECOND", timezone);
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<UnitZoneJSON>("timestamp", "MILLISECOND", timezone);
                    break;
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<UnitZoneJSON>("timestamp", "MICROSECOND", timezone);
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<UnitZoneJSON>("timestamp", "NANOSECOND", timezone);
                    break;
                default:
                    break;
            }
            break;
        }
        case arrow::Type::TIME32:
        {
            auto unit = static_cast<const arrow::Time32Type*>(fieldType.get())->unit();
            auto bitWidth = static_cast<const arrow::Time32Type*>(fieldType.get())->bit_width();
            switch (unit) {
                case arrow::TimeUnit::SECOND:
                    type = std::make_shared<BitWidthJSON>("time", false, bitWidth, "SECOND");
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<BitWidthJSON>("time", false, bitWidth, "MILLISECOND");
                    break;
                default:
                    break;
            }
            break;
        }
        case arrow::Type::TIME64:
        {
            auto unit = static_cast<const arrow::Time32Type*>(fieldType.get())->unit();
            auto bitWidth = static_cast<const arrow::Time32Type*>(fieldType.get())->bit_width();
            switch (unit) {
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<BitWidthJSON>("time", false, bitWidth, "MICROSECOND");
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<BitWidthJSON>("time", false, bitWidth, "NANOSECOND");
                    break;
                default:
                    break;
            }
            break;
        }
        case arrow::Type::INTERVAL_MONTHS:
            type = std::make_shared<UnitZoneJSON>("interval", "YEAR_MONTH");
            break;
        case arrow::Type::INTERVAL_DAY_TIME:
            type = std::make_shared<UnitZoneJSON>("interval", "DAY_TIME");
            break;
        case arrow::Type::DECIMAL128: // fallthrough
        case arrow::Type::DECIMAL256:
        {
            auto scale = static_cast<const arrow::Decimal128Type*>(fieldType.get())->scale();
            auto precision = static_cast<const arrow::Decimal128Type*>(fieldType.get())->precision();
            type = std::make_shared<DecimalJSON>("decimal", scale, precision);
            break;
        }
        case arrow::Type::LIST:
            type = std::make_shared<NameJSON>("list");
            break;
        case arrow::Type::STRUCT:
            type = std::make_shared<NameJSON>("struct");
            break;
        //case arrow::Type::SPARSE_UNION:
        //case arrow::Type::DENSE_UNION:
        //case arrow::Type::DICTIONARY:
        case arrow::Type::MAP:
        {
            auto keySorted = static_cast<arrow::MapType*>(fieldType.get())->keys_sorted();
            type = std::make_shared<MapJSON>("map", keySorted);
            break;
        }
        //case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        {
            auto listSize = static_cast<arrow::FixedSizeListType*>(fieldType.get())->list_size();
            type = std::make_shared<ListSizeJSON>("fixedsizelist", listSize);
            break;
        }
        case arrow::Type::DURATION:
        {
            auto unit = static_cast<const arrow::DurationType*>(fieldType.get())->unit();
            switch(unit) {
                case arrow::TimeUnit::SECOND:
                    type = std::make_shared<UnitZoneJSON>("duration", "SECOND");
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<UnitZoneJSON>("duration", "MILLISECOND");
                    break;
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<UnitZoneJSON>("duration", "MICROSECOND");
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<UnitZoneJSON>("duration", "NANOSECOND");
                    break;
                default:
                    break;
            }
            break;
        }
        //case arrow::Type::LARGE_STRING:
        //case arrow::Type::LARGE_BINARY:
        //case arrow::Type::LARGE_LIST:
        case arrow::Type::INTERVAL_MONTH_DAY_NANO:
            type = std::make_shared<UnitZoneJSON>("interval", "MONTH_DAY_NANO");
            break;
        default:
            break;
    }

    result["name"] = field->name();
    result["type"] = type->MarshalJSON();
    return result;
}

const json SchemaJSONConversion::ToJson(
    const std::shared_ptr<arrow::Schema>& schema) const
{
    json result;

    if (schema->HasMetadata()) {
        auto metadata = schema->metadata();
        for (int i = 0; i < metadata->size(); i++) {
            result["schema"]["metadata"].push_back({
                { "key", metadata->key(i) },
                { "value", metadata->value(i) },
            });
        }
    }

    for (int i = 0; i < schema->num_fields(); i++) {
        auto j_field = marshalJSON(schema->field(i));
        result["schema"]["fields"].push_back(j_field);
    }

    return result;
}

const std::shared_ptr<arrow::Schema> SchemaJSONConversion::ToSchema(
    const json& js) const
{
    return nullptr;
}
