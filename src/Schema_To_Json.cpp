#include "Schema_JSON_Conversion.h"
#include <arrow/extension_type.h>
#include "DataTypes.h"
#include <arrow/util/key_value_metadata.h>

using json = nlohmann::json;

/**
 * @brief Helper function converts an arrow::Field into json
 * @param[in] schema Input field object
 * @return arrow::Result contains the converted json if successful, descriptive
 * status otherwise
 */
static arrow::Result<json> marshalJSON(
    const std::shared_ptr<arrow::Field>& field);

arrow::Result<json> converter::SchemaToJSON(
    const std::shared_ptr<arrow::Schema>& schema) {
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
        if (!j_field.ok()) {
            return j_field.status();
        }
        result["schema"]["fields"].push_back(std::move(j_field).ValueOrDie());
    }

    return result;
}

static arrow::Result<json> marshalJSON(
    const std::shared_ptr<arrow::Field>& field) {
    json result{};
    std::shared_ptr<IDataType> type;
    auto fieldType = field->type();

    result["nullable"] = field->nullable();

    // Handle field's metadata
    if (field->HasMetadata()) {
        auto metadata = field->metadata();
        for (int i = 0; i < metadata->size(); i++) {
            result["metadata"].push_back({
                { "key", metadata->key(i) },
                { "value", metadata->value(i) },
            });
        }
    }

    // Handle user-defined type
    if (fieldType->id() == arrow::Type::EXTENSION) {
        auto extType =
            static_cast<const arrow::ExtensionType*>(fieldType.get());
        result["metadata"].push_back(
            { { "key", EXTENSION_TYPE_KEY_NAME },
              { "value", extType->extension_name() } });

        auto serializedData = extType->Serialize();
        if (serializedData.size() > 0) {
            result["metadata"].push_back(
                { { "key", EXTENSION_METADATA_KEY_NAME },
                  { "value", serializedData } });
        }
        fieldType = extType->storage_type();
    }

    switch (fieldType->id()) {
        case arrow::Type::NA:
            type = std::make_shared<NameJSON>(datatype::kNullType);
            break;
        case arrow::Type::BOOL:
            type = std::make_shared<NameJSON>(datatype::kBoolType);
            break;
        case arrow::Type::UINT8:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, false, 8);
            break;
        case arrow::Type::INT8:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, true, 8);
            break;
        case arrow::Type::UINT16:
            type =
                std::make_shared<BitWidthJSON>(datatype::kIntType, false, 16);
            break;
        case arrow::Type::INT16:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, true, 16);
            break;
        case arrow::Type::UINT32:
            type =
                std::make_shared<BitWidthJSON>(datatype::kIntType, false, 32);
            break;
        case arrow::Type::INT32:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, true, 32);
            break;
        case arrow::Type::UINT64:
            type =
                std::make_shared<BitWidthJSON>(datatype::kIntType, false, 64);
            break;
        case arrow::Type::INT64:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, true, 64);
            break;
        case arrow::Type::HALF_FLOAT:
            type = std::make_shared<FloatJSON>(datatype::kFloatingPointType,
                                               datatype::kPrecisionHalf);
            break;
        case arrow::Type::FLOAT:
            type = std::make_shared<FloatJSON>(datatype::kFloatingPointType,
                                               datatype::kPrecisionSingle);
            break;
        case arrow::Type::DOUBLE:
            type = std::make_shared<FloatJSON>(datatype::kFloatingPointType,
                                               datatype::kPrecisionDouble);
            break;
        case arrow::Type::STRING:
            type = std::make_shared<NameJSON>(datatype::kUtf8Type);
            break;
        case arrow::Type::BINARY:
            type = std::make_shared<NameJSON>(datatype::kBinaryType);
            break;
        case arrow::Type::FIXED_SIZE_BINARY: {
            auto byte_width =
                static_cast<const arrow::FixedSizeBinaryType*>(fieldType.get())
                    ->byte_width();
            type = std::make_shared<ByteWidthJSON>(
                datatype::kFixedSizeBinaryType, byte_width);
            break;
        }
        case arrow::Type::DATE32:
            type = std::make_shared<UnitZoneJSON>(datatype::kDateType,
                                                  datatype::kDayUnit);
            break;
        case arrow::Type::DATE64:
            type = std::make_shared<UnitZoneJSON>(datatype::kDateType,
                                                  datatype::kMillisecondUnit);
            break;
        case arrow::Type::TIMESTAMP: {
            auto unit =
                static_cast<const arrow::TimestampType*>(fieldType.get())
                    ->unit();
            auto timezone =
                static_cast<const arrow::TimestampType*>(fieldType.get())
                    ->timezone();

            switch (unit) {
                case arrow::TimeUnit::SECOND:
                    type =
                        std::make_shared<UnitZoneJSON>(datatype::kTimestampType,
                                                       datatype::kSecondUnit,
                                                       timezone);
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<UnitZoneJSON>(
                        datatype::kTimestampType,
                        datatype::kMillisecondUnit,
                        timezone);
                    break;
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<UnitZoneJSON>(
                        datatype::kTimestampType,
                        datatype::kMicrosecondUnit,
                        timezone);
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<UnitZoneJSON>(
                        datatype::kTimestampType,
                        datatype::kNanosecondUnit,
                        timezone);
                    break;
                default:
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case arrow::Type::TIME32: {
            auto unit =
                static_cast<const arrow::Time32Type*>(fieldType.get())->unit();
            auto bitWidth =
                static_cast<const arrow::Time32Type*>(fieldType.get())
                    ->bit_width();
            switch (unit) {
                case arrow::TimeUnit::SECOND:
                    type =
                        std::make_shared<BitWidthJSON>(datatype::kTimeType,
                                                       false,
                                                       bitWidth,
                                                       datatype::kSecondUnit);
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<BitWidthJSON>(
                        datatype::kTimeType,
                        false,
                        bitWidth,
                        datatype::kMillisecondUnit);
                    break;
                default:
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case arrow::Type::TIME64: {
            auto unit =
                static_cast<const arrow::Time32Type*>(fieldType.get())->unit();
            auto bitWidth =
                static_cast<const arrow::Time32Type*>(fieldType.get())
                    ->bit_width();
            switch (unit) {
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<BitWidthJSON>(
                        datatype::kTimeType,
                        false,
                        bitWidth,
                        datatype::kMicrosecondUnit);
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<BitWidthJSON>(
                        datatype::kTimeType,
                        false,
                        bitWidth,
                        datatype::kNanosecondUnit);
                    break;
                default:
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case arrow::Type::INTERVAL_MONTHS:
            type = std::make_shared<UnitZoneJSON>(
                datatype::kIntervalType, datatype::kYearMonthIntervalUnit);
            break;
        case arrow::Type::INTERVAL_DAY_TIME:
            type = std::make_shared<UnitZoneJSON>(
                datatype::kIntervalType, datatype::kDayTimeIntervalUnit);
            break;
        case arrow::Type::DECIMAL128: // fallthrough
        case arrow::Type::DECIMAL256: {
            auto scale =
                static_cast<const arrow::Decimal128Type*>(fieldType.get())
                    ->scale();
            auto precision =
                static_cast<const arrow::Decimal128Type*>(fieldType.get())
                    ->precision();
            type = std::make_shared<DecimalJSON>(
                datatype::kDecimalType, scale, precision);
            break;
        }
        case arrow::Type::LIST: {
            type = std::make_shared<NameJSON>(datatype::kListType);
            auto listType =
                static_cast<const arrow::ListType*>(fieldType.get());
            for (int i = 0; i < listType->num_fields(); i++) {
                auto field = marshalJSON(listType->field(i));
                if (!field.ok()) {
                    return field.status();
                }
                result["children"].push_back(std::move(field).ValueOrDie());
            }
            break;
        }
        case arrow::Type::STRUCT: {
            type = std::make_shared<NameJSON>(datatype::kStructType);
            auto structType =
                static_cast<const arrow::StructType*>(fieldType.get());
            for (int i = 0; i < structType->num_fields(); i++) {
                auto field = marshalJSON(structType->field(i));
                if (!field.ok()) {
                    return field.status();
                }
                result["children"].push_back(std::move(field).ValueOrDie());
            }
            break;
        }
        case arrow::Type::MAP: {
            auto mapType = static_cast<arrow::MapType*>(fieldType.get());
            auto keySorted = mapType->keys_sorted();
            type = std::make_shared<MapJSON>(datatype::kMapType, keySorted);
            auto keyJson = marshalJSON(mapType->key_field());
            if (!keyJson.ok()) {
                return arrow::Status::TypeError("failed to parse key");
            }
            auto itemJson = marshalJSON(mapType->item_field());
            if (!itemJson.ok()) {
                return arrow::Status::TypeError("failed to parse value");
            }
            result["children"].push_back({
                { "key", std::move(keyJson).ValueOrDie() },
                { "item", std::move(itemJson).ValueOrDie() },
            });
            break;
        }
        case arrow::Type::DURATION: {
            auto unit = static_cast<const arrow::DurationType*>(fieldType.get())
                            ->unit();
            switch (unit) {
                case arrow::TimeUnit::SECOND:
                    type = std::make_shared<UnitZoneJSON>(
                        datatype::kDurationType, datatype::kSecondUnit);
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<UnitZoneJSON>(
                        datatype::kDurationType, datatype::kMillisecondUnit);
                    break;
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<UnitZoneJSON>(
                        datatype::kDurationType, datatype::kMicrosecondUnit);
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<UnitZoneJSON>(
                        datatype::kDurationType, datatype::kNanosecondUnit);
                    break;
                default:
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case arrow::Type::INTERVAL_MONTH_DAY_NANO:
            type = std::make_shared<UnitZoneJSON>(
                datatype::kIntervalType, datatype::kMonthDayNanoIntervalUnit);
            break;
        default:
            return arrow::Status::Invalid("unsupported type");
    }

    result["name"] = field->name();
    result["type"] = type->MarshalJSON();
    return result;
}
