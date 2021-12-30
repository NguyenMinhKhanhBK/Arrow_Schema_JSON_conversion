#include "Schema_JSON_Conversion.h"
#include <iostream>
#include "DataTypes.h"
#include <arrow/util/key_value_metadata.h>
#include <arrow/extension_type.h>


const json marshalJSON(const std::shared_ptr<arrow::Field> field) {
    json result;
    std::shared_ptr<IDataType> type;
    auto fieldType = field->type();

    result["nullable"] = field->nullable();

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

    switch(fieldType->id()) {
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
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, false, 16);
            break;
        case arrow::Type::INT16:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, true, 16);
            break;
        case arrow::Type::UINT32:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, false, 32);
            break;
        case arrow::Type::INT32:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, true, 32);
            break;
        case arrow::Type::UINT64:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, false, 64);
            break;
        case arrow::Type::INT64:
            type = std::make_shared<BitWidthJSON>(datatype::kIntType, true, 64);
            break;
        case arrow::Type::HALF_FLOAT:
            type = std::make_shared<FloatJSON>(datatype::kFloatingPointType, datatype::kPrecisionHalf);
            break;
        case arrow::Type::FLOAT:
            type = std::make_shared<FloatJSON>(datatype::kFloatingPointType, datatype::kPrecisionSingle);
            break;
        case arrow::Type::DOUBLE:
            type = std::make_shared<FloatJSON>(datatype::kFloatingPointType, datatype::kPrecisionDouble);
            break;
        case arrow::Type::STRING:
            type = std::make_shared<NameJSON>(datatype::kUtf8Type);
            break;
        case arrow::Type::BINARY:
            type = std::make_shared<NameJSON>(datatype::kBinaryType);
            break;
        case arrow::Type::FIXED_SIZE_BINARY: 
        {
            auto byte_width = static_cast<const arrow::FixedSizeBinaryType*>(fieldType.get())->byte_width();
            type = std::make_shared<ByteWidthJSON>(datatype::kFixedSizeBinaryType, byte_width);
            break;
         }
        case arrow::Type::DATE32:
            type = std::make_shared<UnitZoneJSON>(datatype::kDateType, datatype::kDayUnit);
            break;
        case arrow::Type::DATE64:
            type = std::make_shared<UnitZoneJSON>(datatype::kDateType, datatype::kMillisecondUnit);
            break;
        case arrow::Type::TIMESTAMP:
        {
            auto unit = static_cast<const arrow::TimestampType*>(fieldType.get())->unit();
            auto timezone = static_cast<const arrow::TimestampType*>(fieldType.get())->timezone();
            
            switch (unit) {
                case arrow::TimeUnit::SECOND:
                    type = std::make_shared<UnitZoneJSON>(datatype::kTimestampType, datatype::kSecondUnit, timezone);
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<UnitZoneJSON>(datatype::kTimestampType, datatype::kMillisecondUnit, timezone);
                    break;
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<UnitZoneJSON>(datatype::kTimestampType, datatype::kMicrosecondUnit, timezone);
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<UnitZoneJSON>(datatype::kTimestampType, datatype::kNanosecondUnit, timezone);
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
                    type = std::make_shared<BitWidthJSON>(datatype::kTimeType, false, bitWidth, datatype::kSecondUnit);
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<BitWidthJSON>(datatype::kTimeType, false, bitWidth, datatype::kMillisecondUnit);
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
                    type = std::make_shared<BitWidthJSON>(datatype::kTimeType, false, bitWidth, datatype::kMicrosecondUnit);
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<BitWidthJSON>(datatype::kTimeType, false, bitWidth, datatype::kNanosecondUnit);
                    break;
                default:
                    break;
            }
            break;
        }
        case arrow::Type::INTERVAL_MONTHS:
            type = std::make_shared<UnitZoneJSON>(datatype::kIntervalType, datatype::kYearMonthIntervalUnit);
            break;
        case arrow::Type::INTERVAL_DAY_TIME:
            type = std::make_shared<UnitZoneJSON>(datatype::kIntervalType, datatype::kDayTimeIntervalUnit);
            break;
        case arrow::Type::DECIMAL128: // fallthrough
        case arrow::Type::DECIMAL256:
        {
            auto scale = static_cast<const arrow::Decimal128Type*>(fieldType.get())->scale();
            auto precision = static_cast<const arrow::Decimal128Type*>(fieldType.get())->precision();
            type = std::make_shared<DecimalJSON>(datatype::kDecimalType, scale, precision);
            break;
        }
        case arrow::Type::LIST:
        {
            type = std::make_shared<NameJSON>(datatype::kListType);
            auto listType = static_cast<const arrow::ListType*>(fieldType.get());
            for (int i = 0; i < listType->num_fields(); i++) {
                result["children"].push_back(marshalJSON(listType->field(i)));
            }
            break;

        }
        case arrow::Type::STRUCT:
        {
            type = std::make_shared<NameJSON>(datatype::kStructType);
            auto structType = static_cast<const arrow::StructType*>(fieldType.get());
            for (int i = 0; i < structType->num_fields(); i++) {
                result["children"].push_back(marshalJSON(structType->field(i)));
            }
            break;

        }
        //case arrow::Type::SPARSE_UNION:
        //case arrow::Type::DENSE_UNION:
        //case arrow::Type::DICTIONARY:
        case arrow::Type::MAP:
        {
            auto keySorted = static_cast<arrow::MapType*>(fieldType.get())->keys_sorted();
            type = std::make_shared<MapJSON>(datatype::kMapType, keySorted);
            break;
        }
        //case arrow::Type::EXTENSION:
        case arrow::Type::FIXED_SIZE_LIST:
        {
            auto listSize = static_cast<arrow::FixedSizeListType*>(fieldType.get())->list_size();
            type = std::make_shared<ListSizeJSON>(datatype::kFixedSizeListType, listSize);
            break;
        }
        case arrow::Type::DURATION:
        {
            auto unit = static_cast<const arrow::DurationType*>(fieldType.get())->unit();
            switch(unit) {
                case arrow::TimeUnit::SECOND:
                    type = std::make_shared<UnitZoneJSON>(datatype::kDurationType, datatype::kSecondUnit);
                    break;
                case arrow::TimeUnit::MILLI:
                    type = std::make_shared<UnitZoneJSON>(datatype::kDurationType, datatype::kMillisecondUnit);
                    break;
                case arrow::TimeUnit::MICRO:
                    type = std::make_shared<UnitZoneJSON>(datatype::kDurationType, datatype::kMicrosecondUnit);
                    break;
                case arrow::TimeUnit::NANO:
                    type = std::make_shared<UnitZoneJSON>(datatype::kDurationType, datatype::kNanosecondUnit);
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
            type = std::make_shared<UnitZoneJSON>(datatype::kIntervalType, datatype::kMonthDayNanoIntervalUnit);
            break;
        default:
            break;
    }

    result["name"] = field->name();
    result["type"] = type->MarshalJSON();
    return result;
}

const std::shared_ptr<arrow::Field> unmarshalJSON(const json& jsonField) {
    std::shared_ptr<arrow::Field> resultField{};
    std::shared_ptr<arrow::DataType> resultType{};
    auto fieldName = jsonField.at("name").get<std::string>();
    auto typeNameStr = jsonField.at("type").at("name").get<std::string>();
    auto typeNameEnum = datatype::GetTypeFromString(typeNameStr);

    switch(typeNameEnum) {
        case datatype::TYPE_NAME_NULL:
            resultType = arrow::null();
            break;
        case datatype::TYPE_NAME_BOOL:
            resultType = arrow::boolean();
            break;
        case datatype::TYPE_NAME_INT:
        {
            auto isSigned = jsonField.at("type").at("isSigned").get<bool>();
            auto bitWidth = jsonField.at("type").at("bitWidth").get<int>();

            if (isSigned) {
                switch(bitWidth) {
                    case 8:
                        resultType = arrow::int8();
                        break;
                    case 16:
                        resultType = arrow::int16();
                        break;
                    case 32:
                        resultType = arrow::int32();
                        break;
                    case 64:
                        resultType = arrow::int64();
                        break;
                }
            } else {
                switch(bitWidth) {
                    case 8:
                        resultType = arrow::uint8();
                        break;
                    case 16:
                        resultType = arrow::uint16();
                        break;
                    case 32:
                        resultType = arrow::uint32();
                        break;
                    case 64:
                        resultType = arrow::uint64();
                        break;
                }
            }

            break;
        }
        case datatype::TYPE_NAME_FLOATING_POINT:
        {
            auto precisionStr = jsonField.at("type").at("precision").get<std::string>();
            auto precisionEnum = datatype::GetPrecisionFromString(precisionStr);
            switch (precisionEnum) {
                case datatype::PRECISION_HALF:
                        resultType = arrow::float16();
                    break;
                case datatype::PRECISION_SINGLE:
                        resultType = arrow::float32();
                    break;
                case datatype::PRECISION_DOUBLE:
                        resultType = arrow::float64();
                    break;
                default:
                    std::cout << "unknown precision\n";
                    break;
            }
            break;
        }
        case datatype::TYPE_NAME_BINARY:
            resultType = arrow::binary();
            break;
        case datatype::TYPE_NAME_UTF8:
            resultType = arrow::utf8();
            break;
        case datatype::TYPE_NAME_DATE:
        {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);
            switch(unitEnum) {
                case datatype::DATE_TIME_UNIT_DAY:
                    resultType = arrow::date32();
                    break;
                case datatype::DATE_TIME_UNIT_MILLISECOND:
                    resultType = arrow::date64();
                    break;
                default:
                    std::cout << "unsupported unit\n";
                    break;
            }
            break;
        }
        case datatype::TYPE_NAME_TIME:
        {
            auto bitWidth = jsonField.at("type").at("bitWidth").get<int>();
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);

            switch(bitWidth) {
                case 32:
                    switch(unitEnum) {
                        case datatype::DATE_TIME_UNIT_SECOND:
                            resultType = arrow::time32(arrow::TimeUnit::SECOND);
                            break;
                        case datatype::DATE_TIME_UNIT_MILLISECOND:
                            resultType = arrow::time32(arrow::TimeUnit::MILLI);
                            break;
                        default:
                            break;
                    }
                    break;
                case 64:
                    switch(unitEnum) {
                        case datatype::DATE_TIME_UNIT_MICROSECOND:
                            resultType = arrow::time64(arrow::TimeUnit::MICRO);
                            break;
                        case datatype::DATE_TIME_UNIT_NANOSECOND:
                            resultType = arrow::time64(arrow::TimeUnit::NANO);
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
            break;
        }
        case datatype::TYPE_NAME_TIMESTAMP:
        {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);
            auto timezone = jsonField.at("type").at("timezone").get<std::string>();
            switch(unitEnum) {
                case datatype::DATE_TIME_UNIT_SECOND:
                    resultType = arrow::timestamp(arrow::TimeUnit::SECOND, timezone);
                    break;
                case datatype::DATE_TIME_UNIT_MILLISECOND:
                    resultType = arrow::timestamp(arrow::TimeUnit::MILLI, timezone);
                    break;
                case datatype::DATE_TIME_UNIT_MICROSECOND:
                    resultType = arrow::timestamp(arrow::TimeUnit::MICRO, timezone);
                    break;
                case datatype::DATE_TIME_UNIT_NANOSECOND: 
                    resultType = arrow::timestamp(arrow::TimeUnit::NANO, timezone);
                    break;
                default:
                    break;
            }
            break;
        }
        // TODO Handle list
        case datatype::TYPE_NAME_LIST:
            break;
        // TODO Handle map
        case datatype::TYPE_NAME_MAP:
            break;
        // TODO Handle struct
        case datatype::TYPE_NAME_STRUCT:
            break;
        case datatype::TYPE_NAME_FIXED_SIZE_BINARY:
        {
            auto byteWidth = jsonField.at("type").at("byteWidth").get<int>();
            resultType = arrow::fixed_size_binary(byteWidth);
            break;
        }
        // TODO Handle fixed size list
        case datatype::TYPE_NAME_FIXED_SIZE_LIST:
            break;
        case datatype::TYPE_NAME_INTERVAL:
        {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetIntervalUnitFromString(unitStr);
            switch(unitEnum) {
                case datatype::INTERVAL_UNIT_YEAR_MONTH:
                    resultType = arrow::month_interval();
                    break;
                case datatype::INTERVAL_UNIT_DAY_TIME:
                    resultType = arrow::day_time_interval();
                    break;
                case datatype::INTERVAL_UNIT_MONTH_DAY_NANO:
                    resultType = arrow::month_day_nano_interval();
                    break;
                default:
                    break;
            }
            break;
        }
        case datatype::TYPE_NAME_DURATION:
        {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);
            switch(unitEnum) {
                case datatype::DATE_TIME_UNIT_SECOND:
                    resultType = arrow::duration(arrow::TimeUnit::SECOND);
                    break;
                case datatype::DATE_TIME_UNIT_MILLISECOND:
                    resultType = arrow::duration(arrow::TimeUnit::MILLI);
                    break;
                case datatype::DATE_TIME_UNIT_MICROSECOND:
                    resultType = arrow::duration(arrow::TimeUnit::MICRO);
                    break;
                case datatype::DATE_TIME_UNIT_NANOSECOND:
                    resultType = arrow::duration(arrow::TimeUnit::NANO);
                    break;
                default:
                    break;
            }
            break;
        }
        case datatype::TYPE_NAME_DECIMAL:
        {
            auto precision = jsonField.at("type").at("precision").get<int>();
            auto scale = jsonField.at("type").at("scale").get<int>();
            resultType = arrow::decimal(precision, scale);
            break;
        }
        default:
            std::cout << "unknown type: " << fieldName << std::endl;
            break;
    }

    // Handle metadata
    if (!jsonField.contains("metadata")) {
        std::cout << "no metadata in " << fieldName << std::endl;
        return arrow::field(fieldName, resultType);
    }

    auto jsonMeta = jsonField.at("metadata");
    std::cout << fieldName << " has " << jsonMeta.size() << " metadata\n";
    std::vector<std::string> keys{};
    std::vector<std::string> values{};
    int extKeyIdx = -1;
    int extDataIdx = -1;

    for (int i = 0; i < jsonMeta.size(); i++) {
        auto key = jsonMeta[i].at("key");

        if (key == EXTENSION_TYPE_KEY_NAME) {
            extKeyIdx = i;
        } else if (key == EXTENSION_METADATA_KEY_NAME) {
            extDataIdx = i;
        }

        keys.push_back(key);
        values.push_back(jsonMeta[i].at("value"));
    }

    if (extKeyIdx == -1) {
        return arrow::field(fieldName, resultType)->WithMetadata(arrow::KeyValueMetadata::Make(keys, values));
    }

    auto extType = arrow::GetExtensionType(values[extKeyIdx]);
    if (extType == nullptr) { // unregistered extension type, just keep the metadata
        return arrow::field(fieldName, resultType)->WithMetadata(arrow::KeyValueMetadata::Make(keys, values));
    }

    std::string extData{};
    if (extDataIdx != -1) {
        extData = values[extDataIdx];
        keys.erase(keys.begin() + extKeyIdx);
        values.erase(values.begin() + extKeyIdx);
        keys.erase(keys.begin() + extDataIdx);
        values.erase(values.begin() + extDataIdx);
    } else {
        keys.erase(keys.begin() + extKeyIdx);
        values.erase(values.begin() + extKeyIdx);
    }

    auto deserializeResult = extType->Deserialize(resultType, extData);
    if (deserializeResult.ok()) {
        resultType = deserializeResult.ValueUnsafe();
    }

    return arrow::field(fieldName, resultType);
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
    auto schemaJson = js.at("schema");
    std::vector<std::shared_ptr<arrow::Field>> fields{};

    for (auto fieldJson : schemaJson.at("fields")) {
        auto field = unmarshalJSON(fieldJson);
        fields.push_back(field);
    }

    if (!schemaJson.contains("metadata")) {
        return arrow::schema(fields);
    }

    std::vector<std::string> keys{};
    std::vector<std::string> values{};
    auto metadata = schemaJson.at("metadata");

    for (auto item : metadata) {
        keys.push_back(item.at("key"));
        values.push_back(item.at("value"));
    }
    
    return arrow::schema(fields)->WithMetadata(arrow::KeyValueMetadata::Make(keys, values));
}
