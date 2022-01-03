#include "Schema_JSON_Conversion.h"

#include <arrow/extension_type.h>
#include <arrow/util/key_value_metadata.h>

#include "DataTypes.h"

static arrow::Result<std::shared_ptr<arrow::Field>> unmarshalJSON(
    const json& jsonObj);

arrow::Result<std::shared_ptr<arrow::Schema>> converter::JSONToSchema(
    const json& jsonObj) {
    auto schemaJson = jsonObj.at("schema");
    std::vector<std::shared_ptr<arrow::Field>> fields{};

    for (auto fieldJson : schemaJson.at("fields")) {
        auto field = unmarshalJSON(fieldJson);
        if (!field.ok()) {
            return field.status();
        }
        fields.push_back(std::move(field).ValueOrDie());
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

    return arrow::schema(fields)->WithMetadata(
        arrow::KeyValueMetadata::Make(keys, values));
}

static arrow::Result<std::shared_ptr<arrow::Field>> unmarshalJSON(
    const json& jsonField) {
    std::shared_ptr<arrow::Field> resultField{};
    std::shared_ptr<arrow::DataType> resultType{};

    auto fieldName = jsonField.at("name").get<std::string>();
    auto typeNameStr = jsonField.at("type").at("name").get<std::string>();
    auto typeNameEnum = datatype::GetTypeFromString(typeNameStr);

    switch (typeNameEnum) {
        case datatype::TYPE_NAME_NULL:
            resultType = arrow::null();
            break;
        case datatype::TYPE_NAME_BOOL:
            resultType = arrow::boolean();
            break;
        case datatype::TYPE_NAME_INT: {
            auto isSigned = jsonField.at("type").at("isSigned").get<bool>();
            auto bitWidth = jsonField.at("type").at("bitWidth").get<int>();

            if (isSigned) {
                switch (bitWidth) {
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
                    default:
                        return arrow::Status::Invalid("unsupported bit width");
                }
            } else {
                switch (bitWidth) {
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
                    default:
                        return arrow::Status::Invalid("unsupported bit width");
                }
            }

            break;
        }
        case datatype::TYPE_NAME_FLOATING_POINT: {
            auto precisionStr =
                jsonField.at("type").at("precision").get<std::string>();
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
                    return arrow::Status::Invalid("unsupported precision");
            }
            break;
        }
        case datatype::TYPE_NAME_BINARY:
            resultType = arrow::binary();
            break;
        case datatype::TYPE_NAME_UTF8:
            resultType = arrow::utf8();
            break;
        case datatype::TYPE_NAME_DATE: {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);
            switch (unitEnum) {
                case datatype::DATE_TIME_UNIT_DAY:
                    resultType = arrow::date32();
                    break;
                case datatype::DATE_TIME_UNIT_MILLISECOND:
                    resultType = arrow::date64();
                    break;
                default:
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case datatype::TYPE_NAME_TIME: {
            auto bitWidth = jsonField.at("type").at("bitWidth").get<int>();
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);

            switch (bitWidth) {
                case 32:
                    switch (unitEnum) {
                        case datatype::DATE_TIME_UNIT_SECOND:
                            resultType = arrow::time32(arrow::TimeUnit::SECOND);
                            break;
                        case datatype::DATE_TIME_UNIT_MILLISECOND:
                            resultType = arrow::time32(arrow::TimeUnit::MILLI);
                            break;
                        default:
                            return arrow::Status::Invalid("unsupported unit");
                    }
                    break;
                case 64:
                    switch (unitEnum) {
                        case datatype::DATE_TIME_UNIT_MICROSECOND:
                            resultType = arrow::time64(arrow::TimeUnit::MICRO);
                            break;
                        case datatype::DATE_TIME_UNIT_NANOSECOND:
                            resultType = arrow::time64(arrow::TimeUnit::NANO);
                            break;
                        default:
                            return arrow::Status::Invalid("unsupported unit");
                    }
                    break;
                default:
                    return arrow::Status::Invalid("unsupported bit width");
            }
            break;
        }
        case datatype::TYPE_NAME_TIMESTAMP: {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);
            auto timezone =
                jsonField.at("type").at("timezone").get<std::string>();
            switch (unitEnum) {
                case datatype::DATE_TIME_UNIT_SECOND:
                    resultType =
                        arrow::timestamp(arrow::TimeUnit::SECOND, timezone);
                    break;
                case datatype::DATE_TIME_UNIT_MILLISECOND:
                    resultType =
                        arrow::timestamp(arrow::TimeUnit::MILLI, timezone);
                    break;
                case datatype::DATE_TIME_UNIT_MICROSECOND:
                    resultType =
                        arrow::timestamp(arrow::TimeUnit::MICRO, timezone);
                    break;
                case datatype::DATE_TIME_UNIT_NANOSECOND:
                    resultType =
                        arrow::timestamp(arrow::TimeUnit::NANO, timezone);
                    break;
                default:
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case datatype::TYPE_NAME_LIST: {
            if (!jsonField.contains("children")) {
                return arrow::Status::Invalid("no children found");
            }
            auto field = unmarshalJSON(jsonField.at("children")[0]);
            if (!field.ok()) {
                return field.status();
            }
            resultType = arrow::list(std::move(field).ValueOrDie());
            break;
        }
        case datatype::TYPE_NAME_MAP: {
            if (!jsonField.contains("children")) {
                return arrow::Status::Invalid("no children found");
            }
            auto keySorted = jsonField.at("type").at("keySorted").get<bool>();
            auto keyField =
                unmarshalJSON(jsonField.at("children")[0].at("key"));
            if (!keyField.ok()) {
                return keyField.status();
            }
            auto itemField =
                unmarshalJSON(jsonField.at("children")[0].at("item"));
            if (!itemField.ok()) {
                return itemField.status();
            }
            resultType = arrow::map(std::move(keyField).ValueOrDie()->type(),
                                    std::move(itemField).ValueOrDie(),
                                    keySorted);
            break;
        }
        case datatype::TYPE_NAME_STRUCT: {
            if (!jsonField.contains("children")) {
                return arrow::Status::Invalid("no children found");
            }
            std::vector<std::shared_ptr<arrow::Field>> fields{};
            for (auto childJson : jsonField.at("children")) {
                auto childField = unmarshalJSON(childJson);
                if (!childField.ok()) {
                    return childField.status();
                }
                fields.push_back(std::move(childField).ValueOrDie());
            }
            resultType = arrow::struct_(fields);
            break;
        }
        case datatype::TYPE_NAME_FIXED_SIZE_BINARY: {
            auto byteWidth = jsonField.at("type").at("byteWidth").get<int>();
            resultType = arrow::fixed_size_binary(byteWidth);
            break;
        }
        case datatype::TYPE_NAME_INTERVAL: {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetIntervalUnitFromString(unitStr);
            switch (unitEnum) {
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
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case datatype::TYPE_NAME_DURATION: {
            auto unitStr = jsonField.at("type").at("unit").get<std::string>();
            auto unitEnum = datatype::GetUnitFromString(unitStr);
            switch (unitEnum) {
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
                    return arrow::Status::Invalid("unsupported unit");
            }
            break;
        }
        case datatype::TYPE_NAME_DECIMAL: {
            auto precision = jsonField.at("type").at("precision").get<int>();
            auto scale = jsonField.at("type").at("scale").get<int>();
            resultType = arrow::decimal(precision, scale);
            break;
        }
        default:
            return arrow::Status::Invalid("unsupported type");
    }

    // Handle metadata
    if (!jsonField.contains("metadata")) {
        //std::cout << "no metadata in " << fieldName << std::endl;
        return arrow::field(fieldName, resultType);
    }

    auto jsonMeta = jsonField.at("metadata");
    //std::cout << fieldName << " has " << jsonMeta.size() << " metadata\n";
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
        return arrow::field(fieldName, resultType)
            ->WithMetadata(arrow::KeyValueMetadata::Make(keys, values));
    }

    auto extType = arrow::GetExtensionType(values[extKeyIdx]);
    if (extType ==
        nullptr) { // unregistered extension type, just keep the metadata
        return arrow::field(fieldName, resultType)
            ->WithMetadata(arrow::KeyValueMetadata::Make(keys, values));
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
