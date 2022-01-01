#ifndef _SCHEMA_JSON_CONVERSION_H_
#define _SCHEMA_JSON_CONVERSION_H_

#include <arrow/type.h>

#include <nlohmann/json.hpp>

namespace converter {
arrow::Result<nlohmann::json> SchemaToJSON(
    const std::shared_ptr<arrow::Schema>& schema);

arrow::Result<std::shared_ptr<arrow::Schema>> JSONToSchema(
    const nlohmann::json& jsonObj);
} // namespace converter

#endif // _SCHEMA_JSON_CONVERSION_H_
