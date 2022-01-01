#ifndef _SCHEMA_JSON_CONVERSION_H_
#define _SCHEMA_JSON_CONVERSION_H_

#include <nlohmann/json.hpp>
#include <arrow/type.h>

namespace converter {
    arrow::Result<nlohmann::json> SchemaToJSON(const std::shared_ptr<arrow::Schema>& schema);
    arrow::Result<std::shared_ptr<arrow::Schema>> JSONToSchema(const nlohmann::json& jsonObj);
}

#endif // _SCHEMA_JSON_CONVERSION_H_
