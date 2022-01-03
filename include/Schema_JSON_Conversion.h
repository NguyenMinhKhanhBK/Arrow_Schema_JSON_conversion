#ifndef _SCHEMA_JSON_CONVERSION_H_
#define _SCHEMA_JSON_CONVERSION_H_

#include <arrow/type.h>

#include <nlohmann/json.hpp>

namespace converter {

/**
 * @brief Convert arrow::Schema to Json
 * @param[in] schema Input schema
 * @return arrow::Result contains the converted json if successful, descriptive
 * status otherwise
 *
 * @example
 * auto result = SchemaToJSON(schema);
 * if (!result.ok()) {
 *      std::cout << "error\n";
 *      return;
 * }
 * auto convertedJson = result.ValueOrDie();
 */
arrow::Result<nlohmann::json> SchemaToJSON(
    const std::shared_ptr<arrow::Schema>& schema);

/**
 * @brief Convert Json to arrow::Schema
 * @param[in] jsonObj Input json object
 * @return arrow::Result contains the converted arrow::Schema if successful,
 * descriptive status otherwise
 *
 * @example
 * auto result = JSONToSchema(jsonObj);
 * if (!result.ok()) {
 *      std::cout << "error\n";
 *      return;
 * }
 * auto convertedSchema = result.ValueOrDie();
 */
arrow::Result<std::shared_ptr<arrow::Schema>> JSONToSchema(
    const nlohmann::json& jsonObj);

} // namespace converter

#endif // _SCHEMA_JSON_CONVERSION_H_
