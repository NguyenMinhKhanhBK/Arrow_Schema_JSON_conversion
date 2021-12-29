#ifndef _SCHEMA_JSON_CONVERSION_H_
#define _SCHEMA_JSON_CONVERSION_H_

#include <nlohmann/json.hpp>
#include <arrow/type.h>

using json = nlohmann::json;


class SchemaJSONConversion {
public:
    explicit SchemaJSONConversion() = default;
    ~SchemaJSONConversion() = default;

    const json ToJson(const std::shared_ptr<arrow::Schema>& schema) const;
    const std::shared_ptr<arrow::Schema> ToSchema(const json& js) const;
};

#endif // _SCHEMA_JSON_CONVERSION_H_
