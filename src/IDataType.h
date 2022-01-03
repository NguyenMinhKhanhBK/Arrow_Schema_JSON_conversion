#ifndef _I_DATA_TYPE_H_
#define _I_DATA_TYPE_H_

#include <nlohmann/json.hpp>
using json = nlohmann::json;

class IDataType {
public:
    IDataType() = default;
    virtual ~IDataType() = default;

    /**
     * @brief MarshalJSON converts the inherited class (type) into Json format
     * @return nlohmann::json object represents the inherited class
     */
    virtual const json MarshalJSON() = 0;
};

#endif // _I_DATA_TYPE_H_
