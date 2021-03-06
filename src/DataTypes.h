#ifndef _DATA_TYPES_H_
#define _DATA_TYPES_H_

#include <arrow/type.h>

#include "IDataType.h"

/**
 *
 * NameJSON only includes field name and uses for NULL, BOOL, STRING, BINARY,
 * LIST, STRUCT types
 *
 * mName: field name
 */
class NameJSON : public IDataType {
public:
    NameJSON(const std::string& name)
        : mName{ name } {};

    ~NameJSON() = default;

    const json MarshalJSON() override {
        return {
            { "name", mName },
        };
    }

private:
    std::string mName{};
};

/**
 * BitWidthJSON uses for integer (INT8/ UINT8, INT16/ UINT16, INT32/ UINT32,
 * INT64/ UINT64) and time (TIME32, TIME64) types
 *
 * mName: field name
 * mSigned: signed or unsigned integer
 * mBitWidth: bit width, only supports 8, 16, 32 and 64 bits
 */
class BitWidthJSON : public IDataType {
public:
    BitWidthJSON(const std::string& name,
                 bool isSigned,
                 int bitWidth,
                 const std::string& unit = "")
        : mName{ name }
        , mSigned{ isSigned }
        , mBitWidth{ bitWidth }
        , mUnit{ unit } {};

    ~BitWidthJSON() = default;

    const json MarshalJSON() override {
        return {
            { "name", mName },
            { "isSigned", mSigned },
            { "bitWidth", mBitWidth },
            { "unit", mUnit },
        };
    }

private:
    std::string mName{};
    bool mSigned{};
    int mBitWidth{};
    std::string mUnit{};
};

/**
 * FloatJSON uses for floating point type (FLOAT16, FLOAT32, FLOAT64)
 *
 * mName: field name
 * mPrecision: floating point precision
 *      HALF precision for FLOAT16
 *      SINGLE precision for FLOAT32
 *      DOUBLE precision for FLOAT64
 */
class FloatJSON : public IDataType {
public:
    FloatJSON(const std::string& name, const std::string& precision)
        : mName{ name }
        , mPrecision{ precision } {};

    ~FloatJSON() = default;

    const json MarshalJSON() override {
        return {
            { "name", mName },
            { "precision", mPrecision },
        };
    }

private:
    std::string mName{};
    std::string mPrecision{};
};

/**
 * UnitZoneJSON uses for timezone related types (DATE32, DATE64, TIMESTAMP,
 * INTERVAL_MONTHS, INTERVAL_DAY_TIME)
 *
 * mName: field name
 * mUnit: time unit (such as second, millisecond, microsecond, nanosecond)
 * mTimeZone: timezone
 */
class UnitZoneJSON : public IDataType {
public:
    UnitZoneJSON(const std::string& name,
                 const std::string& unit = "",
                 const std::string& timezone = "")
        : mName{ name }
        , mUnit{ unit }
        , mTimeZone{ timezone } {};

    ~UnitZoneJSON() = default;

    const json MarshalJSON() {
        return {
            { "name", mName },
            { "unit", mUnit },
            { "timezone", mTimeZone },
        };
    }

private:
    std::string mName{};
    std::string mUnit{};
    std::string mTimeZone{};
};

/**
 * DecimalJSON uses for decimal type
 *
 * mName: field name
 * mScale: scale number
 * mPrecision: number of precision
 */
class DecimalJSON : public IDataType {
public:
    DecimalJSON(const std::string& name, int scale, int precision)
        : mName{ name }
        , mScale{ scale }
        , mPrecision{ precision } {};

    ~DecimalJSON() = default;

    const json MarshalJSON() {
        return {
            { "name", mName },
            { "scale", mScale },
            { "precision", mPrecision },
        };
    }

private:
    std::string mName{};
    int mScale{};
    int mPrecision{};
};

/**
 * ByteWidthJSON uses for FIXED_SIZE_BINARY type
 *
 * mName: field name
 * mByteWidth: byte width
 */
class ByteWidthJSON : public IDataType {
public:
    ByteWidthJSON(const std::string& name, int byteWidth)
        : mName{ name }
        , mByteWidth{ byteWidth } {};

    ~ByteWidthJSON() = default;

    const json MarshalJSON() {
        return {
            { "name", mName },
            { "byteWidth", mByteWidth },
        };
    }

private:
    std::string mName{};
    int mByteWidth{};
};

/**
 * MapJSON uses for MAP type
 *
 * mName: field name
 * mKeySorted: key is sorted or not
 */
class MapJSON : public IDataType {
public:
    MapJSON(const std::string& name, bool keySorted = false)
        : mName{ name }
        , mKeySorted{ keySorted } {};

    ~MapJSON() = default;

    const json MarshalJSON() {
        return {
            { "name", mName },
            { "keySorted", mKeySorted },
        };
    }

private:
    std::string mName{};
    bool mKeySorted{};
};

namespace datatype {
const std::string kNullType = "null";
const std::string kBoolType = "bool";
const std::string kIntType = "int";
const std::string kFloatingPointType = "floatingpoint";
const std::string kUtf8Type = "utf8";
const std::string kBinaryType = "binary";
const std::string kFixedSizeBinaryType = "fixedsizebinary";
const std::string kDateType = "date";
const std::string kTimestampType = "timestamp";
const std::string kTimeType = "time";
const std::string kIntervalType = "interval";
const std::string kDecimalType = "decimal";
const std::string kListType = "list";
const std::string kStructType = "struct";
const std::string kMapType = "map";
const std::string kDurationType = "duration";

enum TypeName {
    TYPE_NAME_NOT_SET,
    TYPE_NAME_NULL,
    TYPE_NAME_BOOL,
    TYPE_NAME_INT,
    TYPE_NAME_FLOATING_POINT,
    TYPE_NAME_UTF8,
    TYPE_NAME_BINARY,
    TYPE_NAME_FIXED_SIZE_BINARY,
    TYPE_NAME_DATE,
    TYPE_NAME_TIMESTAMP,
    TYPE_NAME_TIME,
    TYPE_NAME_INTERVAL,
    TYPE_NAME_DECIMAL,
    TYPE_NAME_LIST,
    TYPE_NAME_STRUCT,
    TYPE_NAME_MAP,
    TYPE_NAME_DURATION,
};

inline TypeName GetTypeFromString(const std::string& type) {
    static const std::unordered_map<std::string, TypeName> hashMap{
        { kNullType, TYPE_NAME_NULL },
        { kBoolType, TYPE_NAME_BOOL },
        { kIntType, TYPE_NAME_INT },
        { kFloatingPointType, TYPE_NAME_FLOATING_POINT },
        { kUtf8Type, TYPE_NAME_UTF8 },
        { kBinaryType, TYPE_NAME_BINARY },
        { kFixedSizeBinaryType, TYPE_NAME_FIXED_SIZE_BINARY },
        { kDateType, TYPE_NAME_DATE },
        { kTimestampType, TYPE_NAME_TIMESTAMP },
        { kTimeType, TYPE_NAME_TIME },
        { kIntervalType, TYPE_NAME_INTERVAL },
        { kDecimalType, TYPE_NAME_DECIMAL },
        { kListType, TYPE_NAME_LIST },
        { kStructType, TYPE_NAME_STRUCT },
        { kMapType, TYPE_NAME_MAP },
        { kDurationType, TYPE_NAME_DURATION },
    };
    auto item = hashMap.find(type);
    if (item != hashMap.end()) {
        return item->second;
    }
    return TYPE_NAME_NOT_SET;
}

const std::string kPrecisionHalf = "HALF";
const std::string kPrecisionSingle = "SINGLE";
const std::string kPrecisionDouble = "DOUBLE";

enum Precision {
    PRECISION_NOT_SET,
    PRECISION_HALF,
    PRECISION_SINGLE,
    PRECISION_DOUBLE,
};

inline Precision GetPrecisionFromString(const std::string& precision) {
    static const std::unordered_map<std::string, Precision> hashMap{
        { kPrecisionHalf, PRECISION_HALF },
        { kPrecisionSingle, PRECISION_SINGLE },
        { kPrecisionDouble, PRECISION_DOUBLE },
    };
    auto item = hashMap.find(precision);
    if (item != hashMap.end()) {
        return item->second;
    }
    return PRECISION_NOT_SET;
}

const std::string kDayUnit = "DAY";
const std::string kSecondUnit = "SECOND";
const std::string kMillisecondUnit = "MILLISECOND";
const std::string kMicrosecondUnit = "MICROSECOND";
const std::string kNanosecondUnit = "NANOSECOND";

enum DateTimeUnit {
    DATE_TIME_UNIT_NOT_SET,
    DATE_TIME_UNIT_DAY,
    DATE_TIME_UNIT_SECOND,
    DATE_TIME_UNIT_MILLISECOND,
    DATE_TIME_UNIT_MICROSECOND,
    DATE_TIME_UNIT_NANOSECOND,
};

inline DateTimeUnit GetUnitFromString(const std::string& unit) {
    static const std::unordered_map<std::string, DateTimeUnit> hashMap{
        { kDayUnit, DATE_TIME_UNIT_DAY },
        { kSecondUnit, DATE_TIME_UNIT_SECOND },
        { kMillisecondUnit, DATE_TIME_UNIT_MILLISECOND },
        { kMicrosecondUnit, DATE_TIME_UNIT_MICROSECOND },
        { kNanosecondUnit, DATE_TIME_UNIT_NANOSECOND },
    };
    auto item = hashMap.find(unit);
    if (item != hashMap.end()) {
        return item->second;
    }
    return DATE_TIME_UNIT_NOT_SET;
}

const std::string kYearMonthIntervalUnit = "YEAR_MONTH";
const std::string kDayTimeIntervalUnit = "DAY_TIME";
const std::string kMonthDayNanoIntervalUnit = "MONTH_DAY_NANO";

enum IntervalUnit {
    INTERVAL_UNIT_NOT_SET,
    INTERVAL_UNIT_YEAR_MONTH,
    INTERVAL_UNIT_DAY_TIME,
    INTERVAL_UNIT_MONTH_DAY_NANO,
};

inline IntervalUnit GetIntervalUnitFromString(const std::string& unit) {
    static const std::unordered_map<std::string, IntervalUnit> hashMap{
        { kYearMonthIntervalUnit, INTERVAL_UNIT_YEAR_MONTH },
        { kDayTimeIntervalUnit, INTERVAL_UNIT_DAY_TIME },
        { kMonthDayNanoIntervalUnit, INTERVAL_UNIT_MONTH_DAY_NANO },
    };
    auto item = hashMap.find(unit);
    if (item != hashMap.end()) {
        return item->second;
    }
    return INTERVAL_UNIT_NOT_SET;
}

// These macros are for extension types. Refer
// https://github.com/apache/arrow/blob/8e43f23dcc6a9e630516228f110c48b64d13cec6/cpp/src/arrow/extension_type.cc#L166-L167
#define EXTENSION_TYPE_KEY_NAME "ARROW:extension:name"
#define EXTENSION_METADATA_KEY_NAME "ARROW:extension:metadata"

} // namespace datatype

#endif // _DATA_TYPES_H_
