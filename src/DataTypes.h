#ifndef _DATA_TYPES_H_
#define _DATA_TYPES_H_

#include "IDataType.h"
#include <arrow/type.h>

class NameJSON : public IDataType {
public:
    NameJSON(const std::string& name)
        : mName{ name } {};

    ~NameJSON() = default;

    const json MarshalJSON() override {
        return {
            { "name", mName },
            //{"children", {{}}},
        };
    }

private:
    std::string mName{};
};

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
            //{"children", {{}}},
        };
    }

private:
    std::string mName{};
    bool mSigned{};
    int mBitWidth{};
    std::string mUnit{};
};

class FloatJSON : public IDataType {
public:
    FloatJSON(const std::string& name, const std::string& precision)
        : mName{ name }
        , mPrecision{ precision } {};

    ~FloatJSON() = default;

    const json MarshalJSON() override {
        return {
            { "name", mName }, { "precision", mPrecision },
            //{"children", {{}}},
        };
    }

private:
    std::string mName{};
    std::string mPrecision{};
};

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


TypeName GetTypeFromString(const std::string& type) {
    static const std::unordered_map<std::string, TypeName> hashMap {
        {kNullType, TYPE_NAME_NULL},
        {kBoolType, TYPE_NAME_BOOL},
        {kIntType, TYPE_NAME_INT},
        {kFloatingPointType, TYPE_NAME_FLOATING_POINT},
        {kUtf8Type, TYPE_NAME_UTF8},
        {kBinaryType, TYPE_NAME_BINARY},
        {kFixedSizeBinaryType, TYPE_NAME_FIXED_SIZE_BINARY},
        {kDateType, TYPE_NAME_DATE},
        {kTimestampType, TYPE_NAME_TIMESTAMP},
        {kTimeType, TYPE_NAME_TIME},
        {kIntervalType, TYPE_NAME_INTERVAL},
        {kDecimalType, TYPE_NAME_DECIMAL},
        {kListType, TYPE_NAME_LIST},
        {kStructType, TYPE_NAME_STRUCT},
        {kMapType, TYPE_NAME_MAP},
        {kDurationType, TYPE_NAME_DURATION},
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

Precision GetPrecisionFromString(const std::string& precision) {
    static const std::unordered_map<std::string, Precision> hashMap {
        {kPrecisionHalf, PRECISION_HALF},
        {kPrecisionSingle, PRECISION_SINGLE},
        {kPrecisionDouble, PRECISION_DOUBLE},
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

DateTimeUnit GetUnitFromString(const std::string& unit) {
    static const std::unordered_map<std::string, DateTimeUnit> hashMap {
        {kDayUnit, DATE_TIME_UNIT_DAY},
        {kSecondUnit, DATE_TIME_UNIT_SECOND},
        {kMillisecondUnit, DATE_TIME_UNIT_MILLISECOND},
        {kMicrosecondUnit, DATE_TIME_UNIT_MICROSECOND},
        {kNanosecondUnit, DATE_TIME_UNIT_NANOSECOND},
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

IntervalUnit GetIntervalUnitFromString(const std::string unit) {
    static const std::unordered_map<std::string, IntervalUnit> hashMap {
        {kYearMonthIntervalUnit, INTERVAL_UNIT_YEAR_MONTH},
        {kDayTimeIntervalUnit, INTERVAL_UNIT_DAY_TIME},
        {kMonthDayNanoIntervalUnit, INTERVAL_UNIT_MONTH_DAY_NANO},
    };
    auto item = hashMap.find(unit);
    if (item != hashMap.end()) {
        return item->second;
    }
    return INTERVAL_UNIT_NOT_SET;
}

#define EXTENSION_TYPE_KEY_NAME "ARROW:extension:name"
#define EXTENSION_METADATA_KEY_NAME "ARROW:extension:metadata"

}

#endif // _DATA_TYPES_H_
