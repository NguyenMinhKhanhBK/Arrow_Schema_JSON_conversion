#ifndef _DATA_TYPES_H_
#define _DATA_TYPES_H_

#include "IDataType.h"

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

class ListSizeJSON : public IDataType {
public:
    ListSizeJSON(const std::string& name, int listSize)
        : mName{ name }
        , mListSize{ listSize } {};

    ~ListSizeJSON() = default;

    const json MarshalJSON() {
        return {
            { "name", mName },
            { "listSize", mListSize },
        };
    }

private:
    std::string mName{};
    int mListSize{};
};

#endif // _DATA_TYPES_H_
