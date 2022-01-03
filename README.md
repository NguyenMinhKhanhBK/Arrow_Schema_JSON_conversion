# Arrow Schema <-> JSON conversion

## Overview
This repo is a simple library that converts [`arrow::Schema`](https://arrow.apache.org/docs/cpp/tables.html#schemas) into JSON and vice versa. Due to time limitation, I have just supported some basic data types.

**Supported types**
- Nullable
- Boolean
- Integer: INT8, UINT8, INT16, UINT16, INT32, UINT32, INT64, UINT64
- Floating point: FLOAT16, FLOAT32, FLOAT64
- Decimal
- String
- Binary
- Time related types: TIME32, TIME64, TIMESTAMP
- Date related types: DATE32, DATE64
- Interval: MONTH_INTERVAL, DAY_TIME_INTERVAL, MONTH_DAY_NANO_INTERVAL
- Duration
- Struct
- List
- Map


## Repo structure
```
|-- build.sh                
|-- CMakeLists.txt          
|-- Dockerfile              
|-- include     
|   |-- nlohmann            
|   |   `-- json.hpp
|   `-- Schema_JSON_Conversion.h
|-- run_cppcheck.sh
|-- src
|   |-- DataTypes.h
|   |-- IDataType.h
|   |-- Json_To_Schema.cpp
|   `-- Schema_To_Json.cpp
`-- test
    |-- helper.h
    `-- test.cpp
```
- `build.sh`: Build script 
- `CMakeLists.txt`: CMake file to build library and unittest
- `Dockerfile`: Provides docker environment including neccessary setup
- `include/nlohmann/json.hpp`: Single-include-header library to work with JSON format
- `run_cppcheck.sh`: Simple script for linter
- `src/`: Library implementation
- `test/`: Unittest and helper functions

## How to build and run tests
There are 2 ways to build and run tests: **use docker** (highly recommended) and **prepare environment without docker**

### Method 1: Use docker (highly recommended)
- Make sure you have docker installed on your machine
- Clone source code and build docker image

```
git clone git@github.com:NguyenMinhKhanhBK/Arrow_Schema_JSON_conversion.git
cd Arrow_Schema_JSON_conversion/
docker build -t json_schema_image .
```
- Run the newly-built docker image and execute build script

```
docker run -it --rm json_schema_image /bin/bash
./build.sh
```
- You can run unittest binary or cppcheck

```
./build/Test
./run_cppcheck.sh
```
### Method 2: Prepare environment without docker
- Install build essential and cmake

```
sudo apt update
sudo apt install build-essential cmake wget cppcheck
```
- Install arrow library

```
sudo apt install -y -V ca-certificates lsb-release wget
wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev
```
- Clone source code, execute build script then run unittest as well as cppcheck

```
git clone git@github.com:NguyenMinhKhanhBK/Arrow_Schema_JSON_conversion.git
cd Arrow_Schema_JSON_conversion/
./build.sh
./build/Test
./run_cppcheck.sh
```

## JSON structures
- `arrow::Schema`

```
{
    "schema": {
        "fields": [
            <JSON_OF_FIELDS>
        ],
        "metadata": [
            {
                "key": "k1",
                "value": "v1"
            },
            {
                "key": "k2",
                "value": "v2"
            },
            {
                "key": "k3",
                "value": "v3"
            }
        ]
    }
}
```
`<JSON_OF_FIELDS>` is JSON format of `arrow::Field`, which is different depending on the data type that this field is holding
- Nullable type
```
{
    "name": "nulls",
    "nullable": true,
    "type": {
        "name": "null"
    },
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Boolean

```
{
    "name": "bools",
    "nullable": true,
    "type": {
        "name": "bool"
    },
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Integer

```
{
    "name": "int8s",
    "nullable": true,
    "type": {
        "bitWidth": 8,
        "isSigned": true,
        "name": "int",
        "unit": ""
    },
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Floating point

```
{
    "name": "float32s",
    "nullable": true,
    "type": {
        "name": "floatingpoint",
        "precision": "SINGLE"
    },
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Decimal
```
{
    "name": "decimals",
    "nullable": true,
    "type": {
        "name": "decimal",
        "precision": 10,
        "scale": 1
    },
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- String

```
{
    "name": "strings",
    "nullable": true,
    "type": {
        "name": "utf8"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Binary
```
{
    "name": "bytes",
    "nullable": true,
    "type": {
        "name": "binary"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```

- Time related types
```
{
    "name": "time32ms",
    "nullable": true,
    "type": {
        "bitWidth": 32,
        "isSigned": false,
        "name": "time",
        "unit": "MILLISECOND"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Date related types
```
{
    "name": "date64s",
    "nullable": true,
    "type": {
        "name": "date",
        "timezone": "",
        "unit": "MILLISECOND"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Interval

```
{
    "name": "months",
    "nullable": true,
    "type": {
        "name": "interval",
        "timezone": "",
        "unit": "YEAR_MONTH"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Duration

```
{
    "name": "durations_s",
    "nullable": true,
    "type": {
        "name": "duration",
        "timezone": "",
        "unit": "SECOND"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Struct
```
{
    "children": [
        {
            "name": "f1",
            "nullable": true,
            "type": {
                "bitWidth": 32,
                "isSigned": true,
                "name": "int",
                "unit": ""
            }
        },
        {
            "name": "f2",
            "nullable": true,
            "type": {
                "name": "utf8"
            }
        }
    ],
    "name": "struct_nullable",
    "nullable": true,
    "type": {
        "name": "struct"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```

- List

```
{
    "children": [
        {
            "name": "item",
            "nullable": true,
            "type": {
                "bitWidth": 32,
                "isSigned": true,
                "name": "int",
                "unit": ""
            }
        }
    ],
    "name": "list_nullable",
    "nullable": true,
    "type": {
        "name": "list"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```
- Map

```
{
    "children": [
        {
            "item": {
                <JSON_OF_VALUE_FIELD>
            },
            "key": {
                <JSON_OF_KEY_FIELD>
            }
        }
    ],
    "name": "map_int_utf8",
    "nullable": true,
    "type": {
        "keySorted": true,
        "name": "map"
    }
    "metadata": [
        <KEY_PAIR_METADATA>
    ]
}
```

