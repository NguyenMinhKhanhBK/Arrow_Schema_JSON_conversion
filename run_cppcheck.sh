#! /usr/bin/env bash

# print version
cppcheck --version

# run cppcheck
cppcheck --enable=warning,performance src/ test/ 2>&1 | tee cppcheck_report.xml

# count errors found
ERRORS_FOUND=$(grep "severity=\"error\"" cppcheck_report.xml | wc -l)
if [ $ERRORS_FOUND -gt 0 ] ; then
    echo "Error: cppcheck found $ERRORS_FOUND errors"
    exit 1
else
    echo "Cppcheck didn't find any errors"
    exit 0
fi
