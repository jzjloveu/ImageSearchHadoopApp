#!/bin/bash

testf=${1}
if [ -f result ] ; then
rm result
rm *.jpg
fi
res=`hadoop fs -get $testf/result .`
ext=".jpg"
while read LINE || [[ -n $LINE ]]; do
file=`echo $LINE | cut -d ':' -f 1`
hadoop fs -get CAT2_extract/${file//[[:blank:]]/}$ext .
done < result
