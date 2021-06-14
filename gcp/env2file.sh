#!/bin/bash

FNAME="$2"
ENAME="$1"
CMD="echo \$$ENAME"
if [[ y`eval $CMD` = 'y' ]]; then
	echo ERROR: Environment variable $ENAME is empty
	exit
fi
CMD="echo \$$ENAME |sed -e 's/[\]n/\n/g' > $FNAME"
echo writing $ENAME to  $FNAME
eval $CMD
