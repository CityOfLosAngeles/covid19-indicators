#!/bin/bash
FNAME="$1"
ENAME="$2"
CMD="echo \$$ENAME |sed -e 's/[\]n/\n/g' > $FNAME"
echo writing $ENAME to  $FNAME
eval $CMD
