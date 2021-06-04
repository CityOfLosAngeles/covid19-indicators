#!/bin/bash
FNAME="$2"
ENAME="$1"
CMD="echo \$$ENAME |sed -e 's/[\]n/\n/g' > $FNAME"
echo writing $ENAME to  $FNAME
eval $CMD
