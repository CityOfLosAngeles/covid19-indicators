#!/bin/bash
FNAME="$1"
ENAME="$2"
CMD="echo \$$ENAME > $FNAME"
echo writing $ENAME to  $FNAME
eval $CMD
