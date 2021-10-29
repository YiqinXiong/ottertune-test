#!/bin/sh

TIUP_PATCH='tiup cluster patch'

if [ $# != 2 ]; then
echo "USAGE: $0 <cluster-name> <patch-path>"
echo "e.g.: $0 tidb-1 ./tikv-hotfix.tar.gz"
exit 1;
fi


$TIUP_PATCH $1 $2 -R tikv --overwrite -y > ./log/patch_tikv.log
