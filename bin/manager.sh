#!/bin/sh

# Path where nfdist is installed
if [ -z "$PREFIX" ]; then
	PREFIX="/opt/nfdist"
fi

CONFDIR="${PREFIX}/etc"
. ${CONFDIR}/env.sh

java -cp $CLASSPATH nfdist.Manager "$@"
