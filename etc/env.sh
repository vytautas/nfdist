#!/bin/sh

if [ -z "${PREFIX}" ]; then
	PREFIX=$(pwd)
fi

# Search path for the libraries
LIBDIRS="${PREFIX}/lib"

# Class path for configuration and libraries
if [ -z "${CONFDIR}" ]; then
	CONFDIR="${PREFIX}/etc"
fi

CLASSPATH="${CONFDIR}"

# Load additional libraries into CLASSPATH
. ${CONFDIR}/libdeps.sh

# Native hadoop libraries
LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/hadoop/lib/native
