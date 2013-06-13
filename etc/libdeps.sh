#!/bin/sh

# List of required libraries
LIBS="nfdist.jar"
LIBS="$LIBS log4j-1.2.jar slf4j-api.jar slf4j-log4j12.jar protobuf.jar zookeeper.jar"
LIBS="$LIBS hadoop-common.jar hadoop-auth.jar hadoop-hdfs.jar hadoop-annotations.jar"
LIBS="$LIBS commons-lang.jar commons-logging.jar commons-configuration.jar"
LIBS="$LIBS commons-collections3.jar commons-cli.jar commons-io-2.4.jar guava.jar"

# Search path for library dependencies
if [ -z "$LIBDIRS" ]; then
	LIBDIRS="./lib"
fi
LIBDIRS="$LIBDIRS /usr/share/java /usr/lib/hadoop /usr/lib/hadoop-hdfs /usr/lib/zookeeper"

if [ -z "$CLASSPATH" ]; then
	CLASSPATH="./etc"
fi

# Append full paths of the libraries to the classpath
for LIB in $LIBS; do
	found=0
	for LIBDIR in $LIBDIRS; do
		if [ -e "${LIBDIR}/${LIB}" ]; then
			found=1
			CLASSPATH="$CLASSPATH:${LIBDIR}/${LIB}"
			break
		fi
	done
	if [ $found -ne 1 ]; then
		echo "Library not found: $LIB"
		exit 1
	fi
done
