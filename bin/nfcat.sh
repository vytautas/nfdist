#!/bin/sh

# Append a path where nfdump and nfcat are installed
PATH=$PATH:/opt/nfdump/bin

#combine and pipe everything into nfdump
nfcat | nfdump "$@"
