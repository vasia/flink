#!/bin/sh

if [ $# -eq 0 ]; then
	echo "Servernummer fehlt."
	exit 1
fi
ssh -L 8081:stream$1.inf.tu-dresden.de:8081 marius@stream$1.inf.tu-dresden.de
