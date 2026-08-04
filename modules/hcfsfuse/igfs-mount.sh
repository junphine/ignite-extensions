#!/bin/bash
#
#
# Mount command line loader.
#

#
# Import common functions.
#
if [ "${IGNITE_HOME}" = "" ]; then 
    IGNITE_HOME_TMP="$(dirname "$(cd "$(dirname "$0")"; "pwd")")"
else 
    IGNITE_HOME_TMP=${IGNITE_HOME}
fi

#
# Set SCRIPTS_HOME - base path to scripts.
#
SCRIPTS_HOME="${IGNITE_HOME_TMP}/bin"

source "${SCRIPTS_HOME}"/include/functions.sh

#
# Discover IGNITE_HOME environment variable.
#
setIgniteHome

#
# Set router service environment.
# set JVM_OPTS=-agentlib:jdwp=transport=dt_socket,address=0.0.0.0:19003,server=y,suspend=n

export JVM_OPTS=" -Djni.debug=true -agentlib:jdwp=transport=dt_socket,address=0.0.0.0:19007,server=y,suspend=n "
export DEFAULT_CONFIG="-c config/core-site.ignite.xml -m /home/zhangjunfeng/igfs -r /test/ -debug"
export MAIN_CLASS=hcfsfuse.fuse.HCFSFuse
export USER_LIBS=${IGNITE_HOME_TMP}/hadoop/*
#
# Start router service.
#
. "${SCRIPTS_HOME}"/ignite.sh $@
