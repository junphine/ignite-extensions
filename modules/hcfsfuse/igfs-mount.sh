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
# set JVM_OPTS=-agentlib:jdwp=transport=dt_socket,address=0.0.0.0:19007,server=y,suspend=n

#-o allow_other,big_writes

#export JVM_OPTS="-agentlib:jdwp=transport=dt_socket,server=n,address=0.0.0.0:19007,suspend=n "
export JVM_OPTS="$JVM_OPTS -Dlog4j.configurationFile=config/log4j2-console.xml -Dhcfs.fuse.write.type=CACHE_THROUGH -Dhcfs.fuse.cache.enabled=true -Dhcfs.fuse.random.access.enabled=true"
export DEFAULT_CONFIG="-c config/core-site.xml -m /home/zhangjunfeng/igfs -r igfs://igfs@localhost:10500/ -jniFuse -debug"
#export DEFAULT_CONFIG="-c config/core-site.xml -m /home/zhangjunfeng/igfs -r file:///mnt/d/Data//uploads/  -debug"
export MAIN_CLASS=hcfsfuse.fuse.HCFSFuse
export USER_LIBS=${IGNITE_HOME_TMP}/hadoop/*
#
# Start router service.
#
. "${SCRIPTS_HOME}"/ignite.sh $@
