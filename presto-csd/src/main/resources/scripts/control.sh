#!/bin/bash

PRESTO_HOME="/var/lib/presto"
PRESTO_ETC="./etc"
PRESTO_LOG_LEVELS="$PRESTO_ETC/log.properties"
PRESTO_CONFIG_FILE="$PRESTO_ETC/config.properties"
PRESTO_NODE_FILE="$PRESTO_ETC/node.properties"
PRESTO_JVM_FILE="$PRESTO_ETC/jvm.config"
PRESTO_LOG_FILE=/var/log/presto
DISCOVERY_HOST_PORT=`cat presto-coordinator.properties | sed 's/http-server\.http.port\=//'`
DISCOVERY_URI="http://${DISCOVERY_HOST_PORT}"

setup_node_id() {
  echo "setup_node_id"
  NODE_ID_FILE=$PRESTO_HOME/node.id
  if [ ! -f $NODE_ID_FILE ]; then
    uuidgen > $NODE_ID_FILE
  fi
  export PRESTO_NODE_ID=`cat $NODE_ID_FILE`
}

setup_config() {
  echo "setup_config"
  if [ "$PRESTO_PROCESS_TYPE" == "worker" ]; then
      cat >> $PRESTO_CONFIG_FILE <<CONFIG
coordinator=false
discovery.uri=${DISCOVERY_URI}
CONFIG
  fi

  if [ "$PRESTO_PROCESS_TYPE" == "coordinator" ]; then
      cat >> $PRESTO_CONFIG_FILE <<CONFIG
coordinator=true
node-scheduler.include-coordinator=true
discovery-server.enabled=true
discovery.uri=${DISCOVERY_URI}
CONFIG
  fi
}

setup_log_levels() {
  echo "setup_log_levels"
  cat > $PRESTO_LOG_LEVELS <<LOG
com.facebook.presto=INFO
LOG
}

setup_etc() {
  JAVA_HEAP=$1
  echo "setup_etc"
  if [ ! -d $PRESTO_ETC ]; then
    mkdir $PRESTO_ETC
    
    setup_node_id
    echo "Node id $PRESTO_NODE_ID"
    echo "Copying node.properties to $PRESTO_NODE_FILE"
    cp node.properties $PRESTO_NODE_FILE    
    cat >>$PRESTO_NODE_FILE <<NODE_PROP
node.id=${PRESTO_NODE_ID}
NODE_PROP

    echo "Copying config.properties to $PRESTO_CONFIG_FILE"
    cp config.properties $PRESTO_CONFIG_FILE
    setup_config
    
    echo "Setting up log level file $PRESTO_LOG_LEVELS"
    setup_log_levels
 
    echo "Writing jvm.config to $PRESTO_JVM_FILE"
    JVM_CONFIG=`cat jvm.config | sed 's/jvm\.config=//'`
    IFS=' ' read -ra JVM_CONFIG_ARRAY <<< "$JVM_CONFIG"
    jlen=${#JVM_CONFIG_ARRAY[@]}
    for ((i=0; i<${jlen}; i++));
    do
      echo ${JVM_CONFIG_ARRAY[$i]} >> $PRESTO_JVM_FILE
    done
    echo "-Xmx$JAVA_HEAP" >> $PRESTO_JVM_FILE
    echo "-Xms$JAVA_HEAP" >> $PRESTO_JVM_FILE

  else 
    echo "etc dir already exists."
  fi

}

containsElement() {
  echo "containsElement"
  local e
  for e in "${@:2}"; do [[ "$e" == "$1" ]] && return 0; done
  return 1
}

createJMXCatalog() {
  echo 'connector.name=jmx'>etc/catalog/jmx.properties
}

createCDHCatalogs() {
  cat catalog.properties | java -jar $PRESTO_INSTALL/ft_json_to_catalog/presto-json-catalog-*.jar etc/catalog/
}

setup_environment() {
  echo "setup_environment"
  mkdir etc/catalog
  if [ $? -ne 0 ]; then
	echo "Could not create etc/catalog"
    exit 1
  fi

  createJMXCatalog
  createCDHCatalogs

  HADOOP_USER_NAME=presto hadoop fs -get catalog/* etc/catalog/
  if [ $? -ne 0 ]; then
	echo "Could not copy catalog from HDFS to etc/catalog"
  fi

  PARCEL_DIR=$PRESTO_INSTALL

  mkdir lib
  # Link existing libs
  cd lib
  for f in $PARCEL_DIR/lib/*; do
    ln -s $f
    if [ $? -ne 0 ]; then
      echo "Could not link $PARCEL_DIR/lib/$f locally"
      exit 1
    fi
  done
  cd ..
  
  mkdir plugin
  if [ $? -ne 0 ]; then
	echo "Could not create plugin dir"
    exit 1
  fi

  cd plugin
  # Link existing plugins
  for f in $PARCEL_DIR/plugin/*; do
    ln -s $f
  done
    
  IFS=':' read -ra PLUGINS <<< "$PRESTO_PLUGINS"
  for f in "${PLUGINS[@]}"; do
    IFS='-' read -ra FILE_ARRAY <<< "$f"
    NEWPLUGIN="${FILE_ARRAY[0]}"
    IFS='|' read -ra NEWPLUGIN_PLUS_ALIAS_ARRAY <<< "NEWPLUGIN"
    if [ ${#NEWPLUGIN_PLUS_ALIAS_ARRAY[@]} -eq 1 ]; then
      LINKNAME=`basename $NEWPLUGIN`
    else
      NEWPLUGIN=${NEWPLUGIN_PLUS_ALIAS_ARRAY[0]}
      LINKNAME=${NEWPLUGIN_PLUS_ALIAS_ARRAY[1]}
    fi
    # remove any existing plugins for an overide plugin
    if [ -h $LINKNAME ]; then
      echo "Removing plugin link $LINKNAME -> $NEWPLUGIN"
      rm $LINKNAME
    fi
	echo "Adding Plugin $NEWPLUGIN"
    ln -s $NEWPLUGIN
  done
  cd ..  
  
  cp -r $PARCEL_DIR/bin .
  if [ $? -ne 0 ]; then
	echo "Could not cp $PARCEL_DIR/bin"
    exit 1
  fi

  chown -R presto:presto .
}

start_process() {
  echo "start_process"
  setup_etc $1
  setup_environment
  echo "Starting presto process"
  exec sudo -u presto bin/launcher run -v --server-log-file=/var/log/presto/server.log
}

start_coordinator() {
  echo "start_coordinator"
  export PRESTO_PROCESS_TYPE="coordinator"
  create_dir $1
  create_dir $2
  start_process $3
}

start_worker() {
  echo "start_worker"
  export PRESTO_PROCESS_TYPE="worker"
  create_dir $1
  create_dir $2
  start_process $3
}

create_dir() {
  echo "create_dir $1"
  NEW_DIR=$1
  if [ ! -d $NEW_DIR ]; then
    mkdir -p $NEW_DIR
    if [ $? -ne 0 ]; then
	  echo "Could not create $NEW_DIR"
      exit 1
    fi
    chown -R presto:presto $NEW_DIR
    if [ $? -ne 0 ]; then
      echo "Could not change owner of $NEW_DIR to presto:presto"
      exit 1
    fi
    echo "Dir [$NEW_DIR] created."
  else 
    echo "Dir [$NEW_DIR] already exists."
  fi
}

create_hdfs_dir() {
  NEW_DIR=$1
  HADOOP_USER_NAME=hdfs hadoop fs -mkdir $NEW_DIR
  HADOOP_USER_NAME=hdfs hadoop fs -chown -R presto:presto $NEW_DIR
}

action="$1"

if [ "${action}" == "" ] ;then
  usage
fi

case ${action} in
  (start-coordinator)
  	start_coordinator $2 $3 $4
    ;;
  (start-worker)
  	start_worker $2 $3 $4
    ;;
  (create_dir)
    create_dir $2
    ;;
  (create_hdfs_dir)
    create_hdfs_dir $2
    ;;    
  (*)
    echo "Unknown command[${action}]"
    ;;
esac







