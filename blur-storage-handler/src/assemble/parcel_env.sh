#!/bin/bash
PARCEL_INSTALL=$PARCELS_ROOT/$PARCEL_DIRNAME
if [ -z "$PRESTO_PLUGINS"]; then
  echo "PRESTO_PLUGINS not set, setting var"
  export PRESTO_PLUGINS="$PARCEL_INSTALL"
else
  echo "PRESTO_PLUGINS alread set, $PRESTO_PLUGINS"
  export PRESTO_PLUGINS="$PRESTO_PLUGINS:$PARCEL_INSTALL"
fi
echo "New PRESTO_PLUGINS, $PRESTO_PLUGINS"