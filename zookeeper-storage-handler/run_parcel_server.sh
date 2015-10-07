#!/bin/bash

PROJECT_DIR=`dirname "$0"`
PROJECT_DIR=`cd "$PROJECT_DIR"; pwd`

PARCEL_NAME=`mvn help:evaluate -Dexpression=project.parcel.name | grep -Ev '(^\[|Download\w+:)'`
echo "PARCEL_NAME $PARCEL_NAME"
PARCEL_VERSION=`mvn help:evaluate -Dexpression=project.version | grep -Ev '(^\[|Download\w+:)'`
echo "PARCEL_VERSION $PARCEL_VERSION"
PARCEL_FILE="target/${PARCEL_NAME}-${PARCEL_VERSION}-parcel.tar.gz"
echo "PARCEL_FILE $PARCEL_FILE"

PARCEL_FILE_SHA=$PARCEL_FILE.sha
shasum $PARCEL_FILE | awk '{print $1}' > $PARCEL_FILE_SHA
HASH=`cat $PARCEL_FILE_SHA`
LAST_UPDATED_SEC=`date +%s`
LAST_UPDATED="${LAST_UPDATED_SEC}0000"
HTTP_DIR="$PROJECT_DIR/target/tmp_http"
if [ -d $HTTP_DIR ]; then
  rm -r $HTTP_DIR
fi
mkdir -p $HTTP_DIR
MANIFEST="$HTTP_DIR/manifest.json"
echo "{\"lastUpdated\":${LAST_UPDATED},\"parcels\": [" > $MANIFEST
for DISTRO in el5 el6 sles11 lucid precise trusty squeeze wheezy
do
	if [ $DISTRO != "el5" ] ; then
		echo "," >> $MANIFEST
	fi
	DISTRO_PARCEL="${PARCEL_NAME}-${PARCEL_VERSION}-${DISTRO}.parcel"
	DISTRO_PARCEL_SHA="${PARCEL_NAME}-${PARCEL_VERSION}-${DISTRO}.parcel.sha"
	ln $PARCEL_FILE "${HTTP_DIR}/${DISTRO_PARCEL}"
	ln $PARCEL_FILE_SHA "${HTTP_DIR}/${DISTRO_PARCEL_SHA}"
	echo "{\"parcelName\":\"${DISTRO_PARCEL}\",\"components\": [{\"name\" : \"${PARCEL_NAME}\",\"version\" : \"${PARCEL_VERSION}\",\"pkg_version\": \"${PARCEL_VERSION}\"}],\"hash\":\"${HASH}\"}" >> $MANIFEST
done
echo "]}" >> $MANIFEST
cd $HTTP_DIR
python -m SimpleHTTPServer