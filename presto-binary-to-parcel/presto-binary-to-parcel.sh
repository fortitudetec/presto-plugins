#!/bin/bash

if [ -z "$1" ]; then
  echo "No presto binary tarball specified."
  exit 1
fi

PRESTO_TAR_FILE=$1
TMP_VAR=`echo $PRESTO_TAR_FILE | sed -e 's/\.tar\.gz//g'`
IFS='-' read -a array <<< "$TMP_VAR"
PRESTO_VERSION="${array[2]}"
echo "Presto version for binary is $PRESTO_VERSION"

PROJECT_DIR=`dirname "$0"`
PROJECT_DIR=`cd "$PROJECT_DIR"; pwd`

PROJECT_DIR_TMP=$PROJECT_DIR/tmp

if [ -d $PROJECT_DIR_TMP ] ; then
  rm -r $PROJECT_DIR_TMP
fi

mkdir $PROJECT_DIR_TMP
echo "Extracting $PRESTO_TAR_FILE file."
tar -xzf $PRESTO_TAR_FILE -C $PROJECT_DIR_TMP

cd $PROJECT_DIR_TMP
ls -1 | sed -e 'p;s/-server-/-/' | xargs -n2 mv
#PRESTO_DIR=`ls -1`
PRESTO_DIR_LOWERCASE=`ls -1`
PRESTO_DIR=`echo $PRESTO_DIR_LOWERCASE | awk '{print toupper($0)}'`
mv $PRESTO_DIR_LOWERCASE $PRESTO_DIR

PRESTO_DIR_META=$PRESTO_DIR/meta
mkdir $PRESTO_DIR_META

echo "Writing $PRESTO_DIR_META/parcel.json file."
cat > $PRESTO_DIR_META/parcel.json <<JSON
{
  "schema_version": 1,
  "name": "PRESTO",
  "version" : "${PRESTO_VERSION}",
  "setActiveSymlink": true,
  "depends": "",
  "replaces":"PRESTO",
  "conflicts":"",
  "provides": ["PRESTOCOORDINATOR", "PRESTOWORKER"],
  "scripts": {"defines":"presto_parcel_env.sh"},
  "components": [
    {
      "name" : "PRESTO",
      "version" : "${PRESTO_VERSION}",
      "pkg_version": "${PRESTO_VERSION}"
    }
  ],
  "packages" : [],
  "users":  {
    "presto": {
      "longname" : "presto",
      "home" : "/var/lib/presto",
      "shell" : "/bin/bash",
      "extra_groups": []		
    }
  },
  "groups": []
}
JSON

echo "Writing $PRESTO_DIR_META/presto_parcel_env.sh file."
cat > $PRESTO_DIR_META/presto_parcel_env.sh <<PRESTO_SCRIPT
#!/bin/bash
export PRESTO_INSTALL=\$PARCELS_ROOT/\$PARCEL_DIRNAME
echo "***** PREFIX:"
env
echo "**** DONE."
PRESTO_SCRIPT

PARCEL_FILE="$PROJECT_DIR/PRESTO-$PRESTO_VERSION.parcel"

if [ -f $PARCEL_FILE ]; then
  echo "Removing old parcel file $PARCEL_FILE"
  rm $PARCEL_FILE
fi

echo "Creating parcel $PARCEL_FILE"
tar -czf $PARCEL_FILE PRESTO-$PRESTO_VERSION
cd $PROJECT_DIR
echo "Removing tmp dir"
rm -r $PROJECT_DIR_TMP

PARCEL_FILE_SHA=$PARCEL_FILE.sha
shasum $PARCEL_FILE | awk '{print $1}' > $PARCEL_FILE_SHA
HASH=`cat $PARCEL_FILE_SHA`
LAST_UPDATED_SEC=`date +%s`
LAST_UPDATED="${LAST_UPDATED_SEC}0000"
HTTP_DIR="$PROJECT_DIR/http"
if [ -d $HTTP_DIR ]; then
  rm -r $HTTP_DIR
fi
mkdir $HTTP_DIR
MANIFEST="$HTTP_DIR/manifest.json"
echo "{\"lastUpdated\":${LAST_UPDATED},\"parcels\": [" > $MANIFEST
for DISTRO in el5 el6 sles11 lucid precise trusty squeeze wheezy
do
	if [ $DISTRO != "el5" ] ; then
		echo "," >> $MANIFEST
	fi
	DISTRO_PARCEL="PRESTO-${PRESTO_VERSION}-${DISTRO}.parcel"
	DISTRO_PARCEL_SHA="PRESTO-${PRESTO_VERSION}-${DISTRO}.parcel.sha"
	ln $PARCEL_FILE "${HTTP_DIR}/${DISTRO_PARCEL}"
	ln $PARCEL_FILE_SHA "${HTTP_DIR}/${DISTRO_PARCEL_SHA}"
	echo "{\"parcelName\":\"${DISTRO_PARCEL}\",\"components\": [{\"name\" : \"PRESTO\",\"version\" : \"${PRESTO_VERSION}\",\"pkg_version\": \"${PRESTO_VERSION}\"}],\"hash\":\"${HASH}\"}" >> $MANIFEST
done
echo "]}" >> $MANIFEST

echo "To start parcel server:"
echo "cd http"
echo "python -m SimpleHTTPServer"


