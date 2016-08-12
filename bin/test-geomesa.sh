#!/usr/bin/env bash

## variables

# make sure these paths match
GMVER="1.2.5-SNAPSHOT"
#GMVER="1.2.4"
#TEST_CL_PATH="${HOME}/dev/cloud-local"
TEST_CL_PATH="/opt/devel/src/cloud-local"
TEST_ROOT="/tmp"

# gm stuff
GMSRC="/opt/devel/src/geomesa"
GM="${TEST_ROOT}/geomesa-tools-${GMVER}"
GM_DIST="${TEST_ROOT}/geomesa-${GMVER}"
GMTMP="geomesa-test-tmp"
geomesa="$GM/bin/geomesa"
export GEOMESA_HOME=${GM}

# accumulo namespace and table
NS="gmtest"
CATALOG="${NS}.gmtest1"

## functions

function accrun() {
    accumulo shell -u root -p secret -e "$1"
}

function setup_cloudlocal() {
    . "${TEST_CL_PATH}/bin/config.sh"
}

function build_geomesa() {
    echo "building geomesa"
    cwd=`pwd`
    cd $GM_SOURCE
    mvn clean install -DskipTests
    cd $cwd
}

function unpack_geomesa() {
    echo "unpacking geomesa"
    rm -rf $GM
    tar -xzf $GMSRC/geomesa-tools/target/geomesa-tools-${GMVER}-bin.tar.gz -C $TEST_ROOT

    rm -rf $GM_DIST
    tar -xzf $GMSRC/geomesa-dist/target/geomesa-${GMVER}-bin.tar.gz -C $TEST_ROOT
}

function setup_geomesa_accumulo() {
    echo "placing iter in hdfs"
    itrdir="/geomesa/iter/${NS}"
    itrfile="geomesa-accumulo-distributed-runtime-${GMVER}.jar"
    set -x
    hadoop fs -rm -r $itrdir
    hadoop fs -mkdir -p $itrdir 
    hadoop fs -put ${GM_DIST}/dist/accumulo/${itrfile} ${itrdir}/${itrfile}
    set +x

    echo "configuring namespaces"
    set -x
    accrun "deletenamespace ${NS} -f"
    accrun "createnamespace ${NS}"
    accrun "config -d general.vfs.context.classpath.${NS}"
    accrun "config -s general.vfs.context.classpath.${NS}=hdfs://localhost:9000${itrdir}/${itrfile}"
    accrun "config -ns ${NS} -s table.classpath.context=${NS}"
    set +x
}

function test_local() {
    echo "testing local ingest"
    set -x
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-csv  -C example-csv                                      $GM/examples/ingest/csv/example.csv
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-json -C example-json                                     $GM/examples/ingest/json/example.json
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-json -C $GM/examples/ingest/json/example_multi_line.conf $GM/examples/ingest/json/example_multi_line.json
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-xml  -C example-xml                                      $GM/examples/ingest/xml/example.xml
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-xml  -C example-xml-multi                                $GM/examples/ingest/xml/example_multi_line.xml
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-avro -C example-avro-no-header                           $GM/examples/ingest/avro/example_no_header.avro
    #$geomesa ingest -u root -p secret -c ${CATALOG} -s example-avro -C example-avro-header                             $GM/examples/ingest/avro/with_header.avro
    set +x
}

function test_hdfs() {
    echo "testing HDFS ingest"
    set -x
    hadoop fs -ls /user/$(whoami)
    
    hadoop fs -put -f $GM/examples/ingest/csv/example.csv
    hadoop fs -put -f $GM/examples/ingest/json/example.json
    hadoop fs -put -f $GM/examples/ingest/json/example_multi_line.json
    hadoop fs -put -f $GM/examples/ingest/xml/example.xml
    hadoop fs -put -f $GM/examples/ingest/xml/example_multi_line.xml
    hadoop fs -put -f $GM/examples/ingest/avro/example_no_header.avro
    #hadoop fs -put $GM/examples/ingest/avro/with_header.avro
    
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-csv  -C example-csv                                      hdfs://localhost:9000/user/$(whoami)/example.csv
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-json -C example-json                                     hdfs://localhost:9000/user/$(whoami)/example.json
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-json -C $GM/examples/ingest/json/example_multi_line.conf hdfs://localhost:9000/user/$(whoami)/example_multi_line.json
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-xml  -C example-xml                                      hdfs://localhost:9000/user/$(whoami)/example.xml
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-xml  -C example-xml-multi                                hdfs://localhost:9000/user/$(whoami)/example_multi_line.xml
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-avro -C example-avro-no-header                           hdfs://localhost:9000/user/$(whoami)/example_no_header.avro
    #$geomesa ingest -u root -p secret -c ${CATALOG} -s example-avro -C example-avro-header                             hdfs://localhost:9000/user/$(whoami)/with_header.avro
    set +x
}

function test_s3() {
    echo "testing S3 ingest"
    set -x
    # uses gmdata jar in tools lib
    # s3n
    $geomesa ingest -u root -p secret -c ${CATALOG} -s geolife -C geolife s3n://ahulbert-test/geolife/Data/000/Trajectory/20081023025304.plt s3n://ahulbert-test/geolife/Data/000/Trajectory/20081024020959.plt
    # s3a
    $geomesa ingest -u root -p secret -c ${CATALOG} -s geolife -C geolife s3a://ahulbert-test/geolife/Data/000/Trajectory/20081023025304.plt s3a://ahulbert-test/geolife/Data/000/Trajectory/20081024020959.plt
    set +x
}

function differ() {
  local f=$1
  echo "diff $f"
  diff "${target}" "$f"
  echo "done"
}

function test_export() {
    echo "testing export"
    set -x
    if [[ -d "$GMTMP" ]]; then
      rm "${TEST_ROOT}/${GMTMP}" -rf
    fi
    
    t="${TEST_ROOT}/${GMTMP}"
    mkdir $t
    
    # Export some formats
    $geomesa export -u root -p secret -c ${CATALOG} -f example-csv -F csv  > "$t/e.csv"
    $geomesa export -u root -p secret -c ${CATALOG} -f example-csv -F avro > "$t/e.avro"
    $geomesa export -u root -p secret -c ${CATALOG} -f example-csv -F tsv  > "$t/e.tsv"
    
    # Reingest automatically those formats both locally and via hdfs
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-avro "$t/e.avro"
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-csv  "$t/e.csv"
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-tsv  "$t/e.tsv"
    
    hadoop fs -put -f "$t/e.avro"
    hadoop fs -put -f "$t/e.csv"
    hadoop fs -put -f "$t/e.tsv"
    
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-avro-hdfs hdfs://localhost:9000/user/$(whoami)/e.avro
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-csv-hdfs  hdfs://localhost:9000/user/$(whoami)/e.csv
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-tsv-hdfs  hdfs://localhost:9000/user/$(whoami)/e.tsv
    
    # compare output of reimported tsv,csv,avro to standard export
    $geomesa export -u root -p secret -c ${CATALOG} -f re-avro       -F csv  | sort >  "$t/re.avro.export"
    $geomesa export -u root -p secret -c ${CATALOG} -f re-avro-hdfs  -F csv  | sort >  "$t/re.avro.hdfs.export"
    $geomesa export -u root -p secret -c ${CATALOG} -f re-csv        -F csv  | sort >  "$t/re.csv.export"
    $geomesa export -u root -p secret -c ${CATALOG} -f re-csv-hdfs   -F csv  | sort >  "$t/re.csv.hdfs.export"
    $geomesa export -u root -p secret -c ${CATALOG} -f re-tsv        -F csv  | sort >  "$t/re.tsv.export"
    $geomesa export -u root -p secret -c ${CATALOG} -f re-tsv-hdfs   -F csv  | sort >  "$t/re.tsv.hdfs.export"
    
    target="$t/e.csv.sorted"
    cat "$t/e.csv" | sort > "$target"

    differ "$t/re.avro.export"
    differ "$t/re.avro.hdfs.export"
    differ "$t/re.csv.export"
    differ "$t/re.csv.hdfs.export"
    differ "$t/re.tsv.export"
    differ "$t/re.tsv.hdfs.export"
    set +x
}

function test_vis() {
    echo "testing visibilities"
    set -x
    $geomesa ingest -u root -p secret -c ${CATALOG} -s example-csv -f viscsv  -C example-csv-with-visibilities $GM/examples/ingest/csv/example.csv
    set +x

    echo "auths: ''"
    accumulo shell -u root -p secret -e "setauths -u root -s ''"
    res=$($geomesa export -u root -p secret -c ${CATALOG} -f viscsv | wc -l)
    if [[ "${res}" -ne "1" ]]; then
        echo "error vis should be 1"
        exit 1
    fi

    echo "auths: 'user'"
    accumulo shell -u root -p secret -e "setauths -u root -s user"
    res=$($geomesa export -u root -p secret -c ${CATALOG} -f viscsv | wc -l)
    if [[ "${res}" -ne "3" ]]; then
        echo "error vis should be 3"
        exit 2
    fi

    echo "auths: 'user,admin'"
    # no auths gets no data
    accumulo shell -u root -p secret -e "setauths -u root -s user,admin"
    res=$($geomesa export -u root -p secret -c ${CATALOG} -f viscsv | wc -l)
    if [[ "${res}" -ne "4" ]]; then
        echo "error vis should be 4"
        exit 3
    fi
}

## main

setup_cloudlocal
#build_geomesa
unpack_geomesa
setup_geomesa_accumulo

test_local
test_hdfs
test_export
test_vis

echo "DONE"
