#!/usr/bin/env bash

# you needito set these and put the dist and tools in /tmp and have cloudlocal at the path below
GMVER="1.3.0-m1"
VER_SHORT="130m1"   # TODO make this read from ${GMVER}
#TEST_CL_PATH="${HOME}/dev/cloud-local"
TEST_CL_PATH=/opt/devel/src/cloud-local
SCALAVER="2.11"

# Twitter data
TWITTER_FILE=/fill/me/in

# gm stuff
GM="/tmp/geomesa-accumulo_${SCALAVER}-${GMVER}"
GMTMP="geomesa-test-tmp"
geomesa="$GM/bin/geomesa"
export GEOMESA_ACCUMULO_HOME=${GM}

. "${TEST_CL_PATH}/bin/config.sh"

NS="gmtest${VER_SHORT}"
CATALOG="${NS}.gmtest1"

function accrun() {
    accumulo shell -u root -p secret -e "$1"
}


function setup()  {
	set -x
    echo "Placing iter in hdfs" && \
    itrdir="/geomesa/iter/${NS}" && \
    itrfile="geomesa-accumulo-distributed-runtime_${SCALAVER}-${GMVER}.jar" && \
    (hadoop fs -test -d $itrdir && hadoop fs -rm -r $itrdir); \
    hadoop fs -mkdir -p $itrdir && \
    hadoop fs -put ${GM}/dist/accumulo/${itrfile} ${itrdir}/${itrfile} && \
    
    echo "configuring namespaces" && \
    (test "$(accrun "namespaces" | grep $NS | wc -l)" == "1" && accrun "deletenamespace ${NS} -f"); \
    accrun "createnamespace ${NS}" && \
    (test "$(accrun "config -np" | grep "general.vfs.context.classpath.${NS}" | wc -l)" == "1" && accrun "config -d general.vfs.context.classpath.${NS}"); \
    accrun "config -s general.vfs.context.classpath.${NS}=hdfs://localhost:9000${itrdir}/${itrfile}" && \
    accrun "config -ns ${NS} -s table.classpath.context=${NS}"
	set +x
}

function test_local() {
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
	set -x
    hadoop fs -ls /user/$(whoami)
    
    hadoop fs -put $GM/examples/ingest/csv/example.csv
    hadoop fs -put $GM/examples/ingest/json/example.json
    hadoop fs -put $GM/examples/ingest/json/example_multi_line.json
    hadoop fs -put $GM/examples/ingest/xml/example.xml
    hadoop fs -put $GM/examples/ingest/xml/example_multi_line.xml
    hadoop fs -put $GM/examples/ingest/avro/example_no_header.avro
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
    if [[ -d "$GMTMP" ]]; then
      rm "/tmp/${GMTMP}" -rf
    fi
    
    t="/tmp/${GMTMP}"
    mkdir $t
    
    # Export some formats
    $geomesa export -u root -p secret -c ${CATALOG} -f example-csv -F csv  > "$t/e.csv"
    $geomesa export -u root -p secret -c ${CATALOG} -f example-csv -F avro > "$t/e.avro"
    $geomesa export -u root -p secret -c ${CATALOG} -f example-csv -F tsv  > "$t/e.tsv"
    
    # Reingest automatically those formats both locally and via hdfs
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-avro "$t/e.avro"
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-csv  "$t/e.csv"
    $geomesa ingest -u root -p secret -c ${CATALOG} -f re-tsv  "$t/e.tsv"
    
    hadoop fs -put "$t/e.avro"
    hadoop fs -put "$t/e.csv"
    hadoop fs -put "$t/e.tsv"
    
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
}

function test_vis() {
   $geomesa ingest -u root -p secret -c ${CATALOG} -s example-csv -f viscsv  -C example-csv-with-visibilities $GM/examples/ingest/csv/example.csv
   
   # no auths gets no data
   accumulo shell -u root -p secret -e "setauths -u root -s ''"
   res=$($geomesa export -u root -p secret -c ${CATALOG} -f viscsv | wc -l)
   if [[ "${res}" -ne "1" ]]; then
     echo "error vis should be 1"
     exit 1
   fi

   # no auths gets no data
   accumulo shell -u root -p secret -e "setauths -u root -s user"
   res=$($geomesa export -u root -p secret -c ${CATALOG} -f viscsv | wc -l)
   if [[ "${res}" -ne "3" ]]; then
     echo "error vis should be 3"
     exit 2
   fi

   # no auths gets no data
   accumulo shell -u root -p secret -e "setauths -u root -s user,admin"
   res=$($geomesa export -u root -p secret -c ${CATALOG} -f viscsv | wc -l)
   if [[ "${res}" -ne "4" ]]; then
     echo "error vis should be 4"
     exit 3
   fi

}

function test_twitter() {
    # number of expected records to ingest correctly
    expected=97045 && \
    $geomesa ingest -u root -p secret -c ${CATALOG} -s twitter -C twitter-place-centroid ${TWITTER_FILE} && \
    $geomesa ingest -u root -p secret -c ${CATALOG} -s twitter-polygon -C twitter-polygon ${TWITTER_FILE} && \
    c1=$($geomesa export -u root -p secret -c ${CATALOG} -f twitter -a user_id --no-header | wc -l) && \
    c2=$($geomesa export -u root -p secret -c ${CATALOG} -f twitter-polygon -a user_id --no-header | wc -l) && \
    test $c1 -eq $c2 && \
    test $c1 -eq $expected
}


echo -n "setup..." && \
setup && \
echo && \

echo "testing local" && \
test_local && \
echo && \

echo "testing hdfs" && \
test_hdfs && \
echo && \

echo "testing export" && \
test_export && \
echo && \

echo "testing vis" && \
test_vis && \
echo && \

#echo "testing s3" && \
#test_s3 && \
#echo && \

#echo "Testing twitter schema" && \
#test_twitter && \
#echo "PASSED - Twitter tests"

if [[ "$?" == "0" ]]; then
  echo "ALL TESTS PASSED";
else
  echo "FAILURE"
fi
