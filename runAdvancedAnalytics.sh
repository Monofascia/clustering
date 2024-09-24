#!/bin/bash

declare -r SH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
declare -r SPARK_SUBMIT_PATH="/usr/hdp/current/spark2-client/bin/spark-submit"
declare -r py_files="${SH_DIR}/src.zip"

(cd ${SH_DIR}/advanced_analytics && zip -r ${py_files}  src/ -x \*.pyc* \*\_\_pycache__*)

umask 0002

$SPARK_SUBMIT_PATH \
  --properties-file "${SH_DIR}/resources/spark.conf" \
  --files "${SH_DIR}/resources/config.ini" \
  --py-files ${py_files} \
  --keytab "${HOME}/ponlegalita.keytab" \
  --principal ponlegalita@SERVIZI.INPS \
   ${SH_DIR}/advanced_analytics/main.py $@
