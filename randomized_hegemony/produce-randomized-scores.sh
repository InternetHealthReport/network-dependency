#!/bin/bash
set -euo pipefail
if [ ! $# -eq 5 ]
then
    echo "usage: $0 config timestamp sampling_percentage iterations (-a | -p)"
    exit 1
fi
AS_HEGEMONY=
CONFIG_TEMPLATE="${AS_HEGEMONY}/template-config-randomized.json"
SCRIPT_TEMPLATE="produce-template.sh"
WORKING_DIR="${PWD}/tmp"
QUEUE="${WORKING_DIR}/queue.parallel"

CONFIG="${1}"
TIMESTAMP="${2}"
SAMPLING_PERCENTAGE="${3}"
ITERATIONS="${4}"
MODE="${5}"

# Hack to get the collector name from the config. xargs is used to trim possible whitespace...
COLLECTOR_LIST=$(grep -e "collector.*=.*" "${CONFIG}" | cut -d'=' -f 2 | xargs).collectors.csv
START_TS=$(date --utc +%Y-%m-%dT%H:%M:00 --date="${TIMESTAMP}")
STOP_TS1=$(date --utc +%Y-%m-%dT%H:%M:01 --date="${TIMESTAMP}")
STOP_TS2=$(date --utc +%Y-%m-%dT%H:%M:02 --date="${TIMESTAMP}")


mkdir -p "${WORKING_DIR}"

python3 generate_topics.py "${CONFIG}" "${TIMESTAMP}" "${SAMPLING_PERCENTAGE}" "${ITERATIONS}" "${MODE}" -o "${WORKING_DIR}"

[ -f "${QUEUE}" ] && rm "${QUEUE}"
while read -r COLLECTOR
do
    COLLECTOR_CONFIG="${WORKING_DIR}/${COLLECTOR}.json"
    COLLECTOR_SCRIPT="${WORKING_DIR}/${COLLECTOR}.sh"
    sed "s#<%COLLECTOR>#${COLLECTOR}#" "${CONFIG_TEMPLATE}" > "${COLLECTOR_CONFIG}"
    sed "s#<%COLLECTOR>#${COLLECTOR}#;s#<%CONFIG>#${COLLECTOR_CONFIG}#;s#<%START_TS>#${START_TS}#;s#<%STOP_TS1>#${STOP_TS1}#;s#<%STOP_TS2>#${STOP_TS2}#" "${SCRIPT_TEMPLATE}" > "${COLLECTOR_SCRIPT}"
    chmod +x "${COLLECTOR_SCRIPT}"
    echo "${COLLECTOR_SCRIPT}" >> "${QUEUE}"
done < "${WORKING_DIR}/${COLLECTOR_LIST}"
exit


