#!/bin/bash
set -euo pipefail
AS_HEGEMONY=
python3 "${AS_HEGEMONY}"/produce_bgpatom.py -C <%CONFIG> -c <%COLLECTOR> -s <%START_TS> -e <%STOP_TS2> &&
python3 "${AS_HEGEMONY}"/produce_bcscore.py -C <%CONFIG> -c <%COLLECTOR> -s <%START_TS> -e <%STOP_TS1> &&
python3 "${AS_HEGEMONY}"/produce_hege.py -C <%CONFIG> -c <%COLLECTOR> -s <%START_TS> -e <%STOP_TS1> --sparse_peers

