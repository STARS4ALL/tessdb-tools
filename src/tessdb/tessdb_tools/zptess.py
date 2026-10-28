# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

import os
import csv
import logging
import functools

# -------------------
# Third party imports
# -------------------

from lica.cli import execute

# --------------
# local imports
# -------------

from ._version import __version__

from .utils import open_database, formatted_mac
from .dbutils import (
    get_tessdb_connection_string,
    get_zptess_connection_string,
    group_by_mac,
    common_A_B_items,
    in_A_not_in_B,
)


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

COMMON_COLUMNS = (
    "mac",
    "zptess_name",
    "tessdb_state",
    "zptess_zp",
    "tessdb_zp",
    "zptess_method",
    "tessdb_registered",
    "zptess_date",
    "tessdb_date",
    "tessdb_entries",
    "zptess_entries",
)

TESSDB_COLUMNS = (
    "mac",
    "tessdb_state",
    "tessdb_zp",
    "tessdb_registered",
    "tessdb_date",
    "tessdb_entries",
)

ZPTESS_COLUMNS = (
    "mac",
    "zptess_name",
    "zptess_zp",
    "zptess_method",
    "zptess_date",
    "zptess_entries",
)

# -------------------------
# Module auxiliar functions
# -------------------------


def _photometers_from_tessdb1(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT mac_address, valid_state, zp1, valid_since, registered
        FROM tess_t
        WHERE valid_state = 'Current'
        AND mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) = 1)
        """
    )
    return cursor.fetchall()


def _photometers_from_tessdb2(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT mac_address, valid_state, zp1, valid_since, registered
        FROM tess_t
        ORDER BY mac_address, valid_since
        """
    )
    return cursor.fetchall()


def _photometers_from_zptess(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT mac, zero_point, session, name, calibration 
        FROM summary_v 
        WHERE name LIKE 'stars%' AND upd_flag = 1
        ORDER BY mac, session
        """
    )
    return cursor.fetchall()


def tessdb_remap_info(row):
    new_row = dict()
    try:
        new_row["mac"] = formatted_mac(row[0])
    except Exception:
        return None
    new_row["tessdb_state"] = row[1]
    new_row["tessdb_zp"] = row[2]
    new_row["tessdb_date"] = row[3]
    new_row["tessdb_registered"] = row[4]
    return new_row


def zptess_remap_info(row):
    new_row = dict()
    try:
        new_row["mac"] = formatted_mac(row[0])
    except Exception:
        return None
    new_row["zptess_zp"] = row[1]
    new_row["zptess_date"] = row[2]
    new_row["zptess_name"] = row[3]
    new_row["zptess_method"] = row[4]
    return new_row


def complex_filtering(item, mac_list=None):
    if item["mac"] not in mac_list:
        return True
    if item["tessdb_zp"] != item["zptess_zp"]:
        return True
    return False


def generate_common(conn_tessdb, conn_zptess, historic_flag, path):
    zptess_input_list = list(map(zptess_remap_info, _photometers_from_zptess(conn_zptess)))
    if historic_flag:
        tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb2(conn_tessdb)))
    else:
        tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb1(conn_tessdb)))
    log.info("%d entries from tessdb", len(tessdb_input_list))
    log.info("%d entries from zptess", len(zptess_input_list))
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== ZPTESS Grouping by MAC ==========================")
    zptess_input_list = group_by_mac(zptess_input_list)
    common_macs = common_A_B_items(zptess_input_list, tessdb_input_list)
    log.info("Common entries: %d", len(common_macs))
    common_list = list()
    aux_list = list()
    # For each photometer we calculate the cartesian product
    for key in common_macs:
        tdblen = len(tessdb_input_list[key])
        zptlen = len(zptess_input_list[key])
        entries = tdblen * zptlen
        log.debug("Generating %d entries for %s", entries, key)
        aux_list.append(
            {"mac": key, "entries": entries, "tessdb_entries": tdblen, "zptess_entries": zptlen}
        )
        common_list.extend(
            [
                {**x, **y, "tessdb_entries": tdblen, "zptess_entries": zptlen}
                for x in tessdb_input_list[key]
                for y in zptess_input_list[key]
            ]
        )
    simple_case_list = list(map(lambda x: x["mac"], filter(lambda x: x["entries"] == 1, aux_list)))
    simples_out_func = functools.partial(complex_filtering, mac_list=simple_case_list)
    common_list = filter(simples_out_func, common_list)
    common_list = sorted(common_list, key=lambda x: x["mac"])
    aux_list = sorted(aux_list, key=lambda x: x["mac"])
    log.info("Final list of %d common, filtered elements", len(common_list))
    folder = os.path.dirname(path)
    aux_path = os.path.join(folder, "common_mac_entries.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, delimiter=";", fieldnames=COMMON_COLUMNS)
        writer.writeheader()
        for row in common_list:
            writer.writerow(row)
    with open(aux_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f, delimiter=";", fieldnames=["mac", "entries", "tessdb_entries", "zptess_entries"]
        )
        writer.writeheader()
        for row in aux_list:
            writer.writerow(row)


def generate_tessdb(conn_tessdb, conn_zptess, historic_flag, path):
    zptess_input_list = list(map(zptess_remap_info, _photometers_from_zptess(conn_zptess)))
    if historic_flag:
        tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb2(conn_tessdb)))
    else:
        tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb1(conn_tessdb)))
    log.info("%d entries from tessdb", len(tessdb_input_list))
    log.info("%d entries from zptess", len(zptess_input_list))
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== ZPTESS Grouping by MAC ==========================")
    zptess_input_list = group_by_mac(zptess_input_list)
    only_tessdb_macs = in_A_not_in_B(tessdb_input_list, zptess_input_list)
    log.info("TESSDB MACs only, entries: %d", len(only_tessdb_macs))
    only_tessdb_list = list()
    # For each photometer we calculate the cartesian product
    for key in only_tessdb_macs:
        tdblen = len(tessdb_input_list[key])
        only_tessdb_list.extend([{**x, "tessdb_entries": tdblen} for x in tessdb_input_list[key]])
    only_tessdb_list = sorted(only_tessdb_list, key=lambda x: x["mac"])
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, delimiter=";", fieldnames=TESSDB_COLUMNS)
        writer.writeheader()
        for row in only_tessdb_list:
            writer.writerow(row)


def generate_zptess(conn_tessdb, conn_zptess, historic_flag, path):
    zptess_input_list = list(map(zptess_remap_info, _photometers_from_zptess(conn_zptess)))
    if historic_flag:
        tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb2(conn_tessdb)))
    else:
        tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb1(conn_tessdb)))
    log.info("%d entries from tessdb", len(tessdb_input_list))
    log.info("%d entries from zptess", len(zptess_input_list))
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== ZPTESS Grouping by MAC ==========================")
    zptess_input_list = group_by_mac(zptess_input_list)
    only_zptess_macs = in_A_not_in_B(zptess_input_list, tessdb_input_list)
    log.info("ZPTESS MACs only, entries: %d", len(only_zptess_macs))
    only_zptess_list = list()
    # For each photometer we calculate the cartesian product
    for key in only_zptess_macs:
        zptlen = len(zptess_input_list[key])
        only_zptess_list.extend([{**x, "zptess_entries": zptlen} for x in zptess_input_list[key]])
    only_zptess_list = sorted(only_zptess_list, key=lambda x: x["mac"])
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, delimiter=";", fieldnames=ZPTESS_COLUMNS)
        writer.writeheader()
        for row in only_zptess_list:
            writer.writerow(row)


# ===================
# Module entry points
# ===================


def generate(options):
    log.info(
        " ====================== ANALIZING DUPLICATES IN TESSDB METADATA ======================"
    )
    tessdb = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", tessdb)
    conn_tessdb = open_database(tessdb)
    zptess = get_zptess_connection_string()
    log.info("connecting to SQLite database %s", zptess)
    conn_zptess = open_database(zptess)
    if options.common:
        generate_common(conn_tessdb, conn_zptess, options.historic, options.file)
    elif options.tessdb:
        generate_tessdb(conn_tessdb, conn_zptess, options.historic, options.file)
    else:
        generate_zptess(conn_tessdb, conn_zptess, options.historic, options.file)


# ================
# MAIN ENTRY POINT
# ================


def add_args(parser):
    # ------------------------------------------
    # Create second level parsers for 'zptess'
    # ------------------------------------------

    subparser = parser.add_subparsers(dest="command")

    zpt = subparser.add_parser("generate", help="Generate cross zptess/tessdb CSV comparison")
    zpt.add_argument("-f", "--file", type=str, required=True, help="Output CSV File")
    zpex1 = zpt.add_mutually_exclusive_group(required=True)
    zpex1.add_argument("--common", action="store_true", help="Common MACs")
    zpex1.add_argument("--zptess", action="store_true", help="MACs in ZPTESS not in TESSDB")
    zpex1.add_argument("--tessdb", action="store_true", help="MACs in TESSDB not in ZPTESS")

    zpex1 = zpt.add_mutually_exclusive_group(required=True)
    zpex1.add_argument("-c", "--current", action="store_true", help="Current ZP")
    zpex1.add_argument("-i", "--historic", action="store_true", help="Historic ZP entries")


ENTRY_POINT = {
    "generate": generate,
}


def zp_tess(args):
    func = ENTRY_POINT[args.command]
    func(args)


def main():
    execute(
        main_func=zp_tess,
        add_args_func=add_args,
        name=__name__,
        version=__version__,
        description="STARS4ALL MongoDB Utilities",
    )
