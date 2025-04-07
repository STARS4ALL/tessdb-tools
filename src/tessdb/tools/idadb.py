# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

import logging

import collections

# -------------------
# Third party imports
# -------------------

from lica.csv import write_csv
from lica.sqlite import open_database

# --------------
# local imports
# -------------


from .utils import formatted_mac
from .dbutils import (
    get_tessdb_connection_string,
    get_idadb_connection_string,
    group_by_mac,
    common_A_B_items,
    in_A_not_in_B,
)


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)


IDADB_COLUMNS = (
    "mac",
    "name",
    "filename",
    "data_rows",
    "computed_zp_median",
    "computed_zp_stdev",
    "tessdb_zp_median",
    "tessdb_zp_stdev",
    "computed_zp_min",
    "computed_zp_max",
    "t0",
    "t1",
)

TESSDB_COLUMNS = (
    "mac",
    "name",
    "name_valid_since",
    "name_valid_until",
    "name_valid_state",
    "tess_id",
    "zero_point",
    "zp_valid_since",
    "zp_valid_until",
    "zp_valid_state",
    "registered",
)


# -------------------------
# Module auxiliar functions
# -------------------------
def _photometers_from_idadb(connection):
    cursor = connection.cursor()
    cursor.execute(
        """                                                    
        SELECT mac, name, filename, data_rows, computed_zp_median, computed_zp_stdev, 
                tessdb_zp_median, tessdb_zp_stdev, computed_zp_min, computed_zp_max, t0,  t1 
        FROM ida_summary_t
        ORDER BY mac, filename
        """
    )
    return cursor.fetchall()


def _photometers_from_tessdb2(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT t.mac_address, n.name, 
        n.valid_since AS name_valid_since, n.valid_until AS name_valid_until, n.valid_state AS name_valid_state, 
        t.tess_id, t.zero_point, t.valid_since AS zp_valid_since, t.valid_until AS zp_valid_until, t.valid_state AS zp_valid_state,
        t.registered AS registered
        FROM tess_t AS t
        JOIN name_to_mac_t as n USING (mac_address)
        WHERE n.name LIKE 'stars%'
        AND t.valid_since BETWEEN n.valid_since AND n.valid_until
        ORDER BY t.mac_address, t.valid_since
        """
    )
    return cursor.fetchall()


def tessdb_remap_info(row):
    new_row = dict()
    try:
        new_row["mac"] = formatted_mac(row[0])
    except Exception:
        return None
    new_row["name"] = row[1]
    new_row["name_valid_since"] = row[2]
    new_row["name_valid_until"] = row[3]
    new_row["name_valid_state"] = row[4]
    new_row["tess_id"] = row[5]
    new_row["zero_point"] = row[6]
    new_row["zp_valid_since"] = row[7]
    new_row["zp_valid_until"] = row[8]
    new_row["zp_valid_state"] = row[9]
    new_row["registered"] = row[10]
    return new_row


def ida_remap_info(row):
    new_row = dict()
    try:
        new_row["mac"] = formatted_mac(row[0])
    except Exception:
        return None
    new_row["name"] = row[1]
    new_row["filename"] = row[2]
    new_row["data_rows"] = row[3]
    new_row["computed_zp_median"] = row[4]
    new_row["computed_zp_stdev"] = row[5]
    new_row["tessdb_zp_median"] = row[6]
    new_row["tessdb_zp_stdev"] = row[7]
    new_row["computed_zp_min"] = row[8]
    new_row["computed_zp_max"] = row[9]
    new_row["t0"] = row[10]
    new_row["t1"] = row[11]
    return new_row


def read_databases():
    tessdb_url = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", tessdb_url)
    conn_tessdb = open_database(tessdb_url)
    idadb_url = get_idadb_connection_string()
    log.info("connecting to SQLite database %s", idadb_url)
    conn_idadb = open_database(idadb_url)
    tessdb_input_list = list(map(tessdb_remap_info, _photometers_from_tessdb2(conn_tessdb)))
    log.info("%d entries from tessdb", len(tessdb_input_list))
    log.info("=========================== TESSDB Grouping by MAC=========================== ")
    tessdb_input_list = group_by_mac(tessdb_input_list)
    log.info("=========================== IDADB Grouping by MAC ==========================")
    idadb_input_list = list(map(ida_remap_info, _photometers_from_idadb(conn_idadb)))
    log.info("%d entries from idadb", len(idadb_input_list))
    idadb_input_list = group_by_mac(idadb_input_list)
    return tessdb_input_list, idadb_input_list


def generate_common(output_path):
    tessdb_input_list, idadb_input_list = read_databases()
    common_macs = common_A_B_items(tessdb_input_list, idadb_input_list)
    log.info("Common MAC entries: %d", len(common_macs))
    output = intra_ida_analisys(common_macs, idadb_input_list)
    write_csv(output, TESSDB_COLUMNS, output_path)


def generate_only_tessdb(output_path):
    tessdb_input_list, idadb_input_list = read_databases()
    only_tessdb_macs = in_A_not_in_B(tessdb_input_list, idadb_input_list)
    log.info("TESSDB MACs only, entries: %d", len(only_tessdb_macs))
    output = {mac: tessdb_input_list[mac] for mac in only_tessdb_macs}
    write_csv(output, TESSDB_COLUMNS, output_path)


def generate_only_idadb(output_path):
    tessdb_input_list, idadb_input_list = read_databases()
    only_ida_file_macs = in_A_not_in_B(idadb_input_list, tessdb_input_list)
    log.info("IDADB MACs only, entries: %d", len(only_ida_file_macs))
    output = {mac: idadb_input_list[mac] for mac in only_ida_file_macs}
    write_csv(output, TESSDB_COLUMNS, output_path)


def all_equal(pair):
    """All coincident entries"""
    result = False
    mac, items = pair
    for item in items:
        if item["computed_zp_median"] != item["tessdb_zp_median"]:
            result = True
            break
    if result is False:
        computed_zp_list = list(map(lambda x: x["computed_zp_median"], items))
        frequencies_comp = collections.Counter(computed_zp_list).most_common()
        tessdb_zp_list = list(map(lambda x: x["tessdb_zp_median"], items))
        frequencies_tdb = collections.Counter(tessdb_zp_list).most_common()
        log.info("[%s] skipping case %s %s", mac, frequencies_comp, frequencies_tdb)
    return result


def plain_wrong_tessdb_zp(pair):
    """ZP from tessdb is 2.0 or 0.0 means regeneration"""
    result = True
    mac, items = pair
    tessdb_zp_list = list(map(lambda x: x["tessdb_zp_median"], items))
    frequencies_tdb = collections.Counter(tessdb_zp_list).most_common()
    if frequencies_tdb[0][0] < 5.0:
        log.info("[%s] skipping IDA regeneration case %s", mac, frequencies_tdb)
        result = False
    return result


def intra_ida_analisys(mac_list, idadb_input_list):
    idadb_input_list = dict(filter(all_equal, idadb_input_list.items()))
    idadb_input_list = dict(filter(plain_wrong_tessdb_zp, idadb_input_list.items()))
    log.info("Common MAC entries after filtering: %d", len(idadb_input_list))
    return idadb_input_list


# ===================
# Module entry points
# ===================


def generate(options):
    log.info(" ====================== CROSS IDA / TESSD FILE comparison ======================")
    if options.common:
        generate_common(options.file)
    elif options.tessdb:
        generate_only_tessdb(options.file)
    else:
        generate_only_idadb(options.file)
