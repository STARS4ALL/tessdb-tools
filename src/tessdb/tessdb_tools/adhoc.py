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
import logging
import functools
import datetime

# -------------------
# Third party imports
# -------------------

from lica.cli import execute
from lica.csv import write_csv
from lica.sqlite import open_database
from lica.jinja2 import render_from
from lica.validators import vmonth

# --------------
# local imports
# -------------

from ._version import __version__


from .dbutils import (
    get_tessdb_connection_string,
    get_zptess_connection_string,
    group_by_mac,
    common_A_B_items,
    in_A_not_in_B,
)

# ----------------
# Module constants
# ----------------

SQL_ABSURD_ZP_TEMPLATE = "sql-fix-absurd-zp.j2"

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)
package = __name__.split(".")[0]
render = functools.partial(render_from, package)

# -------------------------
# Module auxiliar functions
# -------------------------


def _wrong_zp_photometers_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT mac_address, zp1, valid_since
        FROM tess_t
        WHERE zp1 < 10
        ORDER BY mac_address
        """
    )
    result = [dict(zip(["mac", "zero_point", "valid_since"], row)) for row in cursor]
    return result


def _zp_photometers_from_zptess(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT mac, zero_point
        FROM summary_v
        WHERE zero_point IS NOT NULL
        ORDER BY mac, session desc -- get the latest session first
        """
    )
    result = [dict(zip(["mac", "zero_point"], row)) for row in cursor]
    return result


def _names_from_mac(connection, mac):
    params = {"mac_address": mac}
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE mac_address = :mac_address
        """,
        params,
    )
    result = [
        dict(zip(["name", "valid_since", "valid_until", "valid_state"], row)) for row in cursor
    ]
    return result


def _render_sql(output_dir, items):
    context = {"items": items}
    output = render(SQL_ABSURD_ZP_TEMPLATE, context)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "absurd_zp.sql")
    with open(output_file, "w") as sqlfile:
        sqlfile.write(output)


def _render_IDA_ctrl_files(output_dir, items, start_month):
    os.makedirs(output_dir, exist_ok=True)
    for item in items:
        for name in item["names"]:
            output_file = os.path.join(output_dir, name["name"])
            with open(output_file, "w") as fd:
                if start_month is None:
                    month = datetime.datetime.strptime(name["valid_since"], "%Y-%m-%d %H:%M:%S%z")
                    month = month.strftime("%Y-%m")
                else:
                    month = start_month.strftime("%Y-%m")
                fd.write(month + "\n")


def _report_remaining_ZPs(output_dir: str, remaining_macs: list, tessdb_dict: dict):
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "remaining_absurd_zp.csv")
    flattened_list = [items for mac in remaining_macs for items in tessdb_dict[mac]]
    write_csv(output_file, header=("mac", "zero_point", "valid_since"), sequence=flattened_list)


# ===================
# Module entry points
# ===================


def fix(args):
    log.info(" ====================== FIXING WRONG ZP FOR EASY PHOTOMETERS ======================")
    tessdb = get_tessdb_connection_string()
    log.info("connecting to SQLite database %s", tessdb)
    conn_tessdb, _ = open_database(tessdb)
    zptess = get_zptess_connection_string()
    log.info("connecting to SQLite database %s", zptess)
    conn_zptess, _ = open_database(zptess)
    tessdb_input_list = _wrong_zp_photometers_from_tessdb(conn_tessdb)
    tessdb_dict = group_by_mac(tessdb_input_list)
    zptess_input_list = _zp_photometers_from_zptess(conn_zptess)
    zptess_dict = group_by_mac(zptess_input_list)
    common_mac_keys = common_A_B_items(tessdb_dict, zptess_dict)
    log.info("Generating SQL statements for %d different MACs", len(common_mac_keys))
    items = list()
    for mac in sorted(common_mac_keys):
        item = {}
        item["mac"] = mac
        item["new_zp"] = zptess_dict[mac][0]["zero_point"]
        item["old_zps"] = [v["zero_point"] for v in tessdb_dict[mac]]
        item["names"] = _names_from_mac(conn_tessdb, mac)
        items.append(item)
    _render_sql(args.output_dir, items)
    _render_IDA_ctrl_files(args.output_dir, items, args.start_month)
    remaining_mac_keys = in_A_not_in_B(tessdb_dict, zptess_dict)
    log.info(
        "There are %d MACs with wrong ZPs and missing calibration ZPs", len(remaining_mac_keys)
    )
    _report_remaining_ZPs(args.output_dir, remaining_mac_keys, tessdb_dict)


# ================
# MAIN ENTRY POINT
# ================


def add_args(parser):
    # ------------------------------------------
    # Create second level parsers for 'zptess'
    # ------------------------------------------
    subparser = parser.add_subparsers(dest="command")
    zpt = subparser.add_parser("fix", help="Generate SQL file to fix ZPs < 10.0")
    zpt.add_argument("-o", "--output-dir", type=str, required=True, help="Output SQL File")
    zpt.add_argument("-m", "--start-month", type=vmonth, default=None, help="Output SQL File")


ENTRY_POINT = {
    "fix": fix,
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
        description="Generate SQL to fix photometers with zp < 10.0",
    )
