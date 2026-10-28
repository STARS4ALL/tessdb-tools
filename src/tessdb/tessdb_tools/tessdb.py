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
# -------------------
# Third party imports
# -------------------
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

from lica.cli import execute
from lica.validators import vdir, vmac
from lica.jinja2 import render_from
from lica.sqlite import open_database
from lica.csv import write_csv

# --------------
# local imports
# -------------

from ._version import __version__

from .utils import formatted_mac
from .dbutils import (
    group_by_coordinates,
    group_by_mac,
    group_by_name,
    log_coordinates,
    log_coordinates_nearby,
    group_by_place,
    log_places,
)

# ----------------
# Module constants
# ----------------

SQL_PHOT_UPD_MAC_ADDRESS = "sql-phot-upd-mac.j2"
SQL_PHOT_UPD_READINGS_LOCATIONS = "sql-phot-upd-readings-locations.j2"


PHOTOMETER_TYPE = ("easy", "repaired", "renamed", "complicated")

HEADER_NAME = (
    "name",
    "mac",
    "valid_since",
    "valid_until",
    "contiguous_flag",
    "valid_state",
    "valid_days",
)
HEADER_MAC = (
    "mac",
    "name",
    "valid_since",
    "valid_until",
    "contiguous_flag",
    "valid_state",
    "valid_days",
)

# -----------------------
# Module global variables
# -----------------------

package = __name__.split(".")[0]
log = logging.getLogger(__name__)
geolocator = Nominatim(user_agent="STARS4ALL project")
tf = TimezoneFinder()

# -------------------------
# Module auxiliar functions
# -------------------------

# ================================ BEGIN GOOD REUSABLE FUNCTIONS ============================

render = functools.partial(render_from, package)


def get_as_list(field, phot_dict):
    def _collect(rows):
        return sorted(set(row[field] for row in rows))

    result = dict(zip(phot_dict.keys(), map(_collect, phot_dict.values())))
    return result


def filter_current_name(row):
    return row["valid_state"] == "Current"


def filter_current_phot(row):
    return row["phot_valid_state"] == "Current"


def filter_current_name_and_phot(row):
    return row["valid_state"] == "Current" and row["phot_valid_state"] == "Current"


def coordinates_from_location_id(connection, location_id):
    params = {"location_id": location_id}
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT longitude, latitude FROM location_t WHERE location_id = :location_id
        """,
        params,
    )
    return cursor.fetchone()


def photometers_with_locations(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    result = list()
    cursor = connection.cursor()
    for row in name_mac_list:
        params = {"name": row["name"], "mac": row["mac"], "valid_state": row["valid_state"]}
        cursor.execute(
            """
            SELECT  :name, mac_address, :valid_state, tess_id, valid_state, model, firmware, 
            nchannels, zp1, filter1, zp2, filter2, zp3, filter3, zp4, filter4,
            cover_offset, fov, azimuth, altitude,
            longitude, latitude, place, town, sub_region, country, timezone,
            contact_name, contact_email, organization -- This should be removed at some point
            FROM tess_t AS t
            JOIN location_t USING(location_id)
            WHERE mac_address = :mac 
            """
        )
        temp = [
            dict(
                zip(
                    [
                        "name",
                        "mac",
                        "valid_state",
                        "tess_id",
                        "phot_valid_state",
                        "model",
                        "firmware",
                        "nchannels",
                        "zp1",
                        "filter1",
                        "zp2",
                        "filter2",
                        "zp3",
                        "filter3",
                        "zp4",
                        "filter4",
                        "cover_offset",
                        "fov",
                        "azimuth",
                        "altitude",
                        "longitude",
                        "latitude",
                        "place",
                        "town",
                        "sub_region",
                        "country",
                        "timezone",
                        "contact_name",
                        "contact_email",
                        "organization",
                    ],
                    row,
                )
            )
            for row in cursor
        ]
        result.extend(temp)
    return result


# This takes so much time that we converted it to a generator
def readings_unknown_location(connection, name_mac_list, known_flag, threshold=0):
    cursor = connection.cursor()
    for row in name_mac_list:
        params = {"name": row["name"], "mac": row["mac"], "threshold": threshold}
        if known_flag:
            cursor.execute(
                """
                SELECT :name, mac_address, tess_id, t.location_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.location_id = -1
                AND t.location_id > -1 -- known location_id in tess_t !
                GROUP BY tess_id
                HAVING cnt > :threshold
                """,
                params,
            )
        else:
            cursor.execute(
                """
                SELECT :name, mac_address, tess_id, t.location_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.location_id = -1
                GROUP BY tess_id
                HAVING cnt > :threshold
                """,
                params,
            )
        result = [
            dict(zip(["name", "mac", "tess_id", "location_id", "count"], row)) for row in cursor
        ]
        for row in result:
            log.info("Unknown location in readings for %s", row)
        yield result


# This takes so much time that we converted it to a generator
def readings_unknown_observer(connection, name_mac_list, known_flag, threshold=0):
    cursor = connection.cursor()
    for row in name_mac_list:
        params = {
            "name": row["name"],
            "mac": row["mac"],
        }
        if known_flag:
            cursor.execute(
                """
                SELECT :name, mac_address, tess_id, t.observer_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.observer_id = -1
                AND t.observer_id > -1 -- known observer_id in tess_t !
                GROUP BY tess_id
                HAVING cnt > :threshold
                """,
                params,
            )
        else:
            cursor.execute(
                """
                SELECT :name, mac_address, tess_id, t.observer_id, COUNT(tess_id) as cnt
                FROM tess_readings_t AS r
                JOIN tess_t AS t USING (tess_id)
                WHERE mac_address = :mac
                AND r.observer_id = -1
                GROUP BY tess_id
                HAVING cnt > :threshold
                """,
                params,
            )
        result = [
            dict(zip(["name", "mac", "tess_id", "observer_id", "count"], row)) for row in cursor
        ]
        for row in result:
            log.info("Unknown observer in readings for %s", row)
        yield result


def photometers_fake_zero_points(connection, name_mac_list, threshold=18.5):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {
            "name": row["name"],
            "mac": row["mac"],
            "valid_state": row["valid_state"],
            "zp": threshold,
        }
        cursor.execute(
            """
            SELECT :name, mac_address, :valid_state, tess_id, zp1, valid_state
            FROM tess_t
            WHERE mac_address = :mac
            AND zp1 < :zp
            """,
            params,
        )
        result.extend(
            [
                dict(zip(["name", "mac", "valid_state", "tess_id", "zp1", "phot_valid_state"], row))
                for row in cursor
            ]
        )
    return result


def photometers_location_id(connection, name_mac_list, location_id):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {
            "name": row["name"],
            "mac": row["mac"],
            "valid_state": row["valid_state"],
            "location_id": location_id,
        }
        cursor.execute(
            """
            SELECT :name, mac_address, :valid_state, tess_id, location_id, valid_state 
            FROM tess_t
            WHERE mac_address = :mac
            AND location_id = :location_id
            """,
            params,
        )
        temp = [
            dict(
                zip(
                    ["name", "mac", "valid_state", "tess_id", "location_id", "phot_valid_state"],
                    row,
                )
            )
            for row in cursor
        ]
        result.extend(temp)
    return result


def photometers_observer_id(connection, name_mac_list, observer_id):
    cursor = connection.cursor()
    result = list()
    for row in name_mac_list:
        params = {
            "name": row["name"],
            "mac": row["mac"],
            "valid_state": row["valid_state"],
            "observer_id": observer_id,
        }
        cursor.execute(
            """
            SELECT :name, mac_address, :valid_state, tess_id, observer_id, valid_state 
            FROM tess_t
            WHERE mac_address = :mac
            AND observer_id = :observer_id
            """,
            params,
        )
        temp = [
            dict(
                zip(
                    ["name", "mac", "valid_state", "tess_id", "observer_id", "phot_valid_state"],
                    row,
                )
            )
            for row in cursor
        ]
        result.extend(temp)
    return result


def name_mac_current_history_sql(name):
    if name is not None:
        sql = """
            SELECT name, mac_address, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE name = :name
            ORDER BY valid_since
        """
    else:
        sql = """
            SELECT mac_address, name, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE mac_address = :mac
            ORDER BY valid_since
        """
    return sql


def name_mac_previous_related_history_sql(name):
    if name is not None:
        sql = """
            SELECT name, mac_address, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_until = :tstamp
            ORDER BY valid_since
        """
    else:
        sql = """
            SELECT mac_address, name, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_until = :tstamp
            ORDER BY valid_since
        """
    return sql


def name_mac_next_related_history_sql(name):
    if name is not None:
        sql = """
            SELECT name, mac_address, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_since = :tstamp
            ORDER BY valid_since
        """
    else:
        sql = """
            SELECT mac_address, name, valid_since, valid_until, '+', valid_state, julianday(valid_until) - julianday(valid_since)
            FROM name_to_mac_t
            WHERE valid_since = :tstamp
            ORDER BY valid_since
        """
    return sql


def name_mac_previous_related_history(connection, start_tstamp, name, mac):
    cursor = connection.cursor()
    history = list()
    params = {"tstamp": start_tstamp, "name": name, "mac": mac}
    sql = name_mac_previous_related_history_sql(name)
    complicated = False
    while True:
        cursor.execute(sql, params)
        fragment = cursor.fetchall()
        L = len(fragment)
        if L == 0:
            break
        elif L > 1:
            complicated = True
            history.extend(fragment)
            log.warn(
                "Really complicated previous related history with %d heads for name=%s mac=%s",
                L,
                name,
                mac,
            )
            break
        else:
            history.extend(fragment)
            tstamp = fragment[0][2]  # begin timestamp
            params = {"tstamp": tstamp}
    history.reverse()
    history = [list(item) for item in history]
    return history, complicated


def name_mac_next_related_history(connection, end_tstamp, name, mac):
    cursor = connection.cursor()
    history = list()
    params = {"tstamp": end_tstamp, "name": name, "mac": mac}
    sql = name_mac_next_related_history_sql(name)
    complicated = False
    while True:
        cursor.execute(sql, params)
        fragment = cursor.fetchall()
        L = len(fragment)
        if L == 0:
            break
        elif L > 1:
            history.extend(fragment)
            complicated = True
            log.warn(
                "Really complicated next related history with %d heads for name=%s mac=%s",
                L,
                name,
                mac,
            )
            break
        else:
            history.extend(fragment)
            tstamp = fragment[0][3]  # end timestamp
            params = {"tstamp": tstamp}
    history = [list(item) for item in history]
    return history, complicated


def name_mac_current_history(connection, name, mac):
    assert name is not None or mac is not None, f"either name={name} or mac={mac} is None"
    params = {"name": name, "mac": mac}
    cursor = connection.cursor()
    sql = name_mac_current_history_sql(name)
    cursor.execute(sql, params)
    history = [list(item) for item in cursor.fetchall()]
    break_end_tstamps = list()
    break_start_tstamps = list()
    for i in range(len(history) - 1):
        if history[i][3] != history[i + 1][2]:
            history[i][4] = "-"
            break_end_tstamps.append(history[i][3])
            break_start_tstamps.append(history[i + 1][2])
    truncated = history[-1][5] == "Expired"
    return history, break_end_tstamps, break_start_tstamps, truncated


def photometer_classification(args):
    if args.easy:
        return "easy"
    if args.renamed:
        return "renamed"
    if args.repaired:
        return "repaired"
    return "complicated"


def selected_name_mac_list(connection, classification):
    if classification == "easy":
        result = photometers_easy(connection)
    elif classification == "renamed":
        result = photometers_renamed(connection)
    elif classification == "repaired":
        result = photometers_repaired(connection)
    else:
        result = photometers_complicated(connection)
    return result


def photometers_easy(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name, mac_address, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE mac_address IN (
            -- Photometers with no repairs and no renamings
            SELECT mac_address  FROM name_to_mac_t
            WHERE name LIKE 'stars%'
            EXCEPT -- this is the photometer substitution/repair part
            SELECT mac_address FROM name_to_mac_t
            WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
            EXCEPT -- This is the renamings part
            SELECT mac_address FROM name_to_mac_t
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
        ORDER BY name, valid_since
    """
    )
    return [
        dict(zip(["name", "mac", "valid_since", "valid_until", "valid_state"], row))
        for row in cursor
    ]


def photometers_not_easy(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name, mac_address, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE mac_address IN (
            -- Photometers with with substitution
            SELECT mac_address FROM name_to_mac_t
            WHERE name IN (SELECT name FROM name_to_mac_t GROUP BY name HAVING COUNT(name) > 1)
            UNION -- This is the renamings part
            SELECT mac_address FROM name_to_mac_t
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t GROUP BY mac_address HAVING COUNT(mac_address) > 1))
        ORDER BY name, valid_since
    """
    )
    return [
        dict(zip(["name", "mac", "valid_since", "valid_until", "valid_state"], row))
        for row in cursor
    ]


def photometers_repaired(connection):
    output = list()
    for row in photometers_not_easy(connection):
        name = row["name"]
        history, break_end_tstamps, break_start_tstamps, truncated = name_mac_current_history(
            connection, name, mac=None
        )
        start_tstamp = history[0][2]
        end_tstamp = history[-1][3]
        prev_history, _ = name_mac_previous_related_history(
            connection, start_tstamp, name, mac=None
        )
        next_history, _ = name_mac_next_related_history(connection, end_tstamp, name, mac=None)
        pure_repair = (
            len(history) > 1
            and len(break_end_tstamps) == 0
            and len(prev_history) == 0
            and len(next_history) == 0
        )
        if pure_repair:
            output.append(row)
    return output


def photometers_renamed(connection):
    output = list()
    for row in photometers_not_easy(connection):
        mac = row["mac"]
        history, break_end_tstamps, break_start_tstamps, truncated = name_mac_current_history(
            connection, name=None, mac=mac
        )
        start_tstamp = history[0][2]
        end_tstamp = history[-1][3]
        prev_history, _ = name_mac_previous_related_history(
            connection, start_tstamp, name=None, mac=mac
        )
        next_history, _ = name_mac_next_related_history(connection, end_tstamp, name=None, mac=mac)
        pure_renaming = (
            len(history) > 1
            and len(break_end_tstamps) == 0
            and len(prev_history) == 0
            and len(next_history) == 0
        )
        if pure_renaming:
            output.append(row)
    return output


def photometers_complicated(connection):
    total = photometers_not_easy(connection)
    only_repaired = photometers_repaired(connection)
    only_renamed = photometers_renamed(connection)
    keys = total[0].keys()
    total = set(list(zip(*item.items()))[1] for item in total)
    only_repaired = set(list(zip(*item.items()))[1] for item in only_repaired)
    only_renamed = set(list(zip(*item.items()))[1] for item in only_renamed)
    total = list(total - only_repaired - only_renamed)
    output = [dict(zip(keys, item)) for item in total]
    return output


def photometers_with_unknown_location(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    return photometers_location_id(connection, name_mac_list, location_id=-1)


def photometers_with_unknown_observer(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    return photometers_observer_id(connection, name_mac_list, observer_id=-1)


def names(connection, mac):
    cursor = connection.cursor()
    params = {"mac": mac}
    cursor.execute(
        """
        SELECT name, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE mac_address = :mac
        ORDER BY valid_since
        """,
        params,
    )
    return [dict(zip(["name", "valid_since", "valid_until", "valid_state"], row)) for row in cursor]


def mac_addresses(connection, name):
    cursor = connection.cursor()
    params = {"name": name}
    cursor.execute(
        """
        SELECT mac_address, valid_since, valid_until, valid_state
        FROM name_to_mac_t
        WHERE name = :name
        ORDER BY valid_since
        """,
        params,
    )
    return [dict(zip(["mac", "valid_since", "valid_until", "valid_state"], row)) for row in cursor]


# we need 'name' for instead of 'location_id', because we use 'group_by_name()' later on
def places(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT longitude, latitude, place, town, sub_region, region, country, timezone, location_id
        FROM location_t 
        """
    )
    return [
        dict(
            zip(
                [
                    "longitude",
                    "latitude",
                    "place",
                    "town",
                    "sub_region",
                    "region",
                    "country",
                    "timezone",
                    "name",
                ],
                row,
            )
        )
        for row in cursor
    ]


def referenced_photometers(connection, location_id):
    params = {"location_id": location_id}
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) FROM tess_t WHERE location_id = :location_id
        """,
        params,
    )
    return cursor.fetchone()[0]


def referenced_readings(connection, location_id):
    params = {"location_id": location_id}
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) FROM tess_readings_t WHERE location_id = :location_id
        """,
        params,
    )
    return cursor.fetchone()[0]


# ================================ END GOOD REUSABLE FUNCTIONS ===============================

##############################################################################################
################################### BEGIN KAKITA #############################################
##############################################################################################


def _photometers_and_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT tess_id, name, mac_address, model, firmware, 
        nchannels, zp1, filter1, zp2, filter2, zp3, filter3, zp4, filter4,
        cover_offset, fov, azimuth, altitude,
        longitude, latitude, place, town, sub_region, country, timezone,
        contact_name, contact_email, organization -- This should be removed at some point
        FROM tess_t AS t
        JOIN location_t USING(location_id)
        JOIN name_to_mac_t AS n USING (mac_address) 
        WHERE n.valid_state = 'Current'
        AND t.valid_state = 'Current'
        AND name LIKE 'stars%'
        """
    )
    return cursor


def tessdb_remap_info(row):
    new_row = dict()
    try:
        new_row["mac"] = formatted_mac(row[1])
    except ValueError:
        return None
    new_row["mac"] = formatted_mac(row[1])
    new_row["zero_point"] = row[2]
    new_row["filter"] = row[3]
    new_row["name"] = row[0]
    return new_row


def tessdb_remap_all_info(row):
    new_row = dict()
    try:
        new_row["mac"] = formatted_mac(row[1])
    except ValueError:
        return None
    try:
        new_row["longitude"] = float(row[4]) if row[4] is not None else 0.0
    except ValueError:
        new_row["longitude"] = 0.0
    try:
        new_row["latitude"] = float(row[5]) if row[5] is not None else 0.0
    except ValueError:
        new_row["latitude"] = 0.0
    new_row["name"] = row[0]
    new_row["place"] = row[6]
    new_row["town"] = row[7]
    new_row["sub_region"] = row[8]
    new_row["region"] = None
    new_row["country"] = row[9]
    new_row["timezone"] = row[10]
    new_row["contact_name"] = row[11]
    new_row["contact_email"] = row[12]
    new_row["org_name"] = row[13]
    new_row["org_email"] = None
    new_row["org_descr"] = None
    new_row["org_web"] = None
    new_row["org_logo"] = None
    return new_row


def photometers_from_tessdb(connection):
    return list(map(tessdb_remap_info, _photometers_from_tessdb(connection)))


def photometers_and_locations_from_tessdb(connection):
    return list(map(tessdb_remap_all_info, _photometers_and_locations_from_tessdb(connection)))


##############################################################################################
################################### END   KAKITA #############################################
##############################################################################################

# ========================
# PHOTOMETER 'fix' COMMAND
# ========================


def fix(args):
    log.info(
        " ====================== GENERATE SQL FILES TO FIX TESSDB METADATA ======================"
    )
    classification = photometer_classification(args)
    connection, path = open_database(None, "TESSDB_URL")
    log.info("Connecting to SQLite database %s", path)
    name_mac_list = selected_name_mac_list(connection, classification)
    if args.unknown_location:
        generator = readings_unknown_location
    elif args.unknown_observer:
        generator = readings_unknown_observer
    else:
        log.error("No valid input option to command 'fix'")
    for items in generator(connection, name_mac_list, known_flag=True, threshold=args.threshold):
        for i, row in enumerate(items, start=1):
            context = {"row": row}
            output = render(SQL_PHOT_UPD_READINGS_LOCATIONS, context)
            output_path = os.path.join(
                args.directory, f"{row['name']}_{i:03d}_upd_{generator.__name__}.sql"
            )
            log.info(
                "Photometer '%s', %s (%s): generating SQL file '%s'",
                classification,
                row["mac"],
                row["name"],
                output_path,
            )
            with open(output_path, "w") as sqlfile:
                sqlfile.write(output)


# =============================
# PHOTOMETER 'readings' COMMAND
# =============================


def readings(args):
    log.info(
        "====================== CHECKING PHOTOMETERS METADATA IN TESSDB ======================"
    )
    classification = photometer_classification(args)
    connection, path = open_database(None, "TESSDB_URL")
    log.info("Connecting to SQLite database %s", path)
    name_mac_list = selected_name_mac_list(connection, classification)
    result = list()
    if args.unknown_location:
        generator = readings_unknown_location
    elif args.unknown_observer:
        generator = readings_unknown_observer
    else:
        log.error("No valid input option to command 'fix'")
    for items in generator(connection, name_mac_list, args.known):
        if items:
            result.extend(items)
    log.info("Detected %d items to update", len(result))


# ==========================
# PHOTOMETER 'check' COMMAND
# ==========================


def check(args):
    log.info(
        "====================== CHECKING PHOTOMETERS METADATA IN TESSDB ======================"
    )
    classification = photometer_classification(args)
    connection, path = open_database(None, "TESSDB_URL")
    log.info("Connecting to SQLite database %s", path)
    if args.places:
        log.info("Check for same place, different coordinates")
        tessdb_places = group_by_place(places(connection))
        log_places(tessdb_places)
    elif args.coords:
        log.info("Check for same coordinates, different places")
        tessdb_coords = group_by_coordinates(places(connection))
        log_coordinates(tessdb_coords)
    elif args.dupl:
        log.info("Check for same coordinates, duplicated places")
        tessdb_coords = group_by_coordinates(places(connection))
        log_duplicated_coords(connection, tessdb_coords)
        log_detailed_impact(connection, tessdb_coords)
    elif args.nearby:
        log.info("Check for nearby places in radius %0.0f meters", args.nearby)
        tessdb_coords = group_by_coordinates(places(connection))
        log_coordinates_nearby(tessdb_coords, args.nearby)
    elif args.macs:
        log.info("Check for proper MAC addresses in tess_t")
        check_proper_macs(connection, classification)
    elif args.fake_zero_points:
        log.info("Check for fake Zero Points in tess_t")
        check_fake_zero_points(connection, classification)
    elif args.unknown_location:
        log.info("Check for Unknown Location in tess_t")
        check_photometers_with_unknown_location(connection, classification, args.output_file)
    elif args.unknown_observer:
        log.info("Check for Unknown Observer in tess_t")
        check_photometers_with_unknown_observer(connection, classification, args.output_file)
    else:
        log.error("No valid input option to command 'check'")


def check_photometers_with_unknown_location(connection, classification, optional_csv_path):
    result = photometers_with_unknown_location(connection, classification)
    result = list(filter(filter_current_phot, result))
    if optional_csv_path and result:
        with open(optional_csv_path, "w") as csvfile:
            fieldnames = result[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in result:
                writer.writerow(row)
    log.info(
        "Must update location in %d %s photometers (%d entries)",
        len(group_by_mac(result)),
        classification,
        len(result),
    )


def check_photometers_with_unknown_observer(connection, classification, optional_csv_path):
    result = photometers_with_unknown_observer(connection, classification)
    result = list(filter(filter_current_phot, result))
    if optional_csv_path and result:
        with open(optional_csv_path, "w") as csvfile:
            fieldnames = result[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in result:
                writer.writerow(row)
    log.info(
        "Must update observer in %d %s photometers (%d entries)",
        len(group_by_mac(result)),
        classification,
        len(result),
    )


def check_fake_zero_points(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    for row in photometers_fake_zero_points(connection, name_mac_list):
        log.info(row)


def check_proper_macs(connection, classification):
    name_mac_list = selected_name_mac_list(connection, classification)
    bad_macs = list()
    for row in name_mac_list:
        try:
            mac = vmac(row["mac"])
        except Exception:
            bad_macs.append(row["mac"])
            log.warn("%s has a bad mac address => %s", row["name"], row["mac"])
    log.info("%d Bad MAC addresses ", len(bad_macs))


def log_duplicated_coords(connection, coords_iterable):
    for coords, rows in coords_iterable.items():
        if None in coords:
            log.error("entry %s with no coordinates: %s", rows[0]["name"], coords)
        if len(rows) > 1 and all(row["place"] == rows[0]["place"] for row in rows):
            log.error(
                "Coordinates %s has duplicated place names: %s for %s",
                coords,
                [row["place"] for row in rows],
                [row["name"] for row in rows],
            )


def log_detailed_impact(connection, coords_iterable):
    for coords, rows in coords_iterable.items():
        if None in coords:
            continue
        if len(rows) == 1:
            continue
        for row in rows:
            count1 = referenced_photometers(connection, row["name"])
            count2 = referenced_readings(connection, row["name"])
            if count1 == 0 and count2 == 0:
                print("DELETE FROM location_t WHERE location_id = %d;" % row["name"])
            elif count1 != 0 and count2 != 0:
                log.info(
                    "[%d] (%s) Ojito con esta location que tiene %d referencias en tess_t y %d en tess_readings_t",
                    row["name"],
                    row["place"],
                    count1,
                    count2,
                )
            elif count1 != 0:
                log.info(
                    "[%d] (%s) Ojito con esta location que tiene %d referencias en tess_t",
                    row["name"],
                    row["place"],
                    count1,
                )
            elif count2 != 0:
                log.info(
                    "[%d] (%s) Ojito con esta location que tiene %d referencias en tess_readings_t",
                    row["name"],
                    row["place"],
                    count2,
                )


# ===============================
# PHOTOMETER 'photometer' COMMAND
# ===============================


def photometers(args):
    log.info(" ====================== ANALIZING TESSDB LOCATION METADATA ======================")
    connection, path = open_database(None, "TESSDB_URL")
    log.info("Connecting to SQLite database %s", path)
    to_console = args.output_file is None
    if args.repaired:
        output = photometers_repaired(connection)
        output_grp = group_by_name(output)
        if to_console:
            for name, values in output_grp.items():
                log.info("%s => %d entries", name, len(values))
        log.info("Got %d photometers repaired entries", len(output))
        HEADER = ("name", "mac", "valid_since", "valid_until", "valid_state")
    elif args.renamed:
        output = photometers_renamed(connection)
        output_grp = group_by_mac(output)
        if to_console:
            for name, values in output_grp.items():
                log.info("%s => %d entries", name, len(values))
        log.info("Got %d photometers renamed entries", len(output))
        HEADER = ("mac", "name", "valid_since", "valid_until", "valid_state")
    elif args.easy:
        output = photometers_easy(connection)
        if to_console:
            for item in output:
                log.info(item)
        log.info("Got %d 'easy' photometers (not repaired, nor renamed entries)", len(output))
        HEADER = ("name", "mac", "valid_since", "valid_until", "valid_state")
    elif args.complicated:
        output = photometers_complicated(connection)
        if to_console:
            for item in output:
                log.info(item)
        log.info(
            "Got %d really 'complicated' photometers entries (with repairs and renaming entries)",
            len(output),
        )
        HEADER = ("name", "mac", "valid_since", "valid_until", "valid_state")
    else:
        raise ValueError("Unknown option")
    if args.output_file:
        write_csv(args.output_file, HEADER, output)


# ============================
# PHOTOMETER 'history' COMMAND
# ============================


def history(args):
    assert args.name is None or args.mac is None, "Either name or mac addresss should be None"
    name = args.name
    mac = args.mac
    header = HEADER_NAME if name is not None else HEADER_MAC
    global_history = list()
    global_history.append(header)
    connection, path = open_database(None, "TESSDB_URL")
    history, break_end_tstamps, break_start_tstamps, truncated = name_mac_current_history(
        connection, name, mac
    )
    start_tstamp = history[0][2]
    end_tstamp = history[-1][3]
    prev_history, _ = name_mac_previous_related_history(connection, start_tstamp, name, mac)
    next_history, _ = name_mac_next_related_history(connection, end_tstamp, name, mac)
    global_history.append(
        ("xxxx", "xxxx", "valid_since", "valid_until", "prev_related", "valid_state", "valid_days")
    )
    global_history.extend(prev_history)
    global_history.append(
        ("xxxx", "xxxx", "valid_since", "valid_until", "current", "valid_state", "valid_days")
    )
    global_history.extend(history)
    global_history.append(
        ("xxxx", "xxxx", "valid_since", "valid_until", "next_related", "valid_state", "valid_days")
    )
    global_history.extend(next_history)
    for break_tstamp in break_end_tstamps:
        broken_end_history, _ = name_mac_next_related_history(connection, break_tstamp, name, mac)
        global_history.append(
            (
                "xxxx",
                "xxxx",
                "valid_since",
                "valid_until",
                "broken_end",
                "valid_state",
                "valid_days",
            )
        )
        global_history.extend(broken_end_history)
    for break_tstamp in break_start_tstamps:
        broken_start_history, _ = name_mac_next_related_history(connection, break_tstamp, name, mac)
        global_history.append(
            (
                "xxxx",
                "xxxx",
                "valid_since",
                "valid_until",
                "broken_start",
                "valid_state",
                "valid_days",
            )
        )
        global_history.extend(broken_start_history)
    if args.output_file:
        log.info("%d rows of previous related history", len(prev_history))
        log.info("%d rows of proper history", len(history))
        log.info("Proper history breaks in %d end timestamp points", len(break_end_tstamps))
        log.info("Proper history breaks in %d start timestamp points", len(break_start_tstamps))
        log.info("%d rows of next related history", len(next_history))
        with open(args.output_file, "w") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            for row in global_history:
                writer.writerow(row)
    else:
        tag = "" if prev_history else "NO"
        log.info("------------------------------- %s PREVIOUS RELATED HISTORY " + "-" * 75, tag)
        for item in prev_history:
            log.info(item)
        tag = "CONTIGUOUS" if not break_end_tstamps else "NON CONTIGUOUS"
        log.info("=============================== %s %9s HISTORY BEGINS " + "=" * 63, tag, name)
        for item in history:
            log.info(item)
        log.info("=============================== %s %9s HISTORY ENDS   " + "=" * 63, tag, name)
        tag = "" if next_history else "NO"
        log.info("------------------------------- %s NEXT RELATED HISTORY " + "-" * 79, tag)
        for item in next_history:
            log.info(item)
        for break_tstamp in break_end_tstamps:
            log.info(
                "------------------------------- %s BROKEN END TIMESTAMP RELATED HISTORY "
                + "-" * 40,
                break_tstamp,
            )
            for item in broken_end_history:
                log.info(item)
        for break_tstamp in break_start_tstamps:
            log.info(
                "------------------------------- %s BROKEN START TIMESTAMP RELATED HISTORY "
                + "-" * 38,
                break_tstamp,
            )
            for item in broken_start_history:
                log.info(item)


# =============================
# PHOTOMETER 'location' COMMAND
# =============================


def location(args):
    row = dict()
    row["longitude"] = args.longitude
    row["latitude"] = args.latitude
    log.info(
        f" ====== Geolocating Latitude {row['latitude']}, Longitude {row['longitude']} ====== "
    )
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=2)
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", language="en")
    address = location.raw["address"]
    log.info("RAW NOMINATIM METADATA IS\n%s", address)
    for location_type in ("village", "town", "city", "municipality"):
        try:
            row["town"] = address[location_type]
        except KeyError:
            row["town"] = "Unknown"
            continue
        else:
            break
    for sub_region in ("province", "state", "state_district"):
        try:
            row["sub_region"] = address[sub_region]
        except KeyError:
            row["sub_region"] = "Unknown"
            continue
        else:
            break
    for region in ("state", "state_district"):
        try:
            row["region"] = address[region]
        except KeyError:
            row["region"] = "Unknown"
            continue
        else:
            break
    row["zipcode"] = address.get("postcode", "Unknown")
    row["country"] = address.get("country", "Unknown")
    row["tzone"] = tf.timezone_at(lng=row["longitude"], lat=row["latitude"])
    log.info(row)


# ============================
# PARSER AND MAIN ENTRY POINTS
# ============================


def add_args(parser):
    subparser = parser.add_subparsers(dest="command")

    tdread = subparser.add_parser("readings", help="TessDB readings check")
    tdex0 = tdread.add_mutually_exclusive_group(required=True)
    tdex0.add_argument("-rn", "--renamed", action="store_true", help="renamed photometers only")
    tdex0.add_argument("-rp", "--repaired", action="store_true", help="repaired photometers only")
    tdex0.add_argument(
        "-ea", "--easy", action="store_true", help='"easy" (not repaired nor renamed photometers)'
    )
    tdex0.add_argument(
        "-co",
        "--complicated",
        action="store_true",
        help="complicated photometers (with repairs AND renamings)",
    )
    tdex1 = tdread.add_mutually_exclusive_group(required=True)
    tdex1.add_argument(
        "-ul",
        "--unknown-location",
        action="store_true",
        help="Check unknown location_id in tess_readings_t",
    )
    tdex1.add_argument(
        "-uo",
        "--unknown-observer",
        action="store_true",
        help="Check unknown observer_id in tess_readings_t",
    )
    tdread.add_argument(
        "-k",
        "--known",
        action="store_true",
        help="Select only with known location/observer id in tess_t",
    )

    tdcheck = subparser.add_parser("check", help="Various TESSDB metadata checks")
    tdex0 = tdcheck.add_mutually_exclusive_group(required=True)
    tdex0.add_argument("-rn", "--renamed", action="store_true", help="renamed photometers only")
    tdex0.add_argument("-rp", "--repaired", action="store_true", help="repaired photometers only")
    tdex0.add_argument(
        "-ea", "--easy", action="store_true", help='"easy" (not repaired nor renamed photometers)'
    )
    tdex0.add_argument(
        "-co",
        "--complicated",
        action="store_true",
        help="complicated photometers (with repairs AND renamings)",
    )
    tdex1 = tdcheck.add_mutually_exclusive_group(required=True)
    tdex1.add_argument(
        "-p", "--places", action="store_true", help="Check same places, different coordinates"
    )
    tdex1.add_argument(
        "-c", "--coords", action="store_true", help="Check same coordinates, different places"
    )
    tdex1.add_argument(
        "-d", "--dupl", action="store_true", help="Check same coordinates, duplicated places"
    )
    tdex1.add_argument(
        "-b", "--nearby", type=float, default=0, help="Check for nearby places, distance in meters"
    )
    tdex1.add_argument(
        "-m", "--macs", action="store_true", help="Check for proper MACS in tess_t table"
    )
    tdex1.add_argument(
        "-z", "--fake-zero-points", action="store_true", help="Check for fake zero points tess_t"
    )
    tdex1.add_argument(
        "-ul", "--unknown-location", action="store_true", help="Check unknown location in tess_t"
    )
    tdex1.add_argument(
        "-uo", "--unknown-observer", action="store_true", help="Check unknown observer in tess_t"
    )
    tdcheck.add_argument(
        "-o", "--output-file", type=str, help="Optional output CSV file to export info"
    )

    tdfix = subparser.add_parser("fix", help="Fix TessDB data/metadata")
    tdex0 = tdfix.add_mutually_exclusive_group(required=True)
    tdex0.add_argument("-rn", "--renamed", action="store_true", help="renamed photometers only")
    tdex0.add_argument("-rp", "--repaired", action="store_true", help="repaired photometers only")
    tdex0.add_argument(
        "-ea", "--easy", action="store_true", help='"easy" (not repaired nor renamed photometers)'
    )
    tdex0.add_argument(
        "-co",
        "--complicated",
        action="store_true",
        help="complicated photometers (with repairs AND renamings)",
    )
    tdex1 = tdfix.add_mutually_exclusive_group(required=True)
    tdex1.add_argument(
        "-ul", "--unknown-location", action="store_true", help="Fix unknown location readings"
    )
    tdfix.add_argument(
        "-d", "--directory", type=vdir, required=True, help="Directory to place output SQL files"
    )
    tdfix.add_argument(
        "-th", "--threshold", type=int, default=0, help="Fix if count(readings) > threshold"
    )

    tdphot = subparser.add_parser("photometer", help="TessDB photometers metadata list")
    tdphot.add_argument("-o", "--output-file", type=str, help="Optional output CSV file for output")
    tdex0 = tdphot.add_mutually_exclusive_group(required=True)
    tdex0.add_argument("-rn", "--renamed", action="store_true", help="renamed photometers only")
    tdex0.add_argument("-rp", "--repaired", action="store_true", help="repaired photometers only")
    tdex0.add_argument(
        "-ea", "--easy", action="store_true", help='"easy" (not repaired nor renamed photometers)'
    )
    tdex0.add_argument(
        "-co",
        "--complicated",
        action="store_true",
        help="complicated photometers (with repairs AND renamings)",
    )

    tdis = subparser.add_parser("history", help="Single TESSDB photometer history")
    tdis.add_argument("-o", "--output-file", type=str, help="Optional output CSV file for output")
    grp = tdis.add_mutually_exclusive_group(required=True)
    grp.add_argument("-n", "--name", type=str, help="Photometer name")
    grp.add_argument("-m", "--mac", type=vmac, help="Photometer MAC Address")

    tdloc = subparser.add_parser("location", help="Search Nominatim metadata from Coords")
    tdloc.add_argument("-lo", "--longitude", type=float, required=True, help="Longitude (degrees)")
    tdloc.add_argument("-la", "--latitude", type=float, required=True, help="latitude (degrees)")


# ================
# MAIN ENTRY POINT
# ================

ENTRY_POINT = {
    "readings": readings,
    "photometer": photometers,
    "fix": fix,
    "check": check,
    "history": history,
    "location": location,
}


def tessdb_db(args):
    func = ENTRY_POINT[args.command]
    func(args)


def main():
    execute(
        main_func=tessdb_db,
        add_args_func=add_args,
        name=__name__,
        version=__version__,
        description="STARS4ALL TessDB Utilities",
    )
