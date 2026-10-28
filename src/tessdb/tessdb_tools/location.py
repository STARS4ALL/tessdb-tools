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

# -------------------
# Third party imports
# -------------------

from lica.jinja2 import render_from

# --------------
# local imports
# -------------


from .utils import formatted_mac

from .dbutils import group_by_name, common_A_B_items, distance
from .mongodb import mongo_get_all_info


# ----------------
# Module constants
# ----------------

# Distance to consider all coordinates to be the same place
# between tessdb and MongoDB
# Experimentally determined by establishing a growth curve
# with 1, 10, 50, 100, 150, 200 & 500 m
NEARBY_DISTANCE = 200  # meters

SQL_INSERT_LOCATIONS_TEMPLATE = "sql-location-insert.j2"
SQL_PHOT_NEW_LOCATIONS_TEMPLATE = "sql-phot-new-locations.j2"
SQL_PHOT_UPD_LOCATIONS_TEMPLATE = "sql-phot-upd-locations.j2"
SQL_PHOT_UPD_META_LOCATIONS_TEMPLATE = "sql-phot-upd-locations-metadata.j2"


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

# -------------------------
# Module auxiliar functions
# -------------------------

package = __name__.split(".")[0]
render = functools.partial(render_from, package)

# ============================================================
# DISMANTELED AS AN UTILITY
# REMAINS HERE FOR USEFUL CODE THAT COULD BE PORTED ELSEWHERE
# ============================================================


def _easy_photometers_with_former_locations_from_tessdb(connection):
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT name, t.mac_address, tess_id, zp1, location_id,
            l.place, l.town, l.sub_region, l.region, l.country, l.timezone, l.organization, l.contact_email
        FROM tess_t AS t
        JOIN name_to_mac_t AS n USING(mac_address)
        JOIN location_t  AS l USING(location_id)
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
        AND location_id > -1
        ORDER BY mac_address, t.valid_since
        """
    )
    return cursor


def tessdb_remap_location_info(row):
    new_row = dict()
    new_row["name"] = row[0]
    try:
        new_row["mac"] = formatted_mac(row[1])
    except ValueError:
        return None
    new_row["tess_id"] = row[2]
    new_row["zero_point"] = row[3]
    new_row["location_id"] = row[4]
    new_row["place"] = row[5]
    new_row["town"] = row[6]
    new_row["sub_region"] = row[7]
    new_row["region"] = row[8]
    new_row["country"] = row[9]
    new_row["timezone"] = row[10]
    new_row["org_name"] = row[11]
    new_row["org_email"] = row[12]
    return new_row


def tessdb_remap_location_info2(row):
    new_row = dict()
    new_row["name"] = row[0]
    try:
        new_row["mac"] = formatted_mac(row[1])
    except ValueError:
        return None
    new_row["tess_id"] = row[2]
    new_row["zero_point"] = row[3]
    new_row["location_id"] = row[4]
    new_row["place"] = row[5]
    new_row["town"] = row[6]
    new_row["sub_region"] = row[7]
    new_row["region"] = row[8]
    new_row["country"] = row[9]
    new_row["timezone"] = row[10]
    new_row["org_name"] = row[11]
    new_row["org_email"] = row[12]
    new_row["valid_since"] = row[13]  # valid period in names_to_mac table
    new_row["valid_until"] = row[14]
    new_row["valid_state"] = row[15]
    return new_row


def easy_photometers_with_unknown_locations_from_tessdb(connection):
    return list(
        map(
            tessdb_remap_location_info,
            _easy_photometers_with_unknown_locations_from_tessdb(connection),
        )
    )


def easy_photometers_with_former_locations_from_tessdb(connection):
    return list(
        map(
            tessdb_remap_location_info,
            _easy_photometers_with_former_locations_from_tessdb(connection),
        )
    )


def repaired_photometers_with_locations_from_tessdb(connection):
    return list(
        map(
            tessdb_remap_location_info2,
            _repaired_photometers_with_locations_from_tessdb(connection),
        )
    )


def new_photometer_location(mongo_db_input_dict, tessdb_input_dict):
    photometers = list()
    for name, value in sorted(mongo_db_input_dict.items()):
        assert len(value) == 1
        row = value[0]
        row["masl"] = 0.0
        row["mac"] = formatted_mac(row["mac"])
        row["tess_ids"] = tuple(str(item["tess_id"]) for item in tessdb_input_dict[name])
        log.debug(
            "Must update %s [%s] with (%s,%s) coords",
            name,
            row["mac"],
            row["longitude"],
            row["latitude"],
        )
        photometers.append(row)
    return photometers


def check_same_location_metadata(mongo_row, tessdb_sequence):
    # We have already checked that all locations in the sereral tessdb_sequence are the same
    tessdb_row = tessdb_sequence[0]
    same = (
        (mongo_row["place"] == tessdb_row["place"])
        and (mongo_row["town"] == tessdb_row["town"])
        and (mongo_row["sub_region"] == tessdb_row["sub_region"])
        and (mongo_row["region"] == tessdb_row["region"])
        and (mongo_row["country"] == tessdb_row["country"])
        and (mongo_row["timezone"] == tessdb_row["timezone"])
    )  # and \
    # (mongo_row['org_name'] == tessdb_row['org_name']) and (mongo_row['org_email'] == tessdb_row['org_email'])
    if not same:
        log.info("METADATA DIFFERENCE Mongo %s \n TessDB %s", mongo_row, tessdb_row)
    return same


def existing_photometer_location(mongo_db_input_dict, tessdb_input_dict, connection):
    inserters, updaters, metas = list(), list(), list()
    n_inserts = 0
    n_updates = 0
    n_metas = 0
    for name, value in sorted(mongo_db_input_dict.items()):
        assert len(value) == 1
        row = value[0]
        row["masl"] = 0.0
        row["mac"] = formatted_mac(row["mac"])
        row["tess_ids"] = tuple(str(item["tess_id"]) for item in tessdb_input_dict[name])
        locations = tuple(item["location_id"] for item in tessdb_input_dict[name])
        assert all(loc == locations[0] for loc in locations)
        row["location_id"] = locations[0]
        # log.info("Must update %s [%s] with (%s,%s) coords", name, row['mac'], row['longitude'], row['latitude'])
        tessdb_coords = coordinates_from_location_id(connection, row["location_id"])
        mongodb_coords = (row["longitude"], row["latitude"])
        dist = distance(mongodb_coords, tessdb_coords)
        if dist == 0:
            if not check_same_location_metadata(row, tessdb_input_dict[name]):
                n_metas += 1
                metas.append(row)
        elif dist < NEARBY_DISTANCE:
            n_updates += 1
            updaters.append(row)
        else:
            n_inserts += 1
            inserters.append(row)
    log.info(
        "Must perform %d location info updates, %d location info inserts and %d metadata updates",
        n_updates,
        n_inserts,
        n_metas,
    )
    return inserters, updaters, metas


# ======================
# Second level functions
# ======================


def generate_single(connection, mongodb_url, output_dir):
    log.info("Accesing TESSDB database")
    tessdb_input_list = easy_photometers_with_former_locations_from_tessdb(connection)
    tessdb_input_dict = group_by_name(tessdb_input_list)
    log.info("TESSDB Photometer entries with former locations: %d", len(tessdb_input_dict))
    log.info("Accesing MongoDB database")
    mongodb_input_list = mongo_get_all_info(mongodb_url)
    mongo_db_input_dict = group_by_name(mongodb_input_list)
    common_names = common_A_B_items(tessdb_input_dict, mongo_db_input_dict)
    log.info("Photometer names that must be updated with MongoDB location: %d", len(common_names))
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names}
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names}
    common_names = same_mac_filter(mongo_db_input_dict, tessdb_input_dict)
    log.info("Reduced list of only %d entries after MAC exclusion", len(common_names))
    mongo_db_input_dict = {key: mongo_db_input_dict[key] for key in common_names}
    tessdb_input_dict = {key: tessdb_input_dict[key] for key in common_names}
    photometers_with_new_locations, photometers_with_upd_locations, location_metadata_upd = (
        existing_photometer_location(mongo_db_input_dict, tessdb_input_dict, connection)
    )
    photometers_with_new_locations = list(map(quote_for_sql, photometers_with_new_locations))
    for i, phot in enumerate(photometers_with_new_locations, 1):
        context = dict()
        context["row"] = phot
        context["i"] = i
        name = phot["name"]
        output = render(SQL_PHOT_NEW_LOCATIONS_TEMPLATE, context)
        output_path = os.path.join(output_dir, f"{i:03d}_{name}_new_single.sql")
        with open(output_path, "w") as sqlfile:
            sqlfile.write(output)
    photometers_with_upd_locations = list(map(quote_for_sql, photometers_with_upd_locations))
    for i, phot in enumerate(photometers_with_upd_locations, 1):
        context = dict()
        context["row"] = phot
        context["i"] = i
        name = phot["name"]
        output = render(SQL_PHOT_UPD_LOCATIONS_TEMPLATE, context)
        output_path = os.path.join(output_dir, f"{i:03d}_{name}_upd_single.sql")
        with open(output_path, "w") as sqlfile:
            sqlfile.write(output)
    photometers_with_upd_metadata_locations = list(map(quote_for_sql, location_metadata_upd))
    for i, phot in enumerate(location_metadata_upd, 1):
        context = dict()
        context["row"] = phot
        context["i"] = i
        name = phot["name"]
        output = render(SQL_PHOT_UPD_META_LOCATIONS_TEMPLATE, context)
        output_path = os.path.join(output_dir, f"{i:03d}_{name}_upd_meta_single.sql")
        with open(output_path, "w") as sqlfile:
            sqlfile.write(output)


# ===================
# Module entry points
# ===================


# ================
# MAIN ENTRY POINT
# ================

# ============================================================
# DISMANTELED AS AN UTILITY
# REMAINS HERE FOR USEFUL CODE THAT COULD BE PORTED ELSEWHERE
# ============================================================
