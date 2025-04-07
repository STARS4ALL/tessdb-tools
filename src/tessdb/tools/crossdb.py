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

import requests

from lica.cli import execute
from lica.validators import vdir
from lica.csv import write_csv
from lica.sqlite import open_database
from lica.jinja2 import render_from

# --------------
# local imports
# -------------

from ._version import __version__

import tessdb.tools.tessdb as tdb
import tessdb.tools.mongodb as mdb

from .dbutils import common_A_B_items, in_A_not_in_B, filter_and_flatten, filter_selected_keys
from .dbutils import group_by_place, group_by_name, group_by_coordinates, group_by_mac
from .dbutils import distance, get_mongo_api_url

# ----------------
# Module constants
# ----------------

SQL_PHOT_NEW_LOCATIONS_TEMPLATE = "sql-phot-new-locations.j2"

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)
package = __name__.split(".")[0]
render = functools.partial(render_from, package)

# -------------------------
# Module auxiliar functions
# -------------------------

### ##################### ###
### BEGIN UNUSUED SECTION
### ##################### ###


def make_nearby_filter(tuple2, lower, upper):
    def distance_filter(tuple1):
        dist = distance(tuple1, tuple2)
        return (lower <= dist <= upper) if dist is not None else False

    return distance_filter


def similar_locations_csv(iterable, path):
    with open(path, "w", newline="") as csvfile:
        fieldnames = (
            "source",
            "name",
            "longitude",
            "latitude",
            "place",
            "town",
            "sub_region",
            "region",
            "country",
            "timezone",
        )
        writer = csv.DictWriter(csvfile, delimiter=";", fieldnames=fieldnames)
        writer.writeheader()
        for row in iterable:
            writer.writerow(row)


def common_mac_check(keys, mongo_iterable, tessdb_iterable):
    log.info("comparing %d keys", len(keys))
    result = list()
    for key in sorted(keys):
        if len(mongo_iterable[key]) > 1:
            log.warn("Skippiing %s because it is duplicated: %s", key, mongo_iterable[key])
        else:
            mongo_mac = mongo_iterable[key][0]["mac"]
            tessdb_mac = tessdb_iterable[key][0]["mac"]
            if mongo_mac != tessdb_mac:
                result.append(key)
                log.debug(
                    "MAC differs for: %s. Mongo MAC = %s, Tessdb MAC = %s",
                    key,
                    mongo_mac,
                    tessdb_mac,
                )
    log.info("Found %d MAC differences", len(result))


def common_zp_check(keys, mongo_iterable, tessdb_iterable):
    log.info("comparing %d keys", len(keys))
    result = list()
    for key in sorted(keys):
        if len(mongo_iterable[key]) > 1:
            log.warn("Skippiing %s because it is duplicated: %s", key, mongo_iterable[key])
        else:
            mongo_zp = mongo_iterable[key][0]["zero_point"]
            tessdb_zp = tessdb_iterable[key][0]["zero_point"]
            tessdb_mac = tessdb_iterable[key][0]["mac"]
            if mongo_zp != tessdb_zp:
                if tessdb_zp < 19.0:
                    log.warn("Fake tessdb ZP %s for %s, (%s)", tessdb_zp, key, tessdb_mac)
                else:
                    result.append(key)
                    log.debug(
                        "ZP differs for: %s. Mongo ZP = %s, Tessdb ZP = %s",
                        key,
                        mongo_zp,
                        tessdb_zp,
                    )
    log.info("Found %d Zero Point differences", len(result))


def upd_mongo_field(mongo_dict, tessdb_dict, field):
    result = list()
    for key, row in mongo_dict.items():
        mongo_field = row[0][field]
        tessdb_field = tessdb_dict[key][0][field]
        if mongo_field != tessdb_field:
            log.debug(
                "[%s] M[%s] T[%s] UPDATING %s %s  with %s",
                key,
                row[0]["mac"],
                tessdb_dict[key][0]["mac"],
                field,
                mongo_field,
                tessdb_field,
            )
            row[0]["field"] = tessdb_field
            result.append(row)
    log.info("Updated %d rows for field %s", len(result), field)
    return result


def upd_mongo_mac(mongo_dict, tessdb_dict):
    return upd_mongo_field(mongo_dict, tessdb_dict, "mac")


def upd_mongo_zp(mongo_dict, tessdb_dict):
    return upd_mongo_field(mongo_dict, tessdb_dict, "zero_point")


def filter_fake_zero_points(tessdb_sequence):
    def _filter_fake_zero_points(x):
        log.info("%s", x)
        return 20.5 > x["zero_point"] > 18.5 if x is not None else False

    return list(filter(_filter_fake_zero_points, tessdb_sequence))


def do_update_photometer(mongo_output_list, simulated):
    url = get_mongo_api_url()
    mongo_aux_list = mdb.mongo_get_all_info(url)
    for [row] in mongo_output_list:
        oldmac = mdb.get_mac(mongo_aux_list, row["name"])
        body = mdb.mongo_api_body_photometer(row, mongo_aux_list)
        log.info("Updating MongoDB with photometer info for %s (%s)", row["name"], oldmac)
        try:
            if oldmac != row["mac"]:
                log.info("Changing %s MAC: (%s) -> (%s)", row["name"], oldmac, row["mac"])
            mdb.mongo_api_update(url, body, oldmac, simulated)
        except ValueError:
            log.warn("Ignoring update photometer info for item %s (%s)", row["name"], oldmac)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")


def check_unknown(connection, url):
    url = get_mongo_api_url()
    mdb_input_list = mdb.mongo_get_all_info(url)
    log.info("MongoDB: Read %d items", len(mdb_input_list))
    mongo_phot = group_by_name(mdb_input_list)
    tdb_input_list = photometers_with_unknown_current_location(connection)
    tessdb_phot = group_by_name(tdb_input_list)
    log.info("read %d items from TessDB", len(tessdb_phot))
    photometer_names = common_A_B_items(mongo_phot, tessdb_phot)
    log.info("FOUND EXISTING LOCATIONS FOR %d entries", len(photometer_names))
    log.info(photometer_names)
    for name in photometer_names:
        log.info(mongo_phot[name])


def photometers(args):
    log.info(" ====================== CROSS DB PHOTOMETER METADATA ======================")

    connection, path = open_database(None, "TESSDB_URL")
    url = get_mongo_api_url()
    mdb_input_list = mdb.mongo_get_all_info(url)
    log.info("MongoDB: Read %d items", len(mdb_input_list))
    mongo_phot = group_by_name(mdb_input_list)
    tdb_input_list = photometers_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tdb_input_list))

    if args.sim_update_mac:
        tessdb_phot = group_by_name(tdb_input_list)
        photometer_names = common_A_B_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB", len(photometer_names))
        mdb_input_list = mdb.filter_by_names(mdb_input_list, photometer_names)
        mongo_phot = group_by_name(mdb_input_list)  # Again
        mongo_output_list = upd_mongo_mac(mongo_phot, tessdb_phot)
        do_update_photometer(mongo_output_list, simulated=True)
    elif args.update_mac:
        tessdb_phot = group_by_name(tdb_input_list)
        photometer_names = common_A_B_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB", len(photometer_names))
        mdb_input_list = mdb.filter_by_names(mdb_input_list, photometer_names)
        mongo_phot = group_by_name(mdb_input_list)  # Again
        mongo_output_list = upd_mongo_mac(mongo_phot, tessdb_phot)
        do_update_photometer(mongo_output_list, simulated=False)
    if args.sim_update_zp:
        tdb_input_list = filter_fake_zero_points(tdb_input_list)
        log.info("filtered fake ZP, remaining %d items from TessDB", len(tdb_input_list))
        tessdb_phot = group_by_name(tdb_input_list)
        photometer_names = common_A_B_items(mongo_phot, tessdb_phot)
        log.info("%d photometers in common between MongoDB and TessDB", len(photometer_names))
        mdb_input_list = mdb.filter_by_names(mdb_input_list, photometer_names)
        mongo_phot = group_by_name(mdb_input_list)  # Again
        mongo_output_list = upd_mongo_zp(mongo_phot, tessdb_phot)
        # do_update_photometer(mongo_output_list, simulated=True)


# Nos quedamos con el primer elemento del array
def flatten(grouped_iterable, keys):
    return tuple(grouped_iterable[key][0] for key in keys)


def filter_out_from_location(row):
    if "contact_name" in row:
        del row["contact_name"]
    if "contact_email" in row:
        del row["contact_email"]
    if "org_logo" in row:
        del row["org_logo"]
    if "org_email" in row:
        del row["org_email"]
    if "org_web" in row:
        del row["org_web"]
    if "org_descr" in row:
        del row["org_descr"]
    if "org_name" in row:
        del row["org_name"]
    if "mac" in row:
        del row["mac"]
    return row


def locations_by_place(args):
    log.info(" ====================== ANALIZING CROSS DB LOCATION METADATA ======================")
    X_HEADER = (
        "place",
        "longitude",
        "latitude",
        "town",
        "sub_region",
        "region",
        "country",
        "timezone",
        "name",
    )
    connection, path = open_database(None, "TESSDB_URL")
    url = get_mongo_api_url()
    mdb_input_list = mdb.mongo_get_location_info(url)
    log.info("MongoDB: Read %d items", len(mdb_input_list))
    mongo_place = group_by_place(mdb_input_list)
    tdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tdb_input_list))
    tessdb_loc = group_by_place(tdb_input_list)
    if args.mongo:
        locations = in_A_not_in_B(mongo_place, tessdb_loc)
        log.info("%d locations (by place name) exclusive MongoDB locations", len(locations))
        flattended_mongo_places = flatten(mongo_place, locations)
        output_list = list(filter(filter_out_from_location, flattended_mongo_places))
        suffix = "_in_mongo_not_tessdb"
    if args.tess:
        locations = in_A_not_in_B(tessdb_loc, mongo_place)
        log.info(
            "%d locations locations (by place name) exclusive TessDB locations", len(locations)
        )
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        output_list = list(filter(filter_out_from_location, flattened_tessdb_loc))
        suffix = "_in_tessdb_not_mongo"
    if args.common:
        locations = common_A_B_items(mongo_place, tessdb_loc)
        log.info(
            "%d locations locations (by place name) in common between MongoDB and TessDB",
            len(locations),
        )
        for location in locations:
            log.debug("Location %s", location)
        flattended_mongo_places = flatten(mongo_place, locations)
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        combined_list = zip(flattended_mongo_places, flattened_tessdb_loc)
        combined_list = [j for i in zip(flattended_mongo_places, flattened_tessdb_loc) for j in i]
        output_list = list(filter(filter_out_from_location, combined_list))
        suffix = "_common_mongo_tessdb"
    return output_list, suffix


def locations_by_coordinates(args):
    log.info(" ====================== ANALIZING CROSS DB LOCATION METADATA ======================")
    X_HEADER = (
        "place",
        "longitude",
        "latitude",
        "town",
        "sub_region",
        "region",
        "country",
        "timezone",
        "name",
    )
    connection, path = open_database(None, "TESSDB_URL")
    url = get_mongo_api_url()
    mdb_input_list = mdb.mongo_get_location_info(url)
    log.info("MongoDB: Read %d items", len(mdb_input_list))
    mongo_place = group_by_coordinates(mdb_input_list)
    tdb_input_list = photometers_and_locations_from_tessdb(connection)
    log.info("read %d items from TessDB", len(tdb_input_list))
    tessdb_loc = group_by_coordinates(tdb_input_list)
    if args.mongo:
        locations = in_A_not_in_B(mongo_place, tessdb_loc)
        log.info("%d locations (by exact coordinates) exclusive MongoDB locations", len(locations))
        flattended_mongo_places = flatten(mongo_place, locations)
        output_list = list(filter(filter_out_from_location, flattended_mongo_places))
        suffix = "_in_mongo_not_tessdb"
    if args.tess:
        locations = in_A_not_in_B(tessdb_loc, mongo_place)
        log.info("%d locations (by exact coordinates) exclusive TessDB locations", len(locations))
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        output_list = list(filter(filter_out_from_location, flattened_tessdb_loc))
        suffix = "_in_tessdb_not_mongo"
    if args.common:
        locations = common_A_B_items(mongo_place, tessdb_loc)
        log.info(
            "%d locations (by exact coordinates) in common between MongoDB and TessDB",
            len(locations),
        )
        for location in locations:
            log.debug("Location %s", location)
        flattended_mongo_places = flatten(mongo_place, locations)
        flattened_tessdb_loc = flatten(tessdb_loc, locations)
        combined_list = zip(flattended_mongo_places, flattened_tessdb_loc)
        combined_list = [j for i in zip(flattended_mongo_places, flattened_tessdb_loc) for j in i]
        output_list = list(filter(filter_out_from_location, combined_list))
        suffix = "_common_mongo_tessdb"
    return output_list, suffix


def locations(args):
    if args.place:
        output_list, suffix = locations_by_place(args)
    else:
        output_list, suffix = locations_by_coordinates(args)
    if args.output_prefix:
        path = args.output_prefix + suffix + ".csv"
        log.info("Writting to CSV file: %s", path)
        X_HEADER = (
            "place",
            "longitude",
            "latitude",
            "town",
            "sub_region",
            "region",
            "country",
            "timezone",
            "name",
        )
        write_csv(path, X_HEADER, output_list)
    else:
        pass  # AQUI TENGO QUE TRABAJAR


def coordinates(args):
    """Not being used"""
    log.info(
        " ====================== ANALIZING CROSS DB COORDINATES METADATA ======================"
    )
    X_HEADER = (
        "mng_name",
        "tdb_name",
        "mng_coords",
        "tdb_coords",
        "mng_place",
        "tdb_place",
        "mng_town",
        "tdb_town",
        "mng_sub_region",
        "tdb_sub_region",
        "mng_region",
        "tdb_region",
        "mng_country",
        "tdb_country",
        "mng_timezone",
        "tdb_timezone",
    )
    url = get_mongo_api_url()
    connection, path = open_database(None, "TESSDB_URL")
    log.info("reading items from MongoDB")
    mongo_input_map = by_coordinates(mdb.mongo_get_location_info(url))
    log.info("reading items from TessDB")
    tessdb_input_map = by_coordinates(places_from_tessdb(connection))
    output = list()
    for i, (mongo_coords, mongo_items) in enumerate(mongo_input_map.items()):
        nearby_filter = make_nearby_filter(mongo_coords, args.lower, args.upper)
        nearby_list = list(filter(nearby_filter, tessdb_input_map))
        nearby_map = dict(zip(nearby_list, [tessdb_input_map[k] for k in nearby_list]))
        for j, (tessdb_coords, tessdb_items) in enumerate(nearby_map.items()):
            log.info("===================")
            log.info(
                "len(MONGO ITEM[%d]) = %d, len(TESSDB ITEM[%d] = %d)",
                i,
                len(mongo_items),
                j,
                len(tessdb_items),
            )
            for mongo_row in mongo_items:
                for tessdb_row in tessdb_items:
                    log.info(
                        "DIST: %d, MONGO ITEM: %s TESSDB ITEM: %s",
                        distance(mongo_coords, tessdb_coords),
                        mongo_row,
                        tessdb_row,
                    )
                    output.append(
                        {
                            "mng_coords": str(mongo_coords),
                            "tdb_coords": str(tessdb_coords),
                            "distance": distance(mongo_coords, tessdb_coords),
                            "mng_name": mongo_row["name"],
                            "tdb_name": tessdb_row["name"],
                            "mng_place": mongo_row["place"],
                            "tdb_place": tessdb_row["place"],
                            "mng_town": mongo_row["town"],
                            "tdb_town": tessdb_row["town"],
                            "mng_sub_region": mongo_row["sub_region"],
                            "tdb_sub_region": tessdb_row["sub_region"],
                            "mng_region": mongo_row["region"],
                            "tdb_region": tessdb_row["region"],
                            "mng_country": mongo_row["country"],
                            "tdb_country": tessdb_row["country"],
                            "mng_timezone": mongo_row["timezone"],
                            "tdb_timezone": tessdb_row["timezone"],
                        }
                    )
    write_csv(output, X_HEADER, args.file)


### ################### ###
### END UNUSUED SECTION
### ################### ###

### ###################### ###
### NEW REFACTORED SECTION
### ###################### ###


def quote_for_sql(row):
    for key in (
        "timezone",
        "place",
        "town",
        "sub_region",
        "region",
        "country",
        "org_name",
        "org_email",
    ):
        if row.get(key) is not None:
            row[key] = "'" + row[key].replace("'", "''") + "'"
        else:
            row[key] = "NULL"
    return row


def same_mac_filter(mdb_input_dict, tdb_input_dict):
    result = list()
    for name in sorted(mdb_input_dict.keys()):
        mongo_mac = mdb_input_dict[name][0]["mac"]
        tessdb_mac = tdb_input_dict[name][0]["mac"]
        if mongo_mac != tessdb_mac:
            log.info(
                "Excluding photometer %s with different MACs: MongoDB (%s), TESSDB (%s)",
                name,
                mongo_mac,
                tessdb_mac,
            )
        else:
            result.append(name)
    return result


def update_tdb_dict_with_mdb_dict_location(tdb_phot_dict, mdb_phot_dict):
    for name, values in tdb_phot_dict.items():
        N = len(values)
        if N > 1:
            log.info("Updating %d copies of %s", N, name)
        for i in range(len(values)):
            for field in (
                "longitude",
                "latitude",
                "place",
                "town",
                "sub_region",
                "region",
                "country",
                "timezone",
                "org_name",
                "org_email",
            ):
                values[i][field] = mdb_phot_dict[name][0][field]


def location_common_unknown(mdb_input_list, connection, classification):
    tdb_input_list = tdb.photometers_with_unknown_location(connection, classification)
    log.info("TessDB: Read %d items", len(tdb_input_list))
    if classification == "easy" or classification == "renamed":
        tdb_input_list = list(filter(tdb.filter_current_name, tdb_input_list))
    log.info("TessDB: after filtering, %d items", len(tdb_input_list))
    # Wgen compaing to MongoDB, we must group by name
    # and themn select all the common names in each dict
    mdb_phot_dict = group_by_name(mdb_input_list)
    tdb_phot_dict = group_by_name(tdb_input_list)
    common_names = common_A_B_items(mdb_phot_dict, tdb_phot_dict)
    log.info("Found %d common location entries", len(common_names))
    mdb_phot_dict = filter_selected_keys(mdb_phot_dict, common_names)
    tdb_phot_dict = filter_selected_keys(tdb_phot_dict, common_names)
    if classification == "easy" or classification == "renamed":
        common_names = same_mac_filter(mdb_phot_dict, tdb_phot_dict)
    else:
        common_names = tdb_phot_dict.keys()
    log.info("Reduced list of only %d names after MAC exclusion", len(common_names))
    mdb_phot_dict = filter_selected_keys(mdb_phot_dict, common_names)
    tdb_phot_dict = filter_selected_keys(tdb_phot_dict, common_names)
    return mdb_phot_dict, tdb_phot_dict


def location_check_unknown(mdb_input_list, connection, classification):
    mdb_phot_dict, tdb_phot_dict = location_common_unknown(
        mdb_input_list, connection, classification
    )
    for k, values in tdb_phot_dict.items():
        for v in values:
            log.info("%s", v)
    log.info("Must update %s", " ".join(sorted(mdb_phot_dict.keys())))


def location_generate_unknown(mdb_input_list, connection, classification, output_dir):
    mdb_phot_dict, tdb_phot_dict = location_common_unknown(
        mdb_input_list, connection, classification
    )
    update_tdb_dict_with_mdb_dict_location(tdb_phot_dict, mdb_phot_dict)
    tessdb_phot_list = filter_and_flatten(tdb_phot_dict)
    tessdb_phot_list = list(map(quote_for_sql, tessdb_phot_list))
    # However, to generate SQL, we mut group by mac, to get all tess_ids for a single MAC
    tdb_phot_dict = group_by_mac(tessdb_phot_list)
    get_tess_ids = functools.partial(tdb.get_as_list, "tess_id")
    tdb_phot_ids = get_tess_ids(tdb_phot_dict)
    for i, key in enumerate(sorted(tdb_phot_dict.keys()), start=1):
        context = dict()
        phot = tdb_phot_dict[key][0]
        phot["tess_ids"] = list(map(lambda x: str(x), tdb_phot_ids[key]))  # Append this item
        if classification == "renamed":
            context["names_history"] = tdb.names(connection, key)
            context["mac"] = key
        elif classification == "repaired":
            context["macs_history"] = tdb.mac_addresses(connection, phot["name"])
            context["name"] = phot["name"]
        context["row"] = phot
        context["i"] = i
        context["classification"] = classification
        name = phot["name"]
        output = render(SQL_PHOT_NEW_LOCATIONS_TEMPLATE, context)
        output_path = os.path.join(output_dir, f"{i:03d}_{name}_{classification}_new_unknown.sql")
        with open(output_path, "w") as sqlfile:
            sqlfile.write(output)


def location_check(args):
    log.info(" ====================== PERFORM CROSS DB LOCATION CHECKS ======================")
    connection, path = open_database(None, "TESSDB_URL")
    url = get_mongo_api_url()
    mdb_input_list = mdb.mongo_get_all_info(url)
    log.info("MongoDB: Read %d items", len(mdb_input_list))
    classification = tdb.photometer_classification(args)
    if args.unknown:
        location_check_unknown(mdb_input_list, connection, classification)


def location_fix(args):
    log.info(
        " ====================== PERFORM CROSS DB SQL LOCATION GENERATION ======================"
    )
    connection, path = open_database(None, "TESSDB_URL")
    url = get_mongo_api_url()
    mdb_input_list = mdb.mongo_get_all_info(url)  # W need the MAC address for safety reasons
    log.info("MongoDB: Read %d items", len(mdb_input_list))
    classification = tdb.photometer_classification(args)
    if args.unknown:
        location_generate_unknown(mdb_input_list, connection, classification, args.directory)


def add_args(parser):
    # -----------------------------------------
    # Create second level parsers for 'crossdb'
    # -----------------------------------------

    cmd_subparser = parser.add_subparsers(dest="command")
    parser_location = cmd_subparser.add_parser("location", help="location commands")

    # --------------------
    # Location subcommands
    # --------------------

    subparser = parser_location.add_subparsers(dest="subcommand")
    check = subparser.add_parser("check", help="Cross DB locations metadata check")
    grp0 = check.add_mutually_exclusive_group(required=True)
    grp0.add_argument("-rn", "--renamed", action="store_true", help="renamed photometers only")
    grp0.add_argument("-rp", "--repaired", action="store_true", help="repaired photometers only")
    grp0.add_argument(
        "-ea", "--easy", action="store_true", help='"easy" (not repaired nor renamed photometers)'
    )
    grp0.add_argument(
        "-co",
        "--complicated",
        action="store_true",
        help="complicated photometers (with repairs AND renamings)",
    )
    grp1 = check.add_mutually_exclusive_group(required=True)
    grp1.add_argument(
        "-u",
        "--unknown",
        action="store_true",
        help="Unknown TessDB locations vs known MongoDB locations",
    )

    fix = subparser.add_parser("fix", help="Cross DB SQL fix patch")
    fix.add_argument(
        "-d", "--directory", type=vdir, required=True, help="Directory to place output SQL files"
    )
    grp0 = fix.add_mutually_exclusive_group(required=True)
    grp0.add_argument("-rn", "--renamed", action="store_true", help="renamed photometers only")
    grp0.add_argument("-rp", "--repaired", action="store_true", help="repaired photometers only")
    grp0.add_argument(
        "-ea", "--easy", action="store_true", help='"easy" (not repaired nor renamed photometers)'
    )
    grp0.add_argument(
        "-co",
        "--complicated",
        action="store_true",
        help="complicated photometers (with repairs AND renamings)",
    )
    grp1 = fix.add_mutually_exclusive_group(required=True)
    grp1.add_argument(
        "-u",
        "--unknown",
        action="store_true",
        help="Unknown TessDB locations vs known MongoDB locations",
    )

    # ---------------------
    # Photometer subcommands
    # ----------------------


# ================
# MAIN ENTRY POINT
# ================


def cross_db(args):
    func = args.command + "_" + args.subcommand
    globals()[func](args)


def main():
    execute(
        main_func=cross_db,
        add_args_func=add_args,
        name=__name__,
        version=__version__,
        description="STARS4ALL Cross TESSDB-MongoDB Utilities",
    )
