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
import statistics
import collections

# -------------------
# Third party imports
# -------------------

from lica.csv import read_csv

# --------------
# local imports
# -------------

from .dbutils import common_A_B_items, in_A_not_in_B, group_by_name, filter_and_flatten
from .mongodb import mongo_get_all, get_mongo_api_url

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)


FORMS_COLUMNS = (
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

# --------------------------
# Module auxiliar functions
# -------------------------


def google_remap_info(row):
    new_row = dict()
    new_row["name"] = row["Photometer name (Example: stars1)"]
    try:
        new_row["latitude"] = float(row["Latitude (example 42.71167)"])
    except ValueError:
        log.info(
            "[%s] Could not convert to float %s",
            new_row["name"],
            row["Latitude (example 42.71167)"],
        )
        return None
    try:
        new_row["longitude"] = float(row["Longitude (example -1.86509)"])
    except ValueError:
        log.info(
            "Could not convert to float %s", new_row["name"], row["Longitude (example -1.86509)"]
        )
        return None
    new_row["place"] = row["Place"]
    new_row["town"] = row["Town"]
    new_row["region"] = None
    new_row["sub_region"] = None
    new_row["country"] = None
    new_row["org_name"] = row["Organization Name"]
    new_row["org_email"] = None
    new_row["org_description"] = row["Description"]
    new_row["org_web_url"] = row["Web Organization URL"]
    new_row["org_logo_url"] = row["Logo"]
    new_row["org_phone"] = None
    new_row["contact_name"] = row["Name"]
    new_row["contact_mail"] = row["Contact email"]
    new_row["contact_phone"] = None
    new_row["filters"] = None
    new_row["zero_point"] = None
    new_row["period"] = None
    new_row["mac"] = None
    new_row["timestamp"] = row["Timestamp"]  # This is new
    return new_row


def check_common(mongo_url, input_path):
    google_forms_list = read_csv(input_path, delimiter=",")
    google_forms_list = list(
        filter(lambda x: x is not None, map(google_remap_info, google_forms_list))
    )
    mongo_input_list = mongo_get_all(mongo_url)
    log.info("Classifying MongoDB input list by name")
    mongo_input_list = group_by_name(mongo_input_list)
    log.info("Classifying Google Forms input list by name")
    google_forms_list = group_by_name(google_forms_list)
    common_names = common_A_B_items(google_forms_list, mongo_input_list)
    log.info("Common Photometer Name names: %d", len(common_names))


def check_google(mongo_url, input_path):
    google_forms_list = read_csv(input_path, delimiter=",")
    google_forms_list = list(
        filter(lambda x: x is not None, map(google_remap_info, google_forms_list))
    )
    mongo_input_list = mongo_get_all(mongo_url)
    log.info("Classifying MongoDB input list by name")
    mongo_input_list = group_by_name(mongo_input_list)
    log.info("Classifying Google Forms input list by name")
    google_forms_list = group_by_name(google_forms_list)
    in_google_names = in_A_not_in_B(google_forms_list, mongo_input_list)
    log.info("Photometers in Google Forms, not in MongoDB: %d", len(in_google_names))
    log.info("Detailed list of photometers in Google Forms, not in MongoDB: %s", in_google_names)
    google_forms_list = filter_and_flatten(google_forms_list, in_google_names)
    for phot in google_forms_list:
        log.info("%s", phot)


# ===================
# Module entry points
# ===================


def check(options):
    log.info(
        " ====================== CROSS GOOGLE FORMS / TESSD FILE comparison ======================"
    )
    url = get_mongo_api_url()
    if options.common:
        check_common(url, options.file)
    elif options.google:
        check_google(url, options.file)
    else:
        raise NotImplementedError("CLI Option not implemented")
