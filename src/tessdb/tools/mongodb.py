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
import json
import logging

# -------------------
# Third party imports
# -------------------

import requests

from lica.cli import execute
from lica.validators import vfile
from lica.csv import read_csv, write_csv

# --------------
# local imports
# -------------

from ._version import __version__

from .utils import formatted_mac, is_tess_mac, is_mac
from .dbutils import (
    group_by_place,
    group_by_name,
    group_by_mac,
    group_by_coordinates,
    log_places,
    log_names,
    log_macs,
    log_coordinates,
    log_coordinates_nearby,
)
from .dbutils import (
    get_mongo_api_url,
    get_mongo_api_key,
    geolocate,
    timezone,
    common_A_B_items,
    in_A_not_in_B,
    filter_and_flatten,
)


class ListLengthMismatchError(Exception):
    """List length mismatch error between lists"""

    def __str__(self):
        s = self.__doc__
        if self.args:
            s = "{0}: {1!s}".format(s, self.args[0])
        s = "{0}.".format(s)
        return s


class NamesMismatchError(Exception):
    """Names mismatch error between items"""

    def __str__(self):
        s = self.__doc__
        if self.args:
            s = "{0}: {1!s}".format(s, self.args[0])
        s = "{0}.".format(s)
        return s


class DuplicatesError(Exception):
    """Duplicates Error"""

    def __str__(self):
        s = self.__doc__
        if self.args:
            s = "{0}: {1!s}".format(s, self.args[0])
        s = "{0}.".format(s)
        return s


# ----------------
# Module constants
# ----------------

# CSV File generation rows
LOCATION_HEADER = (
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
PHOTOMETER_HEADER = (
    "name",
    "old_mac",
    "mac",
    "old_zero_point",
    "zero_point",
    "old_filters",
    "filters",
    "period",
    "comment",
)
PHOTOMETER_HEADER2 = ("name", "mac", "zero_point", "filters", "period")
ORGANIZATION_HEADER = (
    "name",
    "org_name",
    "org_description",
    "org_phone",
    "org_email",
    "org_web_url",
    "org_logo_url",
)
CONTACT_HEADER = ("name", "contact_name", "contact_mail", "contact_phone")
ALL_HEADER = PHOTOMETER_HEADER2 + LOCATION_HEADER[1:] + ORGANIZATION_HEADER[1:] + CONTACT_HEADER[1:]

NOMINATIM_HEADER = (
    "name",
    "longitude",
    "latitude",
    "place",
    "nominatim_place",
    "nominatim_place_type",
    "town",
    "nominatim_town",
    "nominatim_town_type",
    "sub_region",
    "nominatim_sub_region",
    "nominatim_sub_region_type",
    "region",
    "nominatim_region",
    "nominatim_region_type",
    "country",
    "nominatim_country",
    "timezone",
    "nominatim_timezone",
    "nominatim_zipcode",
)

TZFINDER_HEADER = (
    "name",
    "longitude",
    "latitude",
    "place",
    "town",
    "sub_region",
    "region",
    "country",
    "timezone",
    "tzfinder_timezone",
)

DEFAULT_LOCATION = {
    "longitude": 0.0,
    "latitude": 0.0,
    "place": None,
    "town": None,
    "sub_region": None,
    "region": None,
    "country": None,
}
DEFAULT_IMG = {"urls": []}
DEFAULT_ORG = {}
DEFAULT_CONTACT = {}

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

# -------------------------
# Module auxiliar functions
# -------------------------


def mongo_api_get_names(url):
    """This request gets name, mac & location info"""
    url = url + "/photometers_list"
    body = {"token": get_mongo_api_key()}
    response = requests.post(url, json=body)
    response.raise_for_status()
    return response.json()


def mongo_api_get_details(url):
    """This requests gets all infomation except mac"""
    url = url + "/photometers"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def mongo_api_get_details_single(url, name):
    """This requests gets all infomation except mac"""
    url = url + "/photometers/" + name
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def mongo_api_create(url, body, simulated):
    """Create a new entry in MongoDB"""
    body["isNew"] = True
    body["token"] = get_mongo_api_key()
    name = body["tess"]["name"]
    mac = body["tess"]["mac"]
    url = url + "/photometers/" + name + "/" + mac
    if not simulated:
        response = requests.post(url, json=body)
        response.raise_for_status()
    else:
        req = requests.Request("POST", url, data=json.dumps(body, indent=2))
        prepared = req.prepare()
        prepared.headers["Content-Type"] = "application/json"
        print(
            "{}\n{}\r\n{}\r\n\r\n{}\n{}".format(
                "=" * 66,
                prepared.method + " " + prepared.url,
                "\r\n".join("{}: {}".format(k, v) for k, v in prepared.headers.items()),
                prepared.body,
                "=" * 66,
            )
        )


def mongo_api_update(url, body, mac, simulated=None):
    """Update an existing entry in MongoDB"""
    body["isNew"] = False
    body["token"] = get_mongo_api_key()
    name = body["tess"]["name"]
    if not mac:
        raise ValueError("Missing MAC for photometer {name}")
    url = f"{url}/photometers/{name}/{mac}"
    if not simulated:
        response = requests.post(url, json=body)
        response.raise_for_status()
    else:
        req = requests.Request("POST", url, data=json.dumps(body, indent=2))
        prepared = req.prepare()
        prepared.headers["Content-Type"] = "application/json"
        print(
            "{}\n{}\r\n{}\r\n\r\n{}\n{}".format(
                "=" * 66,
                prepared.method + " " + prepared.url,
                "\r\n".join("{}: {}".format(k, v) for k, v in prepared.headers.items()),
                prepared.body,
                "=" * 66,
            )
        )


def mongo_api_body_location(row, aux_iterable):
    zero_point = get_zero_point(aux_iterable, row["name"])  # This is a hack, shouldn't be here
    filters = get_filters(aux_iterable, row["name"])  # This is a hack, shouldn't be here
    period = get_period(aux_iterable, row["name"])  # This is a hack, shouldn't be here
    return {
        "tess": {
            "name": row["name"].strip(),
            "info_tess": {
                "zero_point": zero_point,
                "filters": filters,
                "period": period,
                "local_timezone": row["timezone"].strip() if row["timezone"] != "" else None,
            },
            "info_location": {
                "longitude": float(row["longitude"]) if row["longitude"] != "" else None,
                "latitude": float(row["latitude"]) if row["latitude"] != "" else None,
                "place": row["place"].strip() if row["place"] != "" else None,
                "town": row["town"].strip() if row["town"] != "" else None,
                "sub_region": row["sub_region"].strip() if row["sub_region"] != "" else None,
                "region": row["region"].strip() if row["region"] != "" else None,
                "country": row["country"].strip() if row["country"] != "" else None,
            },
        },
    }


def mongo_api_body_photometer(row, aux_iterable, create=False):
    local_timezone = (
        get_timezone(aux_iterable, row["name"]).strip() if not create else "Etc/UTC"
    )  # This is a hack, shouldn't be here
    zero_point = float(row["zero_point"]) if row["zero_point"] != "" else None
    filters = row["filters"].strip() if row["filters"] != "" else None
    period = int(row["period"]) if row["period"] != "" else None
    body = {
        "tess": {
            "name": row["name"].strip(),
            "mac": row["mac"].upper().strip(),
            "info_tess": {
                "zero_point": zero_point,
                "filters": filters,
                "local_timezone": local_timezone,
                "period": period,
            },
        },
    }
    if create:
        body["tess"]["info_location"] = DEFAULT_LOCATION
        body["tess"]["info_org"] = DEFAULT_ORG
        body["tess"]["info_img"] = DEFAULT_IMG
        body["tess"]["info_contact"] = DEFAULT_CONTACT
    return body


def mongo_api_body_organization(row):
    return {
        "tess": {
            "name": row["name"].strip(),
            "info_org": {
                "name": row["org_name"].strip() if row["org_name"] != "" else None,
                "web_url": row["org_web_url"].strip() if row["org_web_url"] != "" else None,
                "description": row["org_description"].strip()
                if row["org_description"] != ""
                else None,
                "logo_url": row["org_logo_url"].strip() if row["org_logo_url"] != "" else None,
                "email": row["org_email"].strip() if row["org_email"] != "" else None,
                "phone": row["org_phone"].strip() if row["org_phone"] != "" else None,
            },
        }
    }


def mongo_api_body_contact(row):
    return {
        "tess": {
            "name": row["name"].strip(),
            "info_contact": {
                "name": row["contact_name"].strip() if row["contact_name"] != "" else None,
                "mail": row["contact_mail"].strip() if row["contact_mail"] != "" else None,
                "phone": row["contact_phone"].strip() if row["contact_phone"] != "" else None,
            },
        }
    }


def mongo_api_body_all(row):
    body = {
        "tess": {
            "name": row["name"].strip(),
            "mac": row["mac"].upper().strip(),
            "info_tess": {
                "zero_point": float(row["zero_point"]) if row["zero_point"] != "" else None,
                "filters": row["filters"].strip() if row["filters"] != "" else None,
                "period": int(row["period"]) if row["period"] != "" else None,
                "local_timezone": row["timezone"].strip() if row["timezone"] != "" else None,
            },
            "info_location": {
                "longitude": float(row["longitude"]) if row["longitude"] != "" else None,
                "latitude": float(row["latitude"]) if row["latitude"] != "" else None,
                "place": row["place"].strip() if row["place"] != "" else None,
                "town": row["town"].strip() if row["town"] != "" else None,
                "sub_region": row["sub_region"].strip() if row["sub_region"] != "" else None,
                "region": row["region"].strip() if row["region"] != "" else None,
                "country": row["country"].strip() if row["country"] != "" else None,
            },
        },
    }
    body1 = body["tess"]
    body2 = mongo_api_body_organization(row)["tess"]
    body3 = mongo_api_body_contact(row)["tess"]
    combined = {**body1, **body2, **body3}
    body["tess"] = combined
    return body


def mongo_api_body_photometer(row, aux_iterable, create=False):
    local_timezone = (
        get_timezone(aux_iterable, row["name"]).strip() if not create else "Etc/UTC"
    )  # This is a hack, shouldn't be here
    zero_point = row.get("zero_point")
    zero_point = float(zero_point) if zero_point is not None and zero_point != "" else None
    filters = row.get("filters")
    filters = filters.strip() if filters is not None and filters != "" else None
    period = row.get("period")
    period = int(period) if period is not None and period != "" else None
    body = {
        "tess": {
            "name": row["name"].strip(),
            "mac": row["mac"].upper().strip(),
            "info_tess": {
                "zero_point": zero_point,
                "filters": filters,
                "local_timezone": local_timezone,
                "period": period,
            },
        },
    }
    if create:
        body["tess"]["info_location"] = DEFAULT_LOCATION
        body["tess"]["info_org"] = DEFAULT_ORG
        body["tess"]["info_img"] = DEFAULT_IMG
        body["tess"]["info_contact"] = DEFAULT_CONTACT
    return body


def mongo_get_all(url):
    """Correlates all entries with the missing mac information using just two HTTP requests"""
    names_list = mongo_api_get_names(url)
    details_list = mongo_api_get_details(url)
    if len(names_list) != len(details_list):
        raise ListLengthMismatchError(
            "length names_list (len=%d), length details_list (len=%d)"
            % (len(names_list), len(details_list))
        )
    zipped = zip(names_list, details_list)
    for item in zipped:
        if item[0]["name"] != item[1]["name"]:
            raise NamesMismatchError()
        assert item[0]["name"] == item[1]["name"]
        item[1]["mac"] = item[0].get("mac")
    return details_list


def mongo_flatten_location(row):
    new_row = dict()
    new_row["name"] = row["name"]
    info_location = row.get("info_location")
    if info_location:
        new_row["longitude"] = float(row["info_location"]["longitude"])
        new_row["latitude"] = float(row["info_location"]["latitude"])
        new_row["place"] = row["info_location"]["place"]
        new_row["town"] = row["info_location"].get("town")
        new_row["region"] = row["info_location"].get("region")
        new_row["sub_region"] = row["info_location"].get("sub_region")
        new_row["country"] = row["info_location"].get("country")
    else:
        new_row["longitude"] = None
        new_row["latitude"] = None
        new_row["place"] = None
        new_row["town"] = None
        new_row["region"] = None
        new_row["sub_region"] = None
        new_row["country"] = None
    tess = row.get("info_tess")
    if tess:
        new_row["timezone"] = row["info_tess"].get("local_timezone", "Etc/UTC")
    else:
        new_row["timezone"] = "Etc/UTC"
    return new_row


def mongo_flatten_photometer(row):
    new_row = dict()
    new_row["name"] = row["name"]
    new_row["mac"] = row.get("mac", None)
    tess = row.get("info_tess")
    if tess:
        new_row["zero_point"] = row["info_tess"].get("zero_point", None)
        new_row["filters"] = row["info_tess"].get("filters", None)
        period = row["info_tess"].get("period", None)
        new_row["period"] = int(period) if period is not None else None
    else:
        new_row["filters"] = None
        new_row["zero_point"] = None
        new_row["period"] = None
    return new_row


def mongo_flatten_organization(row):
    new_row = dict()
    new_row["name"] = row["name"]
    organization = row.get("info_org")
    if organization:
        new_row["org_name"] = row["info_org"].get("name")
        new_row["org_email"] = row["info_org"].get("email")
        new_row["org_description"] = row["info_org"].get("description")
        new_row["org_web_url"] = row["info_org"].get("web_url")
        new_row["org_logo_url"] = row["info_org"].get("logo_url")
        new_row["org_phone"] = row["info_org"].get("logo_phone")
    else:
        new_row["org_name"] = None
        new_row["org_email"] = None
        new_row["org_description"] = None
        new_row["org_web_url"] = None
        new_row["org_logo_url"] = None
        new_row["org_phone"] = None
    return new_row


def mongo_flatten_contact(row):
    new_row = dict()
    new_row["name"] = row["name"]
    contact = row.get("info_contact")
    if contact:
        new_row["contact_name"] = row["info_contact"].get("name")
        new_row["contact_mail"] = row["info_contact"].get("mail")
        new_row["contact_phone"] = row["info_contact"].get("phone")
    else:
        new_row["contact_name"] = None
        new_row["contact_mail"] = None
        new_row["contact_phone"] = None
    return new_row


def mongo_flatten_all(row):
    dict1 = mongo_flatten_photometer(row)
    dict2 = mongo_flatten_location(row)
    dict3 = mongo_flatten_organization(row)
    dict4 = mongo_flatten_contact(row)
    new_row = {**dict1, **dict2}
    new_row = {**new_row, **dict3}
    new_row = {**new_row, **dict4}
    return new_row


def add_old_columns(row):
    row["old_mac"] = row["mac"]
    row["old_zero_point"] = row["zero_point"]
    row["old_filters"] = row["filters"]
    row["comment"] = ""
    return row


def mongo_get_location_info(url):
    return list(map(mongo_flatten_location, mongo_get_all(url)))


def mongo_get_photometer_info(url):
    return list(map(add_old_columns, map(mongo_flatten_photometer, mongo_get_all(url))))


def mongo_get_organization_info(url):
    return list(map(mongo_flatten_organization, mongo_get_all(url)))


def mongo_get_contact_info(url):
    return list(map(mongo_flatten_contact, mongo_get_all(url)))


def mongo_get_all_info(url):
    return list(map(mongo_flatten_all, mongo_get_all(url)))


def remap_nominatim(row):
    new_row = dict()
    keys = (
        "place",
        "place_type",
        "town",
        "town_type",
        "sub_region",
        "sub_region_type",
        "region",
        "region_type",
        "country",
        "timezone",
        "zipcode",
    )
    for key in keys:
        new_row[f"nominatim_{key}"] = row[key]
    for key in set(row.keys()) - set(keys):
        new_row[key] = row[key]
    return new_row


def remap_tzfinder(row):
    new_row = dict()
    keys = ("timezone",)
    for key in keys:
        new_row[f"tzfinder_{key}"] = row[key]
    for key in set(row.keys()) - set(keys):
        new_row[key] = row[key]
    return new_row


def remap_mac(input_iterable):
    """When reading MACS from CSV files"""

    def _map_mac(item):
        try:
            new_mac = formatted_mac(item["mac"])
        except ValueError as e:
            log.error("when processing %s => %s", item["name"], e)
        log.debug("remapping MAC: %-17s -> %-17s", item["mac"], new_mac)
        item["mac"] = new_mac
        return item

    return list(map(_map_mac, input_iterable))


def merge_info(input_iterable, nominatim_iterable):
    output = list()
    for i in range(0, len(input_iterable)):
        row = {**input_iterable[i], **nominatim_iterable[i]}
        output.append(row)
    return output


def filter_by_names(iterable, names):
    def _filter_by_names(row):
        return row["name"] in names

    return list(filter(_filter_by_names, iterable))


def filter_by_name(iterable, name):
    return filter_by_names(iterable, [name])


def get_item(iterable, key, item):
    result = filter_by_name(iterable, key)
    assert len(result) > 0, f"get_item(key={key}, item={item})"
    if len(result) > 1:
        raise DuplicatesError("getting by key '%s' returned %d items" % (key, len(result)))
    return result[0][item]


def get_mac(iterable, name):
    return get_item(iterable, name, "mac")


def get_timezone(iterable, name):
    return get_item(iterable, name, "timezone")


def get_zero_point(iterable, name):
    zp = get_item(iterable, name, "zero_point")
    zp = float(zp) if zp is not None else None
    return zp


def get_filters(iterable, name):
    filters = get_item(iterable, name, "filters")
    filters = filters.strip() if filters is not None else None
    return filters


def get_period(iterable, name):
    period = get_item(iterable, name, "period")
    period = int(period) if period is not None else None
    return period


def do_list(url, path, names, header, mongo_get_func):
    mongo_input_list = mongo_get_func(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    if names:
        mongo_input_list = filter_by_names(mongo_input_list, names)
        log.info("filtered up to %d items", len(mongo_input_list))
    write_csv(path, header, mongo_input_list)


def do_update_location(url, path, delimiter, names, simulated):
    mongo_aux_list = mongo_get_all_info(url)
    mongo_output_list = read_csv(path, delimiter)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        mac = get_mac(mongo_aux_list, row["name"])
        log.info("Updating mongoDB with location info for item %s (%s)", row["name"], mac)
        body = mongo_api_body_location(row, mongo_aux_list)
        try:
            mongo_api_update(url, body, mac, simulated)
        except ValueError as e:
            log.error(e)
            log.warn("Ignoring update location info for item %s (%s)", row["name"], mac)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")


def do_update_photometer(url, path, delimiter, names, simulated):
    mongo_aux_list = mongo_get_all_info(url)
    mongo_output_list = read_csv(path, delimiter)
    mongo_output_list = remap_mac(mongo_output_list)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        oldmac = get_mac(mongo_aux_list, row["name"])
        body = mongo_api_body_photometer(row, mongo_aux_list)
        log.info("Updating MongoDB with photometer info for %s (%s)", row["name"], oldmac)
        try:
            mongo_api_update(url, body, oldmac, simulated)
            if oldmac != row["mac"]:
                log.info("Changing %s MAC: (%s) -> (%s)", row["name"], oldmac, row["mac"])
        except ValueError:
            log.warn("Ignoring update photometer info for item %s (%s)", row["name"], oldmac)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")


def do_create_photometer(url, path, delimiter, names, simulated):
    mongo_output_list = read_csv(path, delimiter)
    mongo_output_list = remap_mac(mongo_output_list)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        body = mongo_api_body_photometer(row, list(), create=True)
        log.info(
            "Creating new MongoDB entry with photometer info: %s (%s)", row["name"], row["mac"]
        )
        try:
            mongo_api_create(url, body, simulated)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")


def do_update_organization(url, path, delimiter, names, simulated):
    mongo_input_list = mongo_get_photometer_info(url)
    mongo_output_list = read_csv(path, delimiter)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        mac = get_mac(mongo_input_list, row["name"])
        log.info("Updating mongoDB with organization info for item %s (%s)", row["name"], mac)
        body = mongo_api_body_organization(row)
        try:
            mongo_api_update(url, body, mac, simulated)
        except ValueError:
            log.warn("Ignoring update organization info for item %s (%s)", row["name"], mac)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")


def do_update_contact(url, path, delimiter, names, simulated):
    mongo_input_list = mongo_get_photometer_info(url)
    mongo_output_list = read_csv(path, delimiter)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        mac = get_mac(mongo_input_list, row["name"])
        log.info("Updating mongoDB with contact info for item %s (%s)", row["name"], mac)
        body = mongo_api_body_contact(row)
        try:
            mongo_api_update(url, body, mac, simulated)
        except ValueError:
            log.warn("Ignoring update contact info for item %s (%s)", row["name"], mac)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")


def do_update_all(url, path, delimiter, names, simulated):
    mongo_input_list = mongo_get_photometer_info(url)
    mongo_output_list = read_csv(path, delimiter)
    mongo_output_list = remap_mac(mongo_output_list)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        mac = get_mac(mongo_input_list, row["name"])
        log.info("Updating mongoDB with all info for item %s (%s)", row["name"], mac)
        body = mongo_api_body_all(row)
        try:
            mongo_api_update(url, body, mac, simulated)
        except ValueError:
            log.warn("Ignoring update all info for item %s (%s)", row["name"], mac)
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY = {e.response.text}")


def do_create_all(url, path, delimiter, names, simulated):
    mongo_input_list = mongo_get_photometer_info(url)
    mongo_output_list = read_csv(path, delimiter)
    mongo_output_list = remap_mac(mongo_output_list)
    log.info("read %d items from CSV file %s", len(mongo_output_list), path)
    if names:
        mongo_output_list = filter_by_names(mongo_output_list, names)
        log.info("filtered up to %d items", len(mongo_output_list))
    for row in mongo_output_list:
        log.info(
            "Creating new MongoDB entry with photometer info: %s (%s)", row["name"], row["mac"]
        )
        body = mongo_api_body_all(row)
        try:
            mongo_api_create(url, body, simulated)
        except ValueError:
            log.warn("Ignoring create all info for item %s (%s)", row["name"], row["mac"])
        except requests.exceptions.HTTPError as e:
            log.error(f"{e}, RESP. BODY => {e.response.text}")


def do_check_mac_format(mongo_input_list):
    no_warnings = True
    for item in mongo_input_list:
        if is_mac(item["mac"]):
            if any(x.islower() for x in item["mac"].split(":")):
                no_warnings = False
                log.warn("%s MAC contains lower case letters: %s", item["name"], item["mac"])
            continue
        elif is_tess_mac(item["mac"]):
            no_warnings = False
            log.warn(
                "%s does not have a properly formatted MAC: %s -> %s",
                item["name"],
                item["mac"],
                formatted_mac(item["mac"]),
            )
            if any(x.islower() for x in item["mac"].split(":")):
                log.warn("%s MAC contains lower case letters: %s", item["name"], item["mac"])
        else:
            no_warnings = False
            log.error("%s does not even have not a proper MAC: %s", item["name"], item["mac"])
    if no_warnings:
        log.info("All MAC addresses in MongoDB are properly formatted")


def do_check_etc_utc(mongo_input_list):
    names = list()
    for item in mongo_input_list:
        tz = item["timezone"].upper()
        if tz.startswith("ETC") or tz.startswith("UTC"):
            names.append(item["name"])
            log.warn(
                "%s has a default timezone %s probably not matching its coordinates: (Lon. %s, Lat. %s)",
                item["name"],
                item["timezone"],
                item["longitude"],
                item["latitude"],
            )
    if not names:
        log.info("All timezones ok")
    else:
        log.info("Correct these: %s", " ".join(names))


def do_check_filter(mongo_input_list):
    names = list()
    for item in mongo_input_list:
        if item["filters"] is None:
            names.append(item["name"])
            log.warn("%s has no filter label", item["name"])
        elif item["filters"].upper() == "UV/IR-CUT":
            names.append(item["name"])
            log.warn("%s has an old filter label %s", item["name"], item["filters"])
    if not names:
        log.info("All filter strings ok")
    else:
        log.info("Correct these: %s", " ".join(names))


def do_check_zp(mongo_input_list):
    names = list()
    for item in mongo_input_list:
        if item["zero_point"] is None:
            names.append(item["name"])
            log.warn("%s has no zero point", item["name"])
        else:
            try:
                float(item["zero_point"])
            except:
                names.append(item["name"])
                log.warn("%s hasn't a numerc zero point %s", item["name"], item["zero_point"])
    if not names:
        log.info("All items have a zero point assigned")
    else:
        log.info("Correct these: %s", " ".join(names))


def do_check_location_metadata(mongo_input_list):
    names = list()
    for item in mongo_input_list:
        for key in ("place", "town", "sub_region", "region", "country", "timezone"):
            if item.get(key) is None:
                log.warn("%s has empty %s", item["name"], key)
                names.append(item["name"])
    if not names:
        log.info("All location metadata ok")
    else:
        log.info("Correct these: %s", " ".join(names))


def do_diff_all(url, input_file, delimiter, output_file_prefix):
    mongo_iterable = group_by_name(mongo_get_all_info(url))
    csv_iterable = group_by_name(read_csv(input_file, delimiter))

    keys_in_csv_file_not_in_mongo = in_A_not_in_B(csv_iterable, mongo_iterable)

    log.info("In CSV file, not in MongoDB => %d entries", len(keys_in_csv_file_not_in_mongo))
    csv_not_in_mongo = filter_and_flatten(csv_iterable, keys_in_csv_file_not_in_mongo)
    path_1 = os.path.join(
        os.path.dirname(input_file), f"{output_file_prefix}_in_file_not_in_mongo.csv"
    )
    write_csv(path_1, ALL_HEADER, csv_not_in_mongo)

    keys_in_mongo_not_in_csv_file = in_A_not_in_B(mongo_iterable, csv_iterable)
    log.info("In MongoDB file, not in CSV => %d entries", len(keys_in_mongo_not_in_csv_file))
    mongo_not_in_csv = filter_and_flatten(mongo_iterable, keys_in_mongo_not_in_csv_file)
    path_2 = os.path.join(
        os.path.dirname(input_file), f"{output_file_prefix}_in_mongo_not_in_file.csv"
    )
    write_csv(path_2, ALL_HEADER, csv_not_in_mongo)

    in_both_keys = common_A_B_items(mongo_iterable, csv_iterable)
    log.info("Common in MongoDB and in CSV file, => %d entries", len(in_both_keys))
    common_entries_mongo = filter_and_flatten(mongo_iterable, in_both_keys)
    common_entries_file = filter_and_flatten(csv_iterable, in_both_keys)

    path_3 = os.path.join(os.path.dirname(input_file), f"{output_file_prefix}_common_file.csv")
    write_csv(path_3, ALL_HEADER, csv_not_in_mongo)

    path_4 = os.path.join(os.path.dirname(input_file), f"{output_file_prefix}_common_mongo.csv")
    write_csv(path_4, ALL_HEADER, csv_not_in_mongo)


# ===================
# Module entry points
# ===================


def location(args):
    url = get_mongo_api_url()
    if args.list:
        do_list(url, args.file, args.names, LOCATION_HEADER, mongo_get_location_info)
    elif args.update:
        do_update_location(url, args.file, args.delimiter, args.names, simulated=False)
    elif args.sim_update:
        do_update_location(url, args.file, args.delimiter, args.names, simulated=True)
    elif args.nominatim:
        mongo_input_list = mongo_get_location_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if args.names:
            mongo_input_list = filter_by_names(mongo_input_list, args.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        log.info("including Nominatim geolocalization metadata")
        nominatim_list = geolocate(mongo_input_list)
        nominatim_list = list(map(remap_nominatim, nominatim_list))
        mongo_input_list = merge_info(mongo_input_list, nominatim_list)
        write_csv(args.file, NOMINATIM_HEADER, mongo_input_list)
    elif args.timezone:
        mongo_input_list = mongo_get_location_info(url)
        log.info("read %d items from MongoDB", len(mongo_input_list))
        if args.names:
            mongo_input_list = filter_by_names(mongo_input_list, args.names)
            log.info("filtered up to %d items", len(mongo_input_list))
        log.info("including TZFinder new metadata")
        timezone_list = timezone(mongo_input_list)
        timezone_list = list(map(remap_tzfinder, timezone_list))
        mongo_input_list = merge_info(mongo_input_list, timezone_list)
        write_csv(args.file, TZFINDER_HEADER, timezone_list)
    else:
        log.error("No valid input option to subcommand 'location'")


def photometer(args):
    url = get_mongo_api_url()
    if args.list:
        do_list(url, args.file, args.names, PHOTOMETER_HEADER, mongo_get_photometer_info)
    elif args.create:
        do_create_photometer(url, args.file, args.delimiter, args.names, simulated=False)
    elif args.sim_create:
        do_create_photometer(url, args.file, args.delimiter, args.names, simulated=True)
    elif args.update:
        do_update_photometer(url, args.file, args.delimiter, args.names, simulated=False)
    elif args.sim_update:
        do_update_photometer(url, args.file, args.delimiter, args.names, simulated=True)
    else:
        log.error("No valid input option to subcommand 'photometer'")


def organization(args):
    url = get_mongo_api_url()
    if args.list:
        do_list(url, args.file, args.names, ORGANIZATION_HEADER, mongo_get_organization_info)
    elif args.update:
        do_update_organization(url, args.file, args.delimiter, args.names, simulated=False)
    elif args.sim_update:
        do_update_organization(url, args.file, args.delimiter, args.names, simulated=True)
    else:
        log.error("No valid input option to subcommand 'organization'")


def contact(args):
    url = get_mongo_api_url()
    if args.list:
        do_list(url, args.file, args.names, CONTACT_HEADER, mongo_get_contact_info)
    elif args.update:
        do_update_contact(url, args.file, args.delimiter, args.names, simulated=False)
    elif args.sim_update:
        do_update_contact(url, args.file, args.delimiter, args.names, simulated=True)
    else:
        log.error("No valid input option to subcommand 'contact'")


def all_info(args):
    url = get_mongo_api_url()
    if args.list:
        do_list(url, args.file, args.names, ALL_HEADER, mongo_get_all_info)
    elif args.update:
        do_update_all(url, args.file, args.delimiter, args.names, simulated=False)
    elif args.sim_update:
        do_update_all(url, args.file, args.delimiter, args.names, simulated=True)
    elif args.create:
        do_create_all(url, args.file, args.delimiter, args.names, simulated=False)
    elif args.sim_create:
        do_create_all(url, args.file, args.delimiter, args.names, simulated=True)
    elif args.diff_file:
        log.info(
            "Check differences between MongoDB and a backup CSV file",
        )
        do_diff_all(url, args.diff_file, args.delimiter, args.file)
    else:
        log.error("No valid input option to subcommand 'all'")


def check(args):
    log.info(
        " ====================== ANALIZING DUPLICATES IN MONGODB METADATA ======================"
    )
    url = get_mongo_api_url()
    mongo_input_list = mongo_get_all_info(url)
    log.info("read %d items from MongoDB", len(mongo_input_list))
    if args.names:
        log.info("Check for duplicate photometer names")
        mongo_names = group_by_name(mongo_input_list)
        log_names(mongo_names)
    elif args.macs:
        log.info("Check for duplicate photometer MAC addresses")
        mongo_macs = group_by_mac(mongo_input_list)
        log_macs(mongo_macs)
    elif args.mac_format:
        log.info("Check for properly formatted MAC addresses")
        do_check_mac_format(mongo_input_list)
    elif args.places:
        log.info("Check for same place, different coordinates")
        mongo_places = group_by_place(mongo_input_list)
        log_places(mongo_places)
    elif args.coords:
        log.info("Check for same coordinates, different places")
        mongo_coords = group_by_coordinates(mongo_input_list)
        log_coordinates(mongo_coords)
    elif args.nearby:
        log.info("Check for nearby places in radius %0.0f meters", args.nearby)
        mongo_coords = group_by_coordinates(mongo_input_list)
        log_coordinates_nearby(mongo_coords, args.nearby)
    elif args.filter:
        log.info("Check for 'UV/IR-cut default filter")
        do_check_filter(mongo_input_list)
    elif args.utc:
        log.info("Check for 'ETC/UTC*' timezone")
        do_check_etc_utc(mongo_input_list)
    elif args.zero_point:
        log.info("Check for defined zero points")
        do_check_zp(mongo_input_list)
    elif args.location_metadata:
        log.info("Check for defined zero points")
        do_check_location_metadata(mongo_input_list)
    else:
        log.error("No valid input option to subcommand 'check'")


def add_args(parser):
    # -----------------------------------------
    # Create second level parsers for 'mongodb'
    # -----------------------------------------

    subparser = parser.add_subparsers(dest="command")

    mgloc = subparser.add_parser("location", help="MongoDB location metadata operations")
    mgloc.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Input (for update) / Output (for list) CSV file",
    )
    mgloc.add_argument(
        "-n",
        "--names",
        type=str,
        nargs="+",
        default=None,
        required=False,
        help="Optional names filter",
    )
    mgloc.add_argument(
        "--delimiter",
        type=str,
        default=";",
        help="Optional column delimiter for CSV I/O (semicolon by default)",
    )
    mgex1 = mgloc.add_mutually_exclusive_group(required=True)
    mgex1.add_argument("-l", "--list", action="store_true", help="List MongoDB location data")
    mgex1.add_argument(
        "-m", "--nominatim", action="store_true", help="List MongoDB location + Nominatim metadata"
    )
    mgex1.add_argument(
        "-t",
        "--timezone",
        action="store_true",
        help="List MongoDB location + Extended TZ info based on coordinates",
    )
    mgex1.add_argument(
        "-u", "--update", action="store_true", help="Update MongoDB location metadata"
    )
    mgex1.add_argument(
        "-s",
        "--sim-update",
        action="store_true",
        help="(simulated) Update MongoDB location metadata",
    )

    mgphot = subparser.add_parser("photometer", help="MongoDB photometer metadata operations")
    mgphot.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Input (for update) / Output (for list) CSV file",
    )
    mgphot.add_argument(
        "-n",
        "--names",
        type=str,
        nargs="+",
        default=None,
        required=False,
        help="Optional names filter",
    )
    mgphot.add_argument(
        "--delimiter",
        type=str,
        default=";",
        help="Optional column delimiter for CSV I/O (semicolon by default)",
    )
    mgex1 = mgphot.add_mutually_exclusive_group(required=True)
    mgex1.add_argument("-l", "--list", action="store_true", help="List MongoDB photometer data")
    mgex1.add_argument(
        "-u", "--update", action="store_true", help="Update MongoDB photometer metadata"
    )
    mgex1.add_argument(
        "-c", "--create", action="store_true", help="Create MongoDB photometer metadata"
    )
    mgex1.add_argument(
        "-x",
        "--sim-create",
        action="store_true",
        help="(simulated) Create MongoDB photometer metadata",
    )
    mgex1.add_argument(
        "-s",
        "--sim-update",
        action="store_true",
        help="(simulated) Update MongoDB photometer metadata",
    )
    mgphot.add_argument(
        "-m",
        "--mac",
        type=str,
        default=None,
        required=False,
        help="(Optional) old MAC, needed only to change MAC",
    )

    mgorg = subparser.add_parser("organization", help="MongoDB organiaztion metadata operations")
    mgorg.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Input (for update) / Output (for list) CSV file",
    )
    mgorg.add_argument(
        "-n",
        "--names",
        type=str,
        nargs="+",
        default=None,
        required=False,
        help="Optional names filter",
    )
    mgorg.add_argument(
        "--delimiter",
        type=str,
        default=";",
        help="Optional column delimiter for CSV I/O (semicolon by default)",
    )
    mgex1 = mgorg.add_mutually_exclusive_group(required=True)
    mgex1.add_argument(
        "-l", "--list", action="store_true", help="List MongoDB organization metadata"
    )
    mgex1.add_argument(
        "-u", "--update", action="store_true", help="Update MongoDB organization metadata"
    )
    mgex1.add_argument(
        "-s",
        "--sim-update",
        action="store_true",
        help="(simulated) Update MongoDB organization metadata",
    )

    mgcon = subparser.add_parser("contact", help="MongoDB contact metadata operations")
    mgcon.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Input (for update) / Output (for list) CSV file",
    )
    mgcon.add_argument(
        "-n",
        "--names",
        type=str,
        nargs="+",
        default=None,
        required=False,
        help="Optional names filter",
    )
    mgcon.add_argument(
        "--delimiter",
        type=str,
        default=";",
        help="Optional column delimiter for CSV I/O (semicolon by default)",
    )
    mgex1 = mgcon.add_mutually_exclusive_group(required=True)
    mgex1.add_argument("-l", "--list", action="store_true", help="List MongoDB contact metadata")
    mgex1.add_argument(
        "-u", "--update", action="store_true", help="Update MongoDB contact metadata"
    )
    mgex1.add_argument(
        "-s",
        "--sim-update",
        action="store_true",
        help="(simulated) Update MongoDB contact metadata",
    )

    mgall = subparser.add_parser("all", help="MongoDB all metadata operations")
    mgall.add_argument(
        "-f",
        "--file",
        type=str,
        required=True,
        help="Input (for update) / Output (for list) CSV file",
    )
    mgall.add_argument(
        "-n",
        "--names",
        type=str,
        nargs="+",
        default=None,
        required=False,
        help="Optional names filter",
    )
    mgall.add_argument(
        "--delimiter",
        type=str,
        default=";",
        help="Optional column delimiter for CSV I/O (semicolon by default)",
    )
    mgex1 = mgall.add_mutually_exclusive_group(required=True)
    mgex1.add_argument("-l", "--list", action="store_true", help="List all MongoDB metadata")
    mgex1.add_argument(
        "-d",
        "--diff-file",
        type=vfile,
        help="Diff between Mongo and a backup input CSV file. Generates 4 files.",
    )
    mgex1.add_argument("-u", "--update", action="store_true", help="Update all MongoDB metadata")
    mgex1.add_argument(
        "-c", "--create", action="store_true", help="Create MongoDB photometer metadata"
    )
    mgex1.add_argument(
        "-s", "--sim-update", action="store_true", help="(simulated) Update MongoDB all metadata"
    )
    mgex1.add_argument(
        "-x",
        "--sim-create",
        action="store_true",
        help="(simulated) Create MongoDB photometer metadata",
    )

    mgphck = subparser.add_parser("check", help="Various MongoDB metadata checks")
    mgphck.add_argument(
        "--delimiter",
        type=str,
        default=";",
        help="Optional column delimiter for CSV I/O (semicolon by default)",
    )
    mgex1 = mgphck.add_mutually_exclusive_group(required=True)
    mgex1.add_argument(
        "-n", "--names", action="store_true", help="Check for duplicate photometer names"
    )
    mgex1.add_argument("-m", "--macs", action="store_true", help="Check for duplicate MACs")
    mgex1.add_argument(
        "-a", "--mac-format", action="store_true", help="Check for properly formatted MACs"
    )
    mgex1.add_argument(
        "-p", "--places", action="store_true", help="Check same places, different coordinates"
    )
    mgex1.add_argument(
        "-c", "--coords", action="store_true", help="Check same coordinates, different places"
    )
    mgex1.add_argument(
        "-b", "--nearby", type=float, default=0, help="Check for nearby places, distance in meters"
    )
    mgex1.add_argument("-u", "--utc", action="store_true", help="Check for Etc/UTC* timezone")
    mgex1.add_argument(
        "-i", "--filter", action="store_true", help='Check for "UV/IR-cut" string in filters'
    )
    mgex1.add_argument(
        "-z", "--zero-point", action="store_true", help="Check for defined zero points"
    )
    mgex1.add_argument(
        "-lm",
        "--location-metadata",
        action="store_true",
        help="Check fornon empty location metadata",
    )


# ================
# MAIN ENTRY POINT
# ================

ENTRY_POINT = {
    "location": location,
    "photometer": photometer,
    "all": all_info,
    "contact": contact,
    "organization": organization,
    "check": check,
}


def mongo_db(args):
    func = ENTRY_POINT[args.command]
    func(args)


def main():
    execute(
        main_func=mongo_db,
        add_args_func=add_args,
        name=__name__,
        version=__version__,
        description="STARS4ALL MongoDB Utilities",
    )
