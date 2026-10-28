# ----------------------------------------------------------------------
# Copyright (c) 2020
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

import math
import time
import logging
import itertools
import collections
import functools

# -------------------
# Third party imports
# -------------------

from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import decouple

# --------------
# local imports
# -------------

# ----------------
# Module constants
# ----------------

EARTH_RADIUS = 6371009.0  # in meters

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger(__name__)

# -------------------------
# Module auxiliar functions
# -------------------------


def common_A_B_items(map_A, map_B):
    return set(map_A.keys()).intersection(set(map_B.keys()))


def in_A_not_in_B(map_A, map_B):
    return set(map_A.keys()) - set(map_B.keys())


def get_mongo_api_url():
    return decouple.config("STARS4ALL_API")


def get_mongo_api_key():
    return decouple.config("STARS4ALL_API_KEY")


def get_zptess_connection_string():
    return decouple.config("ZPTESS_URL")


def get_tessdb_connection_string():
    return decouple.config("TESSDB_URL")


def get_idadb_connection_string():
    return decouple.config("IDADB_URL")


def remap_location(geolocator, tzfinder, row):
    time.sleep(1)  # Inserts a one seconds delay
    location = geolocator.reverse(f"{row['latitude']}, {row['longitude']}", language="en")
    if location is None:
        log.error(
            "Nominatim didn't find a location for %s (lon=%s, lat=%s)",
            row["name"],
            row["longitude"],
            row["latitude"],
        )
        return None
    metadata = location.raw["address"]
    out_row = dict()
    out_row["name"] = row["name"]  # Photometer name (this may go away ....)
    out_row["latitude"] = row["latitude"]
    out_row["longitude"] = row["longitude"]
    found = False
    for place_type in (
        "leisure",
        "amenity",
        "tourism",
        "building",
        "road",
        "hamlet",
    ):
        try:
            out_row["place"] = metadata[place_type]
        except KeyError:
            continue
        else:
            found = True
            if place_type == "road" and metadata.get("house_number"):
                out_row["place"] = metadata[place_type] + ", " + metadata["house_number"]
                out_row["place_type"] = "road + house_number"
            else:
                out_row["place_type"] = place_type
            break
    if found:
        log.info(
            "proposal for %s: '%s' (%s)  as Nominatim place name to '%s'",
            row["name"],
            metadata[place_type],
            place_type,
            row["place"],
        )
    else:
        out_row["place"] = None
        out_row["place_type"] = None
        log.warn("still without a valid Nominatim place name to suggest for '%s'", row["name"])

    for location_type in ("village", "town", "city", "municipality"):
        try:
            out_row["town"] = metadata[location_type]
            out_row["town_type"] = location_type
        except KeyError:
            out_row["town"] = None
            out_row["town_type"] = None
            continue
        else:
            break
    for province_type in ("state_district", "province"):
        try:
            out_row["sub_region"] = metadata[province_type]
            out_row["sub_region_type"] = province_type
        except KeyError:
            out_row["sub_region"] = None
            out_row["sub_region_type"] = None
            continue
        else:
            break
    out_row["region"] = metadata.get("state", None)
    out_row["region_type"] = "state"
    out_row["zipcode"] = metadata.get("postcode", None)
    out_row["country"] = metadata.get("country", None)
    out_row["timezone"] = tzfinder.timezone_at(lng=row["longitude"], lat=row["latitude"])
    if row["timezone"] != row["timezone"]:
        log.info("Proposal new timezone: %s -> %s", row["timezone"], out_row["timezone"])
    return out_row


def remap_timezone(tzfinder, row):
    out_row = dict(row)
    out_row["timezone"] = tzfinder.timezone_at(lng=row["longitude"], lat=row["latitude"])
    if row["timezone"] != row["timezone"]:
        log.info("Proposal new timezone: %s -> %s", row["timezone"], out_row["timezone"])
    return out_row


def geolocate(iterable):
    geolocator = Nominatim(user_agent="STARS4ALL project")
    RateLimiter(geolocator.geocode, min_delay_seconds=2)
    tzfinder = TimezoneFinder()
    # remap_location = _make_remap_location(geolocator, tzfinder)
    return list(map(functools.partial(remap_location, geolocator, tzfinder), iterable))


def timezone(iterable):
    tzfinder = TimezoneFinder()
    return list(map(functools.partial(remap_timezone, tzfinder), iterable))


def group_by(iterable, key):
    result = collections.defaultdict(list)
    for row in iterable:
        if row is not None:
            result[row[key]].append(row)
        else:
            log.warn("Skiping None row")
    log.info(
        "From %d entries, we have extracted %d different %s(s)",
        len(iterable),
        len(result.keys()),
        key,
    )
    for k, v in result.items():
        log.debug("%s %s has %d values", key, k, len(v))
    return result


def filter_out_multidict(multidict):
    result = {k: v for k, v in multidict.items() if len(v) == 1}
    log.info(
        "From multidict with %d entries, we have fitered %d entries out",
        len(multidict),
        len(multidict) - len(result),
    )
    return result


def ungroup_from(grouped_iterable, keys):
    return tuple(item for key in keys for item in grouped_iterable[key])


# ----------------------
# Photometers names check
# ----------------------


def group_by_name(iterable):
    return group_by(iterable, "name")


def log_names(names_iterable):
    for name, rows in names_iterable.items():
        if len(rows) > 1:
            log.warn(
                "Photometer %s has %d places: %s", name, len(rows), [row["place"] for row in rows]
            )
            log.warn(
                "Photometer %s has %d coordinates: %s",
                name,
                len(rows),
                [(row["longitude"], row["latitude"]) for row in rows],
            )


# ----------------------
# Photometers MACs check
# ----------------------


def group_by_mac(iterable, column_name="mac"):
    return group_by(iterable, column_name)


def log_macs(macs_iterable):
    for mac, rows in macs_iterable.items():
        if len(rows) > 1:
            log.warn(
                "MAC %s has %d photometer names: %s", mac, len(rows), [row["name"] for row in rows]
            )


# ------------------------
# Photometers Places check
# ------------------------


def group_by_place(iterable):
    return group_by(iterable, "place")


def log_places(places_iterable):
    for place, rows in places_iterable.items():
        if place is None:
            log.warn("No place defined for '%s'", rows[0]["name"])
        elif len(place.lstrip()) != len(place):
            log.warn("Place '%s' has leading spaces", place)
        elif len(place.rstrip()) != len(place):
            log.warn("Place '%s' has trailing spaces", place)
        if len(rows) > 1:
            log.debug(
                "Place %s has %d photometers: %s", place, len(rows), [row["name"] for row in rows]
            )
            check_place_same_coords(place, rows)


def check_place_same_coords(place, rows):
    """Check for coordinates consistency among phothometers deployed on the same 'place' name"""
    result = False
    longitudes = set(phot["longitude"] for phot in rows)
    latitudes = set(phot["latitude"] for phot in rows)
    if len(longitudes) > 1:
        result = True
        log.warn(
            "Place %s has different %d longitudes. %s -> %s",
            place,
            len(longitudes),
            [phot["longitude"] for phot in rows],
            [phot["name"] for phot in rows],
        )
    if len(latitudes) > 1:
        result = True
        log.warn(
            "Place %s has different %d latitudes. %s -> %s",
            place,
            len(latitudes),
            [phot["latitude"] for phot in rows],
            [phot["name"] for phot in rows],
        )
    return result


# ------------------------
# Photometers Coords check
# ------------------------


def group_by_coordinates(iterable):
    coords = collections.defaultdict(list)
    for row in iterable:
        if row is None:
            log.warn("Skipping None row")
            continue
        if row["longitude"] is None or row["latitude"] is None:
            log.warn("Skipping null coordinates: %s", row)
            continue
        coords[(row["longitude"], row["latitude"])].append(row)
    log.info(
        "From %d entries, we have extracted %d different coordinates",
        len(iterable),
        len(coords.keys()),
    )
    return coords


def log_coordinates(coords_iterable):
    """Check for coordinates consistency among phothometers deployed on the same 'place' name"""
    result = list()
    for coords, rows in coords_iterable.items():
        names = [row["name"] for row in rows]
        if None in coords:
            log.error("entry %s with no coordinates: %s", rows[0]["name"], coords)
        if len(rows) > 1 and all(row["name"] == rows[0]["name"] for row in rows):
            result.extend(names)
            log.error("Coordinates %s has %d duplicated photometers: %s", coords, len(rows), names)
        if len(rows) > 1 and not all(row["place"] == rows[0]["place"] for row in rows):
            result.extend(names)
            log.error(
                "Coordinates %s has different place names: %s for %s",
                coords,
                [row["place"] for row in rows],
                names,
            )
        if len(rows) > 1 and not all(row["town"] == rows[0]["town"] for row in rows):
            result.extend(names)
            log.error(
                "Coordinates %s has different town names: %s for %s",
                coords,
                [row["town"] for row in rows],
                names,
            )
        if len(rows) > 1 and not all(row["sub_region"] == rows[0]["sub_region"] for row in rows):
            result.extend(names)
            log.error(
                "Coordinates %s has different sub_region names: %s for %s",
                coords,
                [row["sub_region"] for row in rows],
                names,
            )
        if len(rows) > 1 and not all(row["region"] == rows[0]["region"] for row in rows):
            result.extend(names)
            log.error(
                "Coordinates %s has different region names: %s for %s",
                coords,
                [row["region"] for row in rows],
                names,
            )
        if len(rows) > 1 and not all(row["country"] == rows[0]["country"] for row in rows):
            result.extend(names)
            log.error(
                "Coordinates %s has different region names: %s for %s",
                coords,
                [row["country"] for row in rows],
                names,
            )
        if len(rows) > 1 and not all(row["timezone"] == rows[0]["timezone"] for row in rows):
            result.extend(names)
            log.error(
                "Coordinates %s has different timezone names: %s for %s",
                coords,
                [row["timezone"] for row in rows],
                names,
            )
    log.info(
        "Photometers to fix with inconsistencies in location metadata: (%d) %s",
        len(set(result)),
        " ".join(set(result)),
    )


def log_coordinates_nearby(coords_iterable, limit):
    """Check for possibly duplicates nearby coordinates/places"""
    coords_seq = tuple(coords_iterable.keys())
    coords_seq = tuple(filter(lambda x: x[0] is not None and x[1] is not None, coords_seq))
    coord_pairs = tuple(itertools.combinations(coords_seq, 2))
    for pair in coord_pairs:
        d = distance(pair[0], pair[1])
        if d <= limit:
            place_a = coords_iterable[pair[0]][0]["place"]
            place_b = coords_iterable[pair[1]][0]["place"]
            name_a = coords_iterable[pair[0]][0]["name"]
            name_b = coords_iterable[pair[1]][0]["name"]
            log.warn(
                "Place 1 (%s): '%s' %s vs Place 2 (%s): '%s' %s [%d meters]",
                name_a,
                place_a,
                pair[0],
                name_b,
                place_b,
                pair[1],
                d,
            )


def filter_selected_keys(dictionary, keys):
    return {key: dictionary[key] for key in keys}


def filter_and_flatten(iterable, keys=None):
    """Filter and flaten list created by by_xxx() filters
    Useful to dump in CSV multy-entry iterables, one entry per row
    """
    if keys is None:
        result = [item for k, v in iterable.items() for item in v]
    else:
        result = [item for k, v in iterable.items() for item in v if k in keys]
    return result


def distance(coords_A, coords_B):
    """
    Compute approximate geographical distance (arc) [meters] between two points on Earth
    Coods_A and Coords_B are tuples (longitude, latitude)
    Accurate for small distances only
    """
    longitude_A = coords_A[0]
    longitude_B = coords_B[0]
    latitude_A = coords_A[1]
    latitude_B = coords_B[1]
    try:
        delta_long = math.radians(longitude_A - longitude_B)
        delta_lat = math.radians(latitude_A - latitude_B)
        mean_lat = math.radians((latitude_A + latitude_B) / 2)
        result = round(
            EARTH_RADIUS * math.sqrt(delta_lat**2 + (math.cos(mean_lat) * delta_long) ** 2), 0
        )
    except TypeError:
        result = None
    return result
