# -*- coding: utf-8 -*-

# TESS UTILITY TO PERFORM SOME MAINTENANCE COMMANDS

# ----------------------------------------------------------------------
# Copyright (c) 2014 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

# --------------------
# System wide imports
# -------------------

# ---------------------
# Third party libraries
# ---------------------

import validators


# --------------
# local imports
# -------------

# ----------------
# Module constants
# ----------------

# ----------------
# package constants
# ----------------


# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------


def url(string):
    if not validators.url(string):
        raise ValueError("Invalid URL: %s" % string)
    return string


def formatted_mac(mac):
    """'Corrects TESS-W MAC strings to be properly formatted"""
    try:
        corrected_mac = ":".join(f"{int(x, 16):02X}" for x in mac.split(":"))
    except ValueError:
        raise ValueError("Invalid MAC: %s" % mac)
    except AttributeError:
        raise ValueError("Invalid MAC: %s" % mac)
    return corrected_mac


def is_tess_mac(mac):
    """TESS-W MAC address do not contain with padding 0s"""
    mac_list = mac.split(":")
    result = True
    for x in mac_list:
        try:
            int(x, 16)
        except Exception:
            result = False
            break
    return result and len(mac_list) == 6


def is_mac(mac):
    """Strict MAC address check"""
    return is_tess_mac(mac) and len(mac) == 17


# ==============
# DATABASE STUFF
# ==============
