from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("tessdb-tools")
except PackageNotFoundError:
    __version__ = "0.0.0+dev"
