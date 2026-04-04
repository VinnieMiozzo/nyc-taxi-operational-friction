"""NYC Mobility Friction.

Portfolio project that combines NYC TLC taxi trips and 311 service requests
to identify mobility friction hotspots and prioritize urban interventions.
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("nyc-mobility-friction")
except PackageNotFoundError:
    # fallback when running from source without installation
    __version__ = "0.1.0-dev"

__all__ = ["__version__"]
