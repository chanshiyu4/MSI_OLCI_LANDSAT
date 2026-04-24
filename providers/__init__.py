from .cdse_provider import search_cdse_scenes
from .usgs_provider import search_usgs_scenes

PROVIDER_SEARCHERS = {
    "cdse": search_cdse_scenes,
    "usgs": search_usgs_scenes,
}
