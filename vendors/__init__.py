# vendors/__init__.py

import asyncio
from typing import List, Dict, Any

import httpx
from bs4 import BeautifulSoup
from serpapi import GoogleSearch

# Import the worker functions from their respective modules
from . import itexams
from . import allfreedumps

# A mapping to easily access worker functions by site name
WORKER_MAP = {
    "itexams": itexams.worker_itexams,
    "allfreedumps": allfreedumps.worker_allfreedumps,
}

# A mapping for discovery functions, making the main script cleaner
DISCOVERY_MAP = {
    "itexams": itexams.discover_itexams_tasks,
    "allfreedumps": allfreedumps.discover_allfreedumps_tasks,
}
