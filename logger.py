import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
app_log = logging.getLogger("trend-stock-scanner")
