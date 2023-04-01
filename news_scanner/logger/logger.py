""" Logger configuration setup. """

import logging
from datetime import datetime
from pathlib import Path

_curr_date = datetime.now().strftime("%Y%m%d")
_curr_time = datetime.now().strftime("%I%M")
_log_path = f"./logs/{_curr_date}"
Path(_log_path).mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("SYSTEM_LOGS")
_handler = logging.FileHandler(_log_path + f"/{_curr_time}.log", mode="a")
_log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
_handler.setFormatter(_log_format)
logger.addHandler(_handler)
logger.setLevel(logging.INFO)


td_error_logger = logging.getLogger("TDA_ERROR_LOGGER")
_td_handler = logging.FileHandler(_log_path + f"/{_curr_time}_td_error.log", mode="a")
_td_handler.setFormatter(_log_format)
td_error_logger.addHandler(_td_handler)
td_error_logger.setLevel(logging.ERROR)
