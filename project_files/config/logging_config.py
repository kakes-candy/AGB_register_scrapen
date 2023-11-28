import logging
from logging import config
from utils import logfileNamer

logroot = "./logs"
logmail_ontvangers = ["n.kakes@hsk.nl"]

filename = logfileNamer(path=logroot, basename="_Feedex_datadump")


log_config = {
    "version": 1,
    "root": {"handlers": ["console", "file"], "level": "DEBUG"},
    "handlers": {
        "console": {
            "formatter": "std_out",
            "class": "logging.StreamHandler",
            "level": "DEBUG",
        },
        "file": {
            "formatter": "std_out",
            "class": "logging.FileHandler",
            "level": "DEBUG",
            "filename": filename,
        },
    },
    "formatters": {
        "std_out": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - Module: %(module)s - Function: %(funcName)s - Regel: %(lineno)d \nLog : %(message)s",
            "datefmt": "%d-%m-%Y %I:%M:%S",
        }
    },
}
