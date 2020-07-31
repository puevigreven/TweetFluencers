import codecs
import json
import logging
import logging.config
import sys
import os
import pathlib


def get_logger():
    path = pathlib.Path(__file__).parent.absolute()

    config_file = path / "logging.json"
    with codecs.open(config_file, "r", encoding="utf-8") as fd:
        config = json.load(fd)

    # Set up proper logging. This one disables the previously configured loggers.
    logging.config.dictConfig(config["logging"])
    return logging

    # If we need to separate things, we can always create child loggers:
    # child = logging.getLogger().getChild("child")
    # child.warning("This is a WARNING message on the child logger.")

    # let's create an error. This will send an email
    # child.error("This is an ERROR message.")
