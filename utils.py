"""
Common utility scripts
"""

import os
import yaml

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
config: dict
initialised = False

stream = open(this_dir + "config.yaml", 'r')
config = dict(yaml.load(stream, Loader=yaml.Loader))


def stringify_hour(hour: int) -> str:

    hour = int(hour)
    return f"H0{hour}" if hour<10 else f"H{hour}"


def stringify_day(day: int) -> str:

    day = int(day)
    return f"D00{day}" if day<10 else f"D0{day}" if day<100 else f"D{day}"


def destringify_day(day: str) -> str:

    return int(day[1:])