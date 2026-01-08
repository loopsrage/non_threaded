import functools
import os

import yaml
import json

from lib.containers.container import build_container_tree, Container


def read_settings(settings) -> Container:
    """
    Attempts to read settings as json, if TypeError is raised attempt yaml.safe_load
    raise Exception if neither succeeds
    :param settings:
    :return:
    """
    data = None
    try:
        data = json.loads(settings)
    except (json.decoder.JSONDecodeError, TypeError):
        pass

    if data is None:
        try:
            data = yaml.safe_load(settings)
        except (yaml.YAMLError, TypeError):
            raise Exception('Invalid YAML/JSON')

    if data is None:
        raise Exception('Invalid YAML/JSON')

    return build_container_tree(start=data)

@functools.cache
def load_settings() -> Container:
    """
    ENV_FILE environment variable read and used to open either a json or yaml file
    to be used in read_settings()
    :return:
    """
    path = os.getenv("ENV_FILE")
    try:
        with open(path, "r") as settings_file:
            return read_settings(settings_file.read())
    except FileNotFoundError:
        raise Exception(f"FATAL ERROR: Configuration file not found at '{path}'")
    except Exception as e:
        raise Exception(f"FATAL ERROR during registry initialization: {e}")

@functools.cache
def enabled(feature_name: str = None) -> bool:
    """Returns true if Feature.Enabled"""
    settings: Container = load_settings()
    is_enabled: bool = settings.read_primitive_value(path=feature_name + ".Enabled")
    return is_enabled is not None and is_enabled

@functools.cache
def setting(feature_name: str = None, setting_name: str = None):
    """
    load_settings() then read from feature_name.name
    :param feature_name:
    :param setting_name:
    :return:
    """
    settings: Container = load_settings()
    return settings.read_primitive_value(path=feature_name + "." + setting_name)

@functools.cache
def global_setting(name: str):
    """
    load_settings() then read from Global.name
    :param name:
    :return:
    """
    settings: Container = load_settings()
    return settings.read_primitive_value(path="Global" + "." + name)

def enabled_flag(feature_name: str):
    """
    Returns None if not Feature.Enabled, or returns unmodified if Feature.Enabled
    :param feature_name:
    :return:
    """
    def decorator(func):

        if not enabled(feature_name=feature_name):

            @functools.wraps(func)
            def disabled_wrapper(*args, **kwargs):
                print(f"Skipping {func.__name__}: {feature_name} is disabled.")
                return None
            return disabled_wrapper

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator