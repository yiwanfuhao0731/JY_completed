
from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options

from panormus.config.settings import CACHE_DATA_DIR, CACHE_LOCK_DIR
from panormus.utils.simple_func_decorators import docstring_parameter

region_dict = {
    'mem_1m': {'type': 'memory', 'expire': 60},
    'mem_5m': {'type': 'memory', 'expire': 60 * 5},
    'mem_1h': {'type': 'memory', 'expire': 60 * 60},
    'mem_5h': {'type': 'memory', 'expire': 60 * 60 * 5},
    'mem_10h': {'type': 'memory', 'expire': 60 * 60 * 10},
    'disk_24h': {'type': 'file', 'expire': 60 * 60 * 24},
    'disk_48h': {'type': 'file', 'expire': 60 * 60 * 48},
    'disk': {'type': 'file', 'expire': None},
}
cache_opts = {
    'cache.data_dir': CACHE_DATA_DIR,
    'cache.lock_dir': CACHE_LOCK_DIR,
    'cache.regions': ', '.join(region_dict.keys()),
}
for region in region_dict:
    setting_dict = region_dict[region]
for setting in setting_dict:
    cache_opts['cache.' + region + '.' + setting] = setting_dict[setting]

cache = CacheManager(**parse_cache_config_options(cache_opts))


def key_from_func_args(*args, **kwargs):
    """
    Standardized method for determining a cache key from function args and kwargs.

    """
    return ' '.join((str(arg) for arg in args)) + \
           '|' + \
           ' '.join((str(k) + ':' + str(kwargs[k]) for k in kwargs))


@docstring_parameter(region_dict.keys())
def cache_put(name, region, value, *args, **kwargs):
    """
    Caches value to a given region. Uses name, args and kwargs so you can spoof a cache-decorated function. \
    This is indented for bulk loading caches.

    :param str name: namespace to prevent collisions. Suggested to use func_name or class_name.func_name.
    :param str region: determines cache settings using pre-defined "regions". Defined regions are: {0}
    :param value: thing to cache
    :param args: additional args for spoofing a decorated cache
    :param kwargs: additional kwargs for spoofing a decorated cache
    """
    key = key_from_func_args(*args, **kwargs)
    name_region = cache.get_cache_region(name=name, region=region)
    name_region.put(key, value)


@docstring_parameter(region_dict.keys())
def clear_cache(name, region):
    """
    Clear all keys for a given name and region

    :param str name: namespace to prevent collisions. Suggested to use func_name or class_name.func_name.
    :param str region: determines cache settings using pre-defined "regions". Defined regions are: {0}
    """
    name_region = cache.get_cache_region(name=name, region=region)
    name_region.clear()


class cache_response:
    """
    Decorator for caching. Region specifies caching strategy. \
    Name is appended to caching key so you can keep caches unique. \
    When calling your decorated function, values of args and kwargs values are also used in caching key.

    """

    @docstring_parameter(region_dict.keys())
    def __init__(self, name, region, skip_first_arg=False):
        """
        Region and name are passed through to underlying beaker.cache.

        :param str name: namespace to prevent collisions. Suggested to use func_name or class_name.func_name.
        :param str region: determines cache settings using pre-defined "regions". Defined regions are: {0}
        :param bool skip_first_arg: set to true when decorating a class method to avoid using str(self) in cache key
        """
        self.region = region
        self.name = name
        self.skip_first_arg = skip_first_arg
        self.cache = cache.get_cache_region(name=name, region=region)

    def __call__(self, f):
        """
        If there are decorator arguments, __call__() is only called \
        once, as part of the decoration process! You can only give \
        it a single argument, which is the function object.
        """

        def wrapped_f(*args, **kwargs):
            '''
            Arguments passed to f are received here. This wrapped function replaces f.
            '''
            key = key_from_func_args(*args[1:], **kwargs) if self.skip_first_arg else \
                key_from_func_args(*args, **kwargs)

            def create_func():
                '''
                The beaker cache createfunc must take no arguments. So we use scope to fix the parameter values \
                within a function with no arguments.

                :return: f with its args frozen so we can call it with no args
                '''
                return f(*args, **kwargs)

            # perform a cache git with a createfunc for fallback.
            return self.cache.get(key=key, createfunc=create_func)

        wrapped_f.__doc__ = f.__doc__
        return wrapped_f

