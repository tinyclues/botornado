from connection import AsyncSNSConnection
from boto.regioninfo import RegionInfo, get_regions

import boto.sns


def regions():
    __doc__ = boto.sns.regions.__doc__
    return get_regions('sns', connection_cls=AsyncSNSConnection)


def connect_to_region(region_name, **kw_params):
    __doc__ = boto.sns.connect_to_region.__doc__
    for region in regions():
        if region.name == region_name:
            return region.connect(**kw_params)
    return None
