

TS_L_PARAMS = {
    "slope": 20.0,       # the 'm' or 'slope' parameter
    "intercept": -68.0   # the 'b' or 'y-intercept'
}

GRID_PARAMS = {
    # Grid boundary via longitude and latitude coordinates [minimum, maximum]
    "bounds": {
        "latitude": [32.75, 55.50],
        "longitude": [-135.25, -117.00]
    },
    # Grid the resolution in the x- and y-directions in units nmi
    "resolution" : {
        "x_distance": 25.0,
        "y_distance": 25.0
    },
    # EPSG coordinate projection (latitude/longitude)
    "projection": "epsg:4326"
}

INFO_DATAFRAME_MAPPING = {
    "time_stamp": "timestamp",
    "Latitude": "latitude",
    "Longitude": "longitude",
}
