# -------------------------------------------------------------------------------------------------
# Imports and parameters
# -------------------------------------------------------------------------------------------------

from pathlib import Path
from typing import Dict, List, Literal

import polars as pl

BASE_PATH = Path(__file__).parent.parent.parent.parent


# -------------------------------------------------------------------------------------------------
# Load geographic areas data
# -------------------------------------------------------------------------------------------------

geos_df = (
    pl
    .read_csv(
        f'{BASE_PATH}/data/reference/geo_areas.csv', 
        schema_overrides={
            'region': pl.Utf8,
            'division': pl.Utf8,
            'state_fips': pl.Utf8,
            'county_fips': pl.Utf8,
            'region_name': pl.Utf8,
            'division_name': pl.Utf8,
            'state_abbr': pl.Utf8,
            'state_name': pl.Utf8,
            'county_name': pl.Utf8,
        }
    )
)

# -------------------------------------------------------------------------------------------------
# Mapping geographic area from one key to another
# -------------------------------------------------------------------------------------------------

def area_mapping(
    from_area: Literal[
        'region', 'region_name',
        'division', 'division_name',
        'state_fips', 'state_abbr', 'state_name',
        'county_fips', 'county_name'
    ], 
    to_area: Literal[
        'region', 'region_name',
        'division', 'division_name',
        'state_fips', 'state_abbr', 'state_name',
        'county_fips', 'county_name'
    ], 
    geo_df: pl.DataFrame = geos_df
) -> Dict[str, str]:

    '''
    Create a mapping dictionary from one geographic field to another.
    
    Args:
        from_area: Source area (e.g., 'state_fips')
        to_area: Target area (e.g., 'region', 'division')
        path: Path to the geo_areas.csv file
        
    Returns:
        Dict mapping from_area values to to_area values
    '''

    return dict(
        geo_df
        .select(from_area, to_area)
        .sort(from_area, to_area)
        .unique(maintain_order=True)
        .iter_rows()
    )


# -------------------------------------------------------------------------------------------------
# Mapping geographic area id to name mapping
# -------------------------------------------------------------------------------------------------

def get_area(
    area: Literal['region', 'division', 'state', 'county'],
    geo_df: pl.DataFrame = geos_df
) -> Dict[str, str]:

    '''
    Get a mapping of area codes to area names.
    
    Args:
        area_type: Type of area ('region', 'division', 'state_fips')
        path: Path to the geo_areas.csv file
        
    Returns:
        Dict mapping area codes to area names
    '''

    _id = area if area in ['region', 'division'] else area + '_fips'
    _title = area + '_name'

    return dict(
        geo_df
        .select(_id, _title)
        .sort(_id, _title)
        .unique(maintain_order=True)
        .iter_rows()
    )


# -------------------------------------------------------------------------------------------------
# List valid area codes and titles
# -------------------------------------------------------------------------------------------------

def valid_area(area: Literal[
        'region', 'region_name',
        'division', 'division_name',
        'state_fips', 'state_abbr', 'state_name',
        'county_fips', 'county_name'
    ], 
    geo_df: pl.DataFrame = geos_df
) -> List[str]:

    '''
    Get a list of valid codes for a given area type.
    
    Args:
        area_type: Type of area ('region', 'division', 'state_fips', 'county_fips')
        path: Path to the geo_areas.csv file
        
    Returns:
        List of valid area codes
    '''

    return list(
        geo_df
        .get_column(area)
        .unique()
        .sort()
        .to_list()
    )
