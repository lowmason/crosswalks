# -------------------------------------------------------------------------------------------------
# Imports and parameters
# -------------------------------------------------------------------------------------------------

from pathlib import Path
from typing import Dict, List, Literal

import polars as pl

BASE_PATH = Path(__file__).parent.parent.parent
DEFAULT_YEAR = 2023


# -------------------------------------------------------------------------------------------------
# Load geographic areas data
# -------------------------------------------------------------------------------------------------

geos_df = (
    pl
    .read_csv(
        f'{BASE_PATH}/data/geographic_codes.csv', 
        schema_overrides={
            'year': pl.Int64,
            'region': pl.Utf8,
            'division': pl.Utf8,
            'state_fips': pl.Utf8,
            'county_fips': pl.Utf8,
            'region_name': pl.Utf8,
            'division_name': pl.Utf8,
            'state_abbr': pl.Utf8,
            'state_name': pl.Utf8,
            'county_name': pl.Utf8,
            'cbsa_code': pl.Utf8,
            'msa_code': pl.Utf8,
            'csa_code': pl.Utf8,
            'cbsa_title': pl.Utf8,
            'msa_title': pl.Utf8,
            'csa_title': pl.Utf8,
            'metro': pl.Int64,
        }
    )
)

# -------------------------------------------------------------------------------------------------
# List available years
# -------------------------------------------------------------------------------------------------

def available_years(geo_df: pl.DataFrame = geos_df) -> List[int]:
    '''
    Get a list of available years in the geographic data.
    
    Args:
        geo_df: DataFrame with geographic data
        
    Returns:
        List of available years
    '''
    return geo_df.get_column('year').unique().sort().to_list()


# -------------------------------------------------------------------------------------------------
# Mapping geographic area from one key to another
# -------------------------------------------------------------------------------------------------

def area_mapping(
    from_area: Literal[
        'region', 'region_name',
        'division', 'division_name',
        'state_fips', 'state_abbr', 'state_name',
        'county_fips', 'county_name',
        'cbsa_code', 'cbsa_title',
        'msa_code', 'msa_title',
        'csa_code', 'csa_title',
        'metro'
    ], 
    to_area: Literal[
        'region', 'region_name',
        'division', 'division_name',
        'state_fips', 'state_abbr', 'state_name',
        'county_fips', 'county_name',
        'cbsa_code', 'cbsa_title',
        'msa_code', 'msa_title',
        'csa_code', 'csa_title',
        'metro'
    ], 
    year: int = DEFAULT_YEAR,
    geo_df: pl.DataFrame = geos_df
) -> Dict[str, str]:

    '''
    Create a mapping dictionary from one geographic field to another.
    
    Args:
        from_area: Source area (e.g., 'state_fips')
        to_area: Target area (e.g., 'region', 'division')
        year: Year for geographic definitions (default: 2023)
        geo_df: DataFrame with geographic data
        
    Returns:
        Dict mapping from_area values to to_area values
    '''

    return dict(
        geo_df
        .filter(pl.col('year') == year)
        .select(from_area, to_area)
        .sort(from_area, to_area)
        .unique(maintain_order=True)
        .iter_rows()
    )


# -------------------------------------------------------------------------------------------------
# Mapping geographic area id to name mapping
# -------------------------------------------------------------------------------------------------

def get_area(
    area: Literal['region', 'division', 'state', 'county', 'cbsa', 'msa', 'csa'],
    year: int = DEFAULT_YEAR,
    geo_df: pl.DataFrame = geos_df
) -> Dict[str, str]:

    '''
    Get a mapping of area codes to area names.
    
    Args:
        area: Type of area ('region', 'division', 'state', 'county', 'cbsa', 'msa', 'csa')
        year: Year for geographic definitions (default: 2023)
        geo_df: DataFrame with geographic data
        
    Returns:
        Dict mapping area codes to area names
    '''

    if area in ['region', 'division']:
        _id = area
        _title = area + '_name'
    elif area in ['state', 'county']:
        _id = area + '_fips'
        _title = area + '_name'
    else:
        # cbsa, msa, csa
        _id = area + '_code'
        _title = area + '_title'

    return dict(
        geo_df
        .filter(pl.col('year') == year)
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
        'county_fips', 'county_name',
        'cbsa_code', 'cbsa_title',
        'msa_code', 'msa_title',
        'csa_code', 'csa_title',
        'metro'
    ], 
    year: int = DEFAULT_YEAR,
    geo_df: pl.DataFrame = geos_df
) -> List[str]:

    '''
    Get a list of valid codes for a given area type.
    
    Args:
        area: Column name for area type (e.g., 'region', 'state_fips', 'cbsa_code')
        year: Year for geographic definitions (default: 2023)
        geo_df: DataFrame with geographic data
        
    Returns:
        List of valid area codes
    '''

    return list(
        geo_df
        .filter(pl.col('year') == year)
        .get_column(area)
        .unique()
        .sort()
        .to_list()
    )
