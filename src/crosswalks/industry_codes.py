# -------------------------------------------------------------------------------------------------
# Imports and parameters
# -------------------------------------------------------------------------------------------------

from pathlib import Path
from typing import Dict, List, Literal

import polars as pl

BASE_PATH = Path(__file__).parent.parent.parent
DEFAULT_YEAR = 2023
DEFAULT_SURVEY = 'ces'


# -------------------------------------------------------------------------------------------------
# Load industry codes data
# -------------------------------------------------------------------------------------------------

industries_df = (
    pl
    .read_csv(
        f'{BASE_PATH}/data/industry_codes.csv', 
        schema_overrides={
            'year': pl.Int64,
            'ces': pl.Boolean,
            'bed': pl.Boolean,
            'qcew': pl.Boolean,
            'domain': pl.Utf8,
            'supersector': pl.Utf8,
            'sector': pl.Utf8,
            'subsector': pl.Utf8,
            'industry_group': pl.Utf8,
            'naics_industry': pl.Utf8,
            'detailed_industry': pl.Utf8,
            'domain_name': pl.Utf8,
            'supersector_name': pl.Utf8,
            'sector_name': pl.Utf8,
            'subsector_name': pl.Utf8,
            'industry_group_name': pl.Utf8,
            'naics_industry_name': pl.Utf8,
            'detailed_industry_name': pl.Utf8,
        }
    )
)


# -------------------------------------------------------------------------------------------------
# List available years
# -------------------------------------------------------------------------------------------------

def available_years(
    survey: Literal['ces', 'bed', 'qcew'] = DEFAULT_SURVEY,
    industry_df: pl.DataFrame = industries_df
) -> List[int]:
    '''
    Get a list of available years in the industry data for a given survey.
    
    Args:
        survey: Survey type ('ces', 'bed', 'qcew') (default: 'ces')
        industry_df: DataFrame with industry data
        
    Returns:
        List of available years
    '''
    return (
        industry_df
        .filter(pl.col(survey) == True)
        .get_column('year')
        .unique()
        .sort()
        .to_list()
    )


# -------------------------------------------------------------------------------------------------
# Mapping industry code from one key to another
# -------------------------------------------------------------------------------------------------

def industry_mapping(
    from_industry: Literal[
        'domain', 'domain_name',
        'supersector', 'supersector_name',
        'sector', 'sector_name',
        'subsector', 'subsector_name',
        'industry_group', 'industry_group_name',
        'naics_industry', 'naics_industry_name',
        'detailed_industry', 'detailed_industry_name'
    ], 
    to_industry: Literal[
        'domain', 'domain_name',
        'supersector', 'supersector_name',
        'sector', 'sector_name',
        'subsector', 'subsector_name',
        'industry_group', 'industry_group_name',
        'naics_industry', 'naics_industry_name',
        'detailed_industry', 'detailed_industry_name'
    ], 
    survey: Literal['ces', 'bed', 'qcew'] = DEFAULT_SURVEY,
    year: int = DEFAULT_YEAR,
    industry_df: pl.DataFrame = industries_df
) -> Dict[str, str]:

    '''
    Create a mapping dictionary from one industry field to another.
    
    Args:
        from_industry: Source industry field (e.g., 'sector')
        to_industry: Target industry field (e.g., 'supersector', 'sector_name')
        survey: Survey type ('ces', 'bed', 'qcew') (default: 'ces')
        year: Year for industry definitions (default: 2023)
        industry_df: DataFrame with industry data
        
    Returns:
        Dict mapping from_industry values to to_industry values
    '''

    return dict(
        industry_df
        .filter(
            (pl.col('year') == year) &
            (pl.col(survey) == True)
        )
        .select(from_industry, to_industry)
        .sort(from_industry, to_industry)
        .unique(maintain_order=True)
        .iter_rows()
    )


# -------------------------------------------------------------------------------------------------
# Mapping industry code to name
# -------------------------------------------------------------------------------------------------

def get_industry(
    industry: Literal[
        'domain', 'supersector', 'sector', 'subsector', 
        'industry_group', 'naics_industry', 'detailed_industry'
    ],
    survey: Literal['ces', 'bed', 'qcew'] = DEFAULT_SURVEY,
    year: int = DEFAULT_YEAR,
    industry_df: pl.DataFrame = industries_df
) -> Dict[str, str]:

    '''
    Get a mapping of industry codes to industry names.
    
    Args:
        industry: Type of industry level ('domain', 'supersector', 'sector', 
                  'subsector', 'industry_group', 'naics_industry', 'detailed_industry')
        survey: Survey type ('ces', 'bed', 'qcew') (default: 'ces')
        year: Year for industry definitions (default: 2023)
        industry_df: DataFrame with industry data
        
    Returns:
        Dict mapping industry codes to industry names
    '''

    _code = industry
    _name = industry + '_name'

    return dict(
        industry_df
        .filter(
            (pl.col('year') == year) &
            (pl.col(survey) == True)
        )
        .select(_code, _name)
        .sort(_code, _name)
        .unique(maintain_order=True)
        .iter_rows()
    )


# -------------------------------------------------------------------------------------------------
# List valid industry codes and names
# -------------------------------------------------------------------------------------------------

def valid_industry(
    industry: Literal[
        'domain', 'domain_name',
        'supersector', 'supersector_name',
        'sector', 'sector_name',
        'subsector', 'subsector_name',
        'industry_group', 'industry_group_name',
        'naics_industry', 'naics_industry_name',
        'detailed_industry', 'detailed_industry_name'
    ], 
    survey: Literal['ces', 'bed', 'qcew'] = DEFAULT_SURVEY,
    year: int = DEFAULT_YEAR,
    industry_df: pl.DataFrame = industries_df
) -> List[str]:

    '''
    Get a list of valid codes for a given industry type.
    
    Args:
        industry: Column name for industry type (e.g., 'sector', 'subsector_name')
        survey: Survey type ('ces', 'bed', 'qcew') (default: 'ces')
        year: Year for industry definitions (default: 2023)
        industry_df: DataFrame with industry data
        
    Returns:
        List of valid industry codes
    '''

    return list(
        industry_df
        .filter(
            (pl.col('year') == year) &
            (pl.col(survey) == True)
        )
        .get_column(industry)
        .unique()
        .sort()
        .to_list()
    )
