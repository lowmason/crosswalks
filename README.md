# Crosswalks

A Python library for mapping relationships between geographic area codes and industry codes at various levels of aggregation.

## Overview

Crosswalks provides utilities for:

- **Code-to-code mapping**: Convert between different geographic identifiers (e.g., state FIPS to region, county to MSA) or industry codes (e.g., NAICS sector to supersector)
- **Code-to-name mapping**: Look up human-readable names for area or industry codes
- **Validation**: Get lists of valid codes for each area or industry type
- **Multi-year support**: Access geographic and industry definitions across different years (2003, 2013, 2023)

## Installation

```bash
pip install crosswalks
```

Or with uv:

```bash
uv add crosswalks
```

## Geographic Areas

The library supports the following geographic hierarchy:

| Level | Code Field | Name Field |
|-------|------------|------------|
| Region | `region` | `region_name` |
| Division | `division` | `division_name` |
| State | `state_fips` | `state_name`, `state_abbr` |
| County | `county_fips` | `county_name` |

Additional metropolitan area codes are available:

- `cbsa_code` / `cbsa_title` - Core Based Statistical Areas
- `msa_code` / `msa_title` - Metropolitan Statistical Areas  
- `csa_code` / `csa_title` - Combined Statistical Areas
- `metro` - Metropolitan indicator (1 = metro, 0 = non-metro)

### Usage

#### Check available years

```python
from crosswalks.geographic_codes import available_years

years = available_years()
# [2003, 2013, 2023]
```

#### Map between area types

```python
from crosswalks.geographic_codes import area_mapping

# Get a dictionary mapping state FIPS codes to region codes (default year: 2023)
state_to_region = area_mapping('state_fips', 'region')
# {'01': '3', '02': '4', '04': '4', ...}

# Map county FIPS to state abbreviations for a specific year
county_to_state = area_mapping('county_fips', 'state_abbr', year=2013)
# {'01001': 'AL', '01003': 'AL', ...}
```

#### Get code-to-name mappings

```python
from crosswalks.geographic_codes import get_area

# Get region code to name mapping
regions = get_area('region')
# {'1': 'Northeast', '2': 'Midwest', '3': 'South', '4': 'West'}

# Get state FIPS to state name mapping
states = get_area('state')
# {'01': 'Alabama', '02': 'Alaska', ...}

# Get CBSA codes to titles
cbsas = get_area('cbsa')
# {'C1010': 'Abilene, TX', ...}
```

#### List valid codes

```python
from crosswalks.geographic_codes import valid_area

# Get all valid state FIPS codes
state_codes = valid_area('state_fips')
# ['01', '02', '04', '05', ...]

# Get all valid region names
region_names = valid_area('region_name')
# ['Midwest', 'Northeast', 'South', 'West']
```

## Industry Codes

The library supports NAICS-based industry codes at multiple levels of aggregation, filtered by survey type.

| Level | Code Field | Name Field |
|-------|------------|------------|
| Domain | `domain` | `domain_name` |
| Supersector | `supersector` | `supersector_name` |
| Sector | `sector` | `sector_name` |
| Subsector | `subsector` | `subsector_name` |
| Industry Group | `industry_group` | `industry_group_name` |
| NAICS Industry | `naics_industry` | `naics_industry_name` |
| Detailed Industry | `detailed_industry` | `detailed_industry_name` |

### Survey Types

Industry codes can be filtered by survey validity:

- `ces` - Current Employment Statistics (default)
- `bed` - Business Employment Dynamics
- `qcew` - Quarterly Census of Employment and Wages

### Usage

#### Check available years

```python
from crosswalks.industry_codes import available_years

# Years available for CES survey (default)
ces_years = available_years()
# [2007, 2012, 2017, 2022]

# Years available for QCEW survey
qcew_years = available_years(survey='qcew')
```

#### Map between industry levels

```python
from crosswalks.industry_codes import industry_mapping

# Map sector codes to supersector names (CES survey, default year: 2023)
sector_to_supersector = industry_mapping('sector', 'supersector_name')
# {'11': 'Natural Resources and Mining', '21': 'Natural Resources and Mining', ...}

# Map subsector to sector for QCEW survey
subsector_to_sector = industry_mapping('subsector', 'sector', survey='qcew')

# Map for a specific year
mapping_2017 = industry_mapping('sector', 'domain_name', year=2017)
```

#### Get code-to-name mappings

```python
from crosswalks.industry_codes import get_industry

# Get sector code to name mapping
sectors = get_industry('sector')
# {'11': 'Agriculture, Forestry, Fishing and Hunting', '21': 'Mining, Quarrying, and Oil and Gas Extraction', ...}

# Get supersector mapping for BED survey
supersectors = get_industry('supersector', survey='bed')

# Get domain mapping for a specific year
domains = get_industry('domain', year=2017)
```

#### List valid codes

```python
from crosswalks.industry_codes import valid_industry

# Get all valid sector codes for CES
sector_codes = valid_industry('sector')
# ['11', '21', '22', '23', ...]

# Get all valid subsector names for QCEW
subsector_names = valid_industry('subsector_name', survey='qcew')
```

## Data Sources

Geographic reference data is sourced from:

- U.S. Census Bureau FIPS codes
- Office of Management and Budget (OMB) metropolitan area delineations (2003, 2013, 2023)

Industry reference data is sourced from:

- U.S. Bureau of Labor Statistics NAICS codes
- Survey-specific industry definitions for CES, BED, and QCEW

## Dependencies

- Python 3.12+
- [Polars](https://pola.rs/) for data manipulation
