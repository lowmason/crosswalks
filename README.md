# Crosswalks

A Python library for mapping relationships between geographic area codes and industry codes at various levels of aggregation.

## Overview

Crosswalks provides utilities for:

- **Code-to-code mapping**: Convert between different geographic identifiers (e.g., state FIPS to region, county to MSA)
- **Code-to-title mapping**: Look up human-readable names for area codes
- **Validation**: Get lists of valid codes for each area type

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

## Usage

### Map between area types

```python
from crosswalks.geo_areas import area_mapping

# Get a dictionary mapping state FIPS codes to region codes
state_to_region = area_mapping('state_fips', 'region')
# {'01': '3', '02': '4', '04': '4', ...}

# Map county FIPS to state abbreviations
county_to_state = area_mapping('county_fips', 'state_abbr')
# {'01001': 'AL', '01003': 'AL', ...}
```

### Get code-to-name mappings

```python
from crosswalks.geo_areas import get_area

# Get region code to name mapping
regions = get_area('region')
# {'1': 'Northeast', '2': 'Midwest', '3': 'South', '4': 'West'}

# Get state FIPS to state name mapping
states = get_area('state')
# {'01': 'Alabama', '02': 'Alaska', ...}
```

### List valid codes

```python
from crosswalks.geo_areas import valid_area

# Get all valid state FIPS codes
state_codes = valid_area('state_fips')
# ['01', '02', '04', '05', ...]

# Get all valid region names
region_names = valid_area('region_name')
# ['Midwest', 'Northeast', 'South', 'West']
```

## Data Sources

Geographic reference data is sourced from:

- U.S. Census Bureau FIPS codes
- Office of Management and Budget (OMB) metropolitan area delineations (2003, 2013, 2023)

## Roadmap

- [x] Geographic area crosswalks
- [ ] Industry code crosswalks (NAICS, SIC)

## Dependencies

- Python 3.12+
- [Polars](https://pola.rs/) for data manipulation
