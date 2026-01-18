import marimo

__generated_with = "0.19.4"
app = marimo.App()


@app.cell
def _():
    # -------------------------------------------------------------------------------------------------
    # Imports and parameters
    # -------------------------------------------------------------------------------------------------

    from pathlib import Path
    from typing import Dict, List, Literal

    import polars as pl

    BASE_PATH = Path(__file__).parent.parent.parent
    return BASE_PATH, pl


@app.cell
def _(BASE_PATH, pl):
    # -------------------------------------------------------------------------------------------------
    # Load geographic areas data
    # -------------------------------------------------------------------------------------------------

    fips_csv = (
        pl
        .read_csv(
            f'{BASE_PATH}/data/reference/area/state_county_fips.csv', 
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
    return (fips_csv,)


@app.cell
def _(BASE_PATH, fips_csv, pl):
    fips_list, df_list = [], []
    for yr in [2003, 2013, 2023]:

        fips_list.append(
            fips_csv
            .select(
                pl.lit(yr, pl.UInt16).alias('year'),
                pl.all()
            )
        )
    
        df_list.append(
            pl
            .read_csv(
                f'{BASE_PATH}/data/reference/area/{yr}.csv', 
                schema_overrides={
                    'state_fips': pl.Utf8,
                    'county_fips': pl.Utf8,
                    'cbsa_code': pl.Utf8,
                    'msa_code': pl.Utf8,
                    'csa_code': pl.Utf8,
                    'cbsa_title': pl.Utf8,
                    'msa_title': pl.Utf8,
                    'csa_title': pl.Utf8,
                    'metro': pl.UInt8,
                    'central': pl.UInt8
                }
            )
            .select(
                year=pl.lit(yr, pl.UInt16),
                state_fips=pl.col('state_fips')
                             .str.zfill(2),
                county_fips=pl.col('county_fips')
                              .str.zfill(3),
                cbsa_code=pl.col('cbsa_code'),
                msa_code=pl.col('msa_code'),
                csa_code=pl.col('csa_code'),
                cbsa_title=pl.col('cbsa_title'),
                msa_title=pl.col('msa_title'),
                csa_title=pl.col('csa_title'),
                metro=pl.col('metro')
            )
        )
    return df_list, fips_list


@app.cell
def _(fips_list, pl):
    fips_df = (
        pl
        .concat(
            fips_list
        )
        .sort('year', 'state_fips', 'county_fips')
    )
    return (fips_df,)


@app.cell
def _(df_list, pl):
    df_2003 = (
        df_list[0]
        .with_columns(
            metro=pl.when(pl.col('msa_title').str.contains('MSA', literal=True))
                    .then(pl.lit(1, pl.UInt8))
                    .otherwise(pl.lit(0, pl.UInt8))
        )
        .with_columns(
            msa_title=pl.col('msa_title')
                        .str.replace(' MSA', '', literal=True)
                        .str.replace(' MicroSA', '', literal=True),
            csa_title=pl.col('msa_title')
                        .str.replace(' CSA', '', literal=True)
        )
    )

    df_2013 = df_list[1]
    df_2023 = df_list[2]
    return df_2003, df_2013, df_2023


@app.cell
def _(df_2003, df_2013, df_2023, pl):
    msa_csa = (
        pl
        .concat([
            df_2003,
            df_2013,
            df_2023
        ])
        .with_columns(
            county_fips=pl.concat_str(
                pl.col('state_fips'),
                pl.col('county_fips'),
                separator=''
            )
        )
        .sort('year', 'state_fips', 'county_fips')
    )
    return (msa_csa,)


@app.cell
def _(BASE_PATH, fips_df, msa_csa, pl):
    (
        fips_df
        .join(
            msa_csa,
            how='left',
            on=['year', 'state_fips', 'county_fips']
        )
        .with_columns(
            metro=pl.col('metro')
                    .fill_null(0)
        )
        .write_parquet(f'{BASE_PATH}/data/geographic_codes.parquet')
    )
    return


if __name__ == "__main__":
    app.run()
