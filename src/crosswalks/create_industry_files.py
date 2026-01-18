import marimo

__generated_with = "0.19.4"
app = marimo.App(width="full")


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

    naics_list = []
    for yr in [2007, 2012, 2017, 2022]:
        naics_list.append(
            pl
            .read_excel(
                f'{BASE_PATH}/data/reference/industry/naics_codes.xlsx', 
                sheet_name=f'naics_{yr}'
            )
            .select(
                year=pl.lit(yr, pl.UInt16),
                ces=pl.col('ces')
                      .cast(pl.UInt8),
                bed=pl.lit(1, pl.UInt8),
                qcew=pl.lit(1, pl.UInt8),
                naics_len=pl.col('naics_len')
                            .cast(pl.UInt8),
                naics_code=pl.when(pl.col('naics_len').eq(0))
                             .then(pl.col('naics_code').cast(pl.Utf8).str.zfill(2))
                             .otherwise(pl.col('naics_code').cast(pl.Utf8)),
                naics_title=pl.col('naics_title')
            )
        )

    naics_1 = (
        pl
        .concat(
            naics_list
        )
        .unique()
        .sort('year', 'naics_len', 'naics_code')
    )
    return (naics_1,)


@app.cell
def _(naics_1, pl):
    naics_2 = (
        naics_1
        .filter(
            pl.col('naics_len').eq(6)
        )
        .select(
            year=pl.col('year'),
            ces=pl.col('ces'),
            bed=pl.col('bed'),
            qcew=pl.col('qcew'),
            domain=pl.col('naics_code')
                     .str.slice(0, 1),
            supersector=pl.col('naics_code')
                          .str.slice(0, 1),
            sector=pl.col('naics_code')
                     .str.slice(0, 2),
            subsector=pl.col('naics_code')
                        .str.slice(0, 3),
            industry_group=pl.col('naics_code')
                             .str.slice(0, 4),
            naics_industry=pl.col('naics_code')
                             .str.slice(0, 5),
            detailed_industry=pl.col('naics_code')
        )
        .with_columns(
            sector=pl.when(pl.col('sector').is_in(['31', '32', '33']))
                     .then(pl.lit('31', pl.Utf8))
                     .when(pl.col('sector').is_in(['44', '45']))
                     .then(pl.lit('44', pl.Utf8))
                     .when(pl.col('sector').is_in(['48', '49']))
                     .then(pl.lit('48', pl.Utf8))
                     .otherwise(pl.col('sector'))
        )
        .with_columns(
            supersector=pl.when(pl.col('sector').is_in(['11', '21']))
                          .then(pl.lit('10', pl.Utf8))
                          .when(pl.col('sector').is_in(['23']))
                          .then(pl.lit('20', pl.Utf8))
                          .when(pl.col('sector').is_in(['31']))
                          .then(pl.lit('30', pl.Utf8))
                          .when(pl.col('sector').is_in(['22', '42', '44', '48']))
                          .then(pl.lit('40', pl.Utf8))
                          .when(pl.col('sector').is_in(['51']))
                          .then(pl.lit('50', pl.Utf8))
                          .when(pl.col('sector').is_in(['52', '53']))
                          .then(pl.lit('55', pl.Utf8))
                          .when(pl.col('sector').is_in(['54', '55', '56']))
                          .then(pl.lit('60', pl.Utf8))
                          .when(pl.col('sector').is_in(['61', '62']))
                          .then(pl.lit('65', pl.Utf8))
                          .when(pl.col('sector').is_in(['71', '72']))
                          .then(pl.lit('70', pl.Utf8))
                          .when(pl.col('sector').is_in(['81']))
                          .then(pl.lit('80', pl.Utf8))
                          .when(pl.col('sector').is_in(['92']))
                          .then(pl.lit('92', pl.Utf8))
                          .otherwise(pl.lit('99', pl.Utf8))
        )
        .with_columns(
            domain=pl.when(pl.col('supersector').is_in(['10', '20', '30']))
                          .then(pl.lit('06', pl.Utf8))
                          .when(pl.col('supersector').is_in(['40', '50', '55', '60', '65', '70', '80', '92']))
                          .then(pl.lit('07', pl.Utf8))
                          .otherwise(pl.lit('99', pl.Utf8))
        )

    )
    return (naics_2,)


@app.cell
def _(naics_1, pl):
    df_dict = {}
    for col, lvl in [
        ('domain', 0), 
        ('supersector', 1),
        ('sector', 2),
        ('subsector', 3),
        ('industry_group', 4),
        ('naics_industry', 5),
        ('detailed_industry', 6)
    ]:
    
        df_dict[lvl] = (
            naics_1
            .filter(
                pl.col('naics_len').eq(lvl)
            )
            .select(
                pl.col('naics_code')
                  .alias(f'{col}'),
                pl.col('naics_title')
                  .alias(f'{col}_name')
            )
            .sort(f'{col}', f'{col}_name')
            .unique(maintain_order=True)
        )
    return (df_dict,)


@app.cell
def _(BASE_PATH, df_dict, naics_1, naics_2, pl):
    (
        naics_1
        .filter(
            pl.col('naics_len').eq(6)
        )
        .select('year', 'naics_code')
        .join(
            naics_2,
            how='inner',
            left_on=['year', 'naics_code'],
            right_on=['year', 'detailed_industry']
        )
        .with_columns(
            detailed_industry=pl.col('naics_code')
        )
        .drop('naics_code')
        .join(
            df_dict[0],
            how='left',
            on='domain'
        )
        .join(
            df_dict[1],
            how='left',
            on='supersector'
        )
        .join(
            df_dict[2],
            how='left',
            on='sector'
        )
        .join(
            df_dict[3],
            how='left',
            on='subsector'
        )
        .join(
            df_dict[4],
            how='left',
            on='industry_group'
        )
        .join(
            df_dict[5],
            how='left',
            on='naics_industry'
        )
        .join(
            df_dict[6],
            how='left',
            on='detailed_industry'
        )
        .sort('year', 'domain', 'supersector', 'sector', 'subsector', 'industry_group', 'naics_industry', 'detailed_industry')
        .unique(
            subset=['year', 'domain', 'supersector', 'sector', 'subsector', 'industry_group', 'naics_industry', 'detailed_industry'],
            maintain_order=True
        )
        .write_csv(
                f'{BASE_PATH}/data/industry_codes.csv'        
        )
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
