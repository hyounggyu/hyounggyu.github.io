---
title: "Polars, Dask, DuckDB를 이용하여 여러 Parquet 파일 쿼리하기"
date: 2023-12-26T10:37:52+07:00
draft: true
# Optional variables
# Ref: https://github.com/adityatelange/hugo-PaperMod/wiki/Installation#sample-pagemd
#
# # weight: 1
# # aliases: ["/first"]
# tags: ["first"]
# author: "Me"
# # author: ["Me", "You"] # multiple authors
# showToc: true
# TocOpen: false
# hidemeta: false
# comments: false
# description: "Desc Text."
# canonicalURL: "https://canonical.url/to/page"
# disableHLJS: true # to disable highlightjs
# disableShare: false
# disableHLJS: false
# hideSummary: false
# searchHidden: true
# ShowReadingTime: true
# ShowBreadCrumbs: true
# ShowPostNavLinks: true
# ShowWordCount: true
# ShowRssButtonInSectionTermList: true
# UseHugoToc: true
# cover:
#     image: "<image path/url>" # image path/url
#     alt: "<alt text>" # alt text
#     caption: "<text>" # display caption under cover
#     relative: false # when using page bundles set this to true
#     hidden: true # only hide on current single page
# editPost:
#     URL: "https://github.com/<path_to_repo>/content"
#     Text: "Suggest Changes" # edit text
#     appendFilePath: true # to append file path to Edit link
---

분할되어 있는 여러 Parquet 파일을 읽어서 처리해야하는 경우, Polars, Dask, DuckDB를 이용할 수 있다. 예제에서 사용하는 데이터와 소스코드는 [여기](https://github.com/hyounggyu/polars_dask_duckdb)에서 확인할 수 있다.

데이터는 S&P 500 기업 중 25개 기업을 임의로 뽑아 2023년 1월 2일부터 2023년 12월 22일까지의 종가를 2023년 1월 2일 기준으로 상대적인 수치로 변환하였다. 아래 예,

```
┌────────────┬─────────┬────────────────────┐
│    date    │ ticker  │       close        │
│    date    │ varchar │       double       │
├────────────┼─────────┼────────────────────┤
│ 2023-01-03 │ a       │                1.0 │
│ 2023-01-04 │ a       │ 1.0108637696614236 │
│ 2023-01-05 │ a       │ 1.0137963209810719 │
│ 2023-01-06 │ a       │ 0.9842042122100773 │
│ 2023-01-09 │ a       │   0.98287123433751 │
└────────────┴─────────┴────────────────────┘
```

## 연중 50% 이상 상승한 종목 검색하기

Polars는 다음과 같이 사용할 수 있다.

```python
pl_df = pl.read_parquet("./data/parquet/*.parquet")
pl_df.filter((pl.col("date") == pl.lit(date(2023, 12, 22))) & (pl.col("close") >= 1.5))
```

`scan_parquet`을 활용하여 lazy evaluation을 할 수 있다.

```python
pl_lazy_df = pl.scan_parquet("./data/parquet/*.parquet")
pl_lazy_df.filter((pl.col("date") == pl.lit(date(2023, 12, 22))) & (pl.col("close") >= 1.5)).collect()
```

Dask는 다음과 같이 사용할 수 있다.

```python
dd_df = dd.read_parquet("./data/parquet/*.parquet")
dd_df[(dd_df["date"] == date(2023, 12, 22)) & (dd_df["close"] >= 1.5)].compute()
```

DuckDB는 다음과 같이 사용할 수 있다.

```python
TABLE = "./data/parquet/*.parquet"
duckdb.query(f"""SELECT * FROM '{TABLE}' WHERE date = '2023-12-22' AND close >= 1.5""")
```

## Pivot Table 만들기

위 데이터를 이용하여 pivot table을 만들어보자. 특히, `allclose` 컬럼에는 list 형태로 모든 종목의 상대적인 수치가 들어있다.

```
┌─────────┬────────────┬────────────────────┬───┬────────────────────┬────────────────────┬──────────────────────┐
│ ticker  │ 2023-01-03 │     2023-01-04     │ … │     2023-12-21     │     2023-12-22     │       allclose       │
│ varchar │   double   │       double       │   │       double       │       double       │       double[]       │
├─────────┼────────────┼────────────────────┼───┼────────────────────┼────────────────────┼──────────────────────┤
│ duk     │        1.0 │ 1.0107039537126326 │ … │ 0.9292189006750241 │ 0.9340405014464802 │ [1.0, 1.0107039537…  │
│ cah     │        1.0 │ 1.0035188322689952 │ … │ 1.3194317737521177 │ 1.3206047178417828 │ [1.0, 1.0035188322…  │
│ cnc     │        1.0 │ 0.9964885879107098 │ … │ 0.9206170052671182 │ 0.9197391522447956 │ [1.0, 0.9964885879…  │
│ xyl     │        1.0 │ 0.9984651498736006 │ … │ 1.0083965330444202 │ 1.0169736366919464 │ [1.0, 0.9984651498…  │
│ ipg     │        1.0 │ 1.0269310446877775 │ … │ 0.9757324652263983 │ 0.9724770642201835 │ [1.0, 1.0269310446…  │
│ udr     │        1.0 │ 1.0171028763928478 │ … │ 0.9660533817051049 │ 0.9756413578647317 │ [1.0, 1.0171028763…  │
│ k       │        1.0 │ 0.9924231794583978 │ … │ 0.7555773817875685 │ 0.7644170057527712 │ [1.0, 0.9924231794…  │
│ xray    │        1.0 │ 1.0333435301315388 │ … │ 1.0764759865402265 │ 1.0773936983787091 │ [1.0, 1.0333435301…  │
│ trmb    │        1.0 │ 1.0039525691699605 │ … │ 1.0347826086956522 │ 1.0363636363636364 │ [1.0, 1.0039525691…  │
│ pru     │        1.0 │ 1.0170888620828307 │ … │  1.037696019300362 │ 1.0408122235625252 │ [1.0, 1.0170888620…  │
│ intu    │        1.0 │ 1.0009969834858632 │ … │ 1.5874789099647222 │ 1.5953525231351298 │ [1.0, 1.0009969834…  │
│ etr     │        1.0 │ 1.0007330706496838 │ … │ 0.9138641986621462 │ 0.9183542563914597 │ [1.0, 1.0007330706…  │
│ sbux    │        1.0 │ 1.0360011901219874 │ … │ 0.9454527422394129 │  0.944956858077953 │ [1.0, 1.0360011901…  │
│ wbd     │        1.0 │  1.088050314465409 │ … │ 1.2044025157232705 │ 1.1813417190775681 │ [1.0, 1.0880503144…  │
│ tdy     │        1.0 │ 1.0192379669136913 │ … │ 1.0637023729321058 │ 1.0815430296678894 │ [1.0, 1.0192379669…  │
│ a       │        1.0 │ 1.0108637696614236 │ … │  0.926019728072514 │  0.930218608371101 │ [1.0, 1.0108637696…  │
│ v       │        1.0 │ 1.0251699696224505 │ … │ 1.2514586045614544 │ 1.2461063696417378 │ [1.0, 1.0251699696…  │
│ maa     │        1.0 │  1.014927293784584 │ … │ 0.8495045682666325 │ 0.8565178226740446 │ [1.0, 1.0149272937…  │
│ phm     │        1.0 │  1.026190476190476 │ … │  2.217099567099567 │  2.217099567099567 │ [1.0, 1.0261904761…  │
│ o       │        1.0 │ 1.0126959247648903 │ … │ 0.8927899686520376 │ 0.8916927899686521 │ [1.0, 1.0126959247…  │
│ mtd     │        1.0 │  1.020316444006649 │ … │ 0.8270913282302804 │ 0.8303063883929488 │ [1.0, 1.0203164440…  │
│ ip      │        1.0 │ 1.0401016661959899 │ … │  1.033041513696696 │ 1.0347359502965265 │ [1.0, 1.0401016661…  │
│ eqt     │        1.0 │ 1.0436832181018227 │ … │ 1.2262727844123193 │ 1.2253299811439347 │ [1.0, 1.0436832181…  │
├─────────┴────────────┴────────────────────┴───┴────────────────────┴────────────────────┴──────────────────────┤
│ 23 rows                                                                                  248 columns (6 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Polars는 다음과 같이 사용할 수 있다.

```python
pl_df = pl.read_parquet("./data/parquet/*.parquet")
pl_df.pivot(index="ticker", columns="date", values="close").with_columns(
    pl.concat_list(pl.all().exclude("ticker")).alias("allclose")
)
```

Dask는 다음과 같이 사용할 수 있다.

```python
dd_df = dd.read_parquet("./data/parquet/*.parquet")

# Make date column to categorical to use pivot method
dd_df.date = dd_df.date.dt.strftime("%Y-%m-%d").astype("category").cat.as_known()
dd_pivot_df = dd_df.pivot_table(index='ticker', columns='date', values='close')

# Aggregate the date columns
dd_agg_df = dd_df.groupby('ticker').agg(list).drop(columns="date")

# Merge the two dataframes
dd_final_df = dd_pivot_df.merge(dd_agg_df, on="ticker").compute()

dd_final_df.rename(columns={"close": "allclose"}, inplace=True)
```

DuckDB는 다음과 같이 사용할 수 있다.

```python
TABLE = "./data/parquet/*.parquet"

# Directly join the results of the two queries using subqueries
duckdb.query(f"""
    SELECT pivot_t.*, agg_t.allclose
    FROM (
        PIVOT '{TABLE}' ON date USING first(close) GROUP BY ticker
    ) AS pivot_t
    INNER JOIN (
        SELECT ticker, list(close ORDER BY date ASC) AS allclose 
        FROM '{TABLE}' 
        GROUP BY ticker
    ) AS agg_t ON pivot_t.ticker = agg_t.ticker
""")
```

그리고 DuckDB의 결과를 다음과 같이 Parquet 파일로 저장할 수 있다.

```python
TABLE = "./data/parquet/*.parquet"
OUTPUT = "./tmp/duckdb.parquet"

# Directly join the results of the two queries using subqueries
duckdb.query(f"""
COPY (
    SELECT pivot_t.*, agg_t.allclose
    FROM (
        PIVOT '{TABLE}' ON date USING first(close) GROUP BY ticker
    ) AS pivot_t
    INNER JOIN (
        SELECT ticker, list(close ORDER BY date ASC) AS allclose 
        FROM '{TABLE}' 
        GROUP BY ticker
    ) AS agg_t ON pivot_t.ticker = agg_t.ticker
) TO '{OUTPUT}' (FORMAT PARQUET)
""")
```

## 데이터형 변환에 따른 이슈

