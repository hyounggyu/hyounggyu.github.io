---
title: "Polars, Dask, DuckDB를 이용하여 여러 Parquet 파일 쿼리하기"
date: 2023-12-26T10:37:52+07:00
draft: false
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

분할되어 있는 여러 Parquet 파일을 읽어서 처리해야하는 경우, Polars, Dask, DuckDB를 이용할 수 있다.

Polars는 Apache Arrow 형식을 사용하고 Rust 언어로 구현된 데이터 프레임 라이브러리이다. Apache Arrow 형식은 Pandas를 개발한 Wes McKinney가 참여하고 있으며, 더 자세한 내용은 [Apache Arrow and the Future of Data Frames with Wes McKinney](https://learning.acm.org/techtalks/apache)에서 확인할 수 있다.

Dask는 스케일 확장에 최적화된 Python 라이브러리이다. High Performance Computing (HPC) 환경 뿐 아니라 단일 컴퓨터에서도 사용할 수 있다. 특히, 단일 컴퓨터에서 사용할 때에도 쉽게 설치할 수 있어 로컬 환경에서 테스트한 후 HPC 환경에서 사용할 수 있다.

DuckDB는 C++로 구현된 embeddable SQL OLAP database이다. 손쉽게 설치할 수 있으며 Python에서 사용할 수 있다.

예제에서 사용하는 데이터와 소스코드는 https://github.com/hyounggyu/hello_data 에서 확인할 수 있으며 Jupyter notebook은 [여기](https://github.com/hyounggyu/hello_data/blob/main/polars_dask_duckdb/notebook.ipynb)이다.

데이터는 S&P 500 기업 중 25개 기업을 임의로 뽑아 2023년 1월 2일부터 2023년 12월 22일까지의 종가를 2023년 1월 2일 기준으로 상대적인 수치로 변환하였다. 아래 예,

```plain
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

## 데이터 쿼리하기

Polars는 다음과 같이 사용할 수 있다.

```python
pl_df = pl.read_parquet("./data/parquet/*.parquet")
pl_df.filter((pl.col("date") == pl.lit(date(2023, 12, 22))) & (pl.col("close") >= 1.5))
```

결과는 다음과 같다.

```plain
shape: (2, 3)
┌────────────┬────────┬──────────┐
│ date       ┆ ticker ┆ close    │
│ ---        ┆ ---    ┆ ---      │
│ date       ┆ str    ┆ f64      │
╞════════════╪════════╪══════════╡
│ 2023-12-22 ┆ intu   ┆ 1.595353 │
│ 2023-12-22 ┆ phm    ┆ 2.2171   │
└────────────┴────────┴──────────┘
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

결과는 다음과 같다.

```plain
           date ticker     close
245  2023-12-22   intu  1.595353
245  2023-12-22    phm  2.217100
```

DuckDB는 다음과 같이 사용할 수 있다.

```python
TABLE = "./data/parquet/*.parquet"
duckdb.query(f"""SELECT * FROM '{TABLE}' WHERE date = '2023-12-22' AND close >= 1.5""")
```

결과는 다음과 같다.

```plain
┌────────────┬─────────┬────────────────────┐
│    date    │ ticker  │       close        │
│    date    │ varchar │       double       │
├────────────┼─────────┼────────────────────┤
│ 2023-12-22 │ phm     │  2.217099567099567 │
│ 2023-12-22 │ intu    │ 1.5953525231351298 │
└────────────┴─────────┴────────────────────┘
```

## Pivot과 Aggregate

위 데이터를 이용하여 pivot table을 만들어보자. 특히, `closes` 컬럼에는 list 형태로 모든 종목의 상대적인 수치가 들어있다.

```plain
┌─────────┬────────────┬────────────────────┬───┬────────────────────┬────────────────────┬──────────────────────┐
│ ticker  │ 2023-01-03 │     2023-01-04     │ … │     2023-12-21     │     2023-12-22     │        closes        │
│ varchar │   double   │       double       │   │       double       │       double       │       double[]       │
├─────────┼────────────┼────────────────────┼───┼────────────────────┼────────────────────┼──────────────────────┤
│ intu    │        1.0 │ 1.0009969834858632 │ … │ 1.5874789099647222 │ 1.5953525231351298 │ [1.0, 1.0009969834…  │
│ etr     │        1.0 │ 1.0007330706496838 │ … │ 0.9138641986621462 │ 0.9183542563914597 │ [1.0, 1.0007330706…  │
│ sbux    │        1.0 │ 1.0360011901219874 │ … │ 0.9454527422394129 │  0.944956858077953 │ [1.0, 1.0360011901…  │
│ xyl     │        1.0 │ 0.9984651498736006 │ … │ 1.0083965330444202 │ 1.0169736366919464 │ [1.0, 0.9984651498…  │
│ duk     │        1.0 │ 1.0107039537126326 │ … │ 0.9292189006750241 │ 0.9340405014464802 │ [1.0, 1.0107039537…  │
├─────────┴────────────┴────────────────────┴───┴────────────────────┴────────────────────┴──────────────────────┤
│ 5 rows                                                                                   248 columns (6 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Polars는 다음과 같이 사용할 수 있다.

```python
pl_df = pl.read_parquet("./data/parquet/*.parquet")
pl_df.pivot(index="ticker", columns="date", values="close").with_columns(
    pl.concat_list(pl.all().exclude("ticker")).alias("closes")
)
```

Dask는 다음과 같이 사용할 수 있다.

```python
dd_df = dd.read_parquet("./data/parquet/*.parquet")

# Make date column to categorical to use pivot method
dd_df.date = dd_df.date.dt.strftime("%Y-%m-%d").astype("category").cat.as_known()

dd_pivot_df = dd_df.pivot_table(index='ticker', columns='date', values='close').compute()

# Aggregate the date columns
dd_agg_df = dd_df.compute().groupby('ticker').agg({"close": list})

# Merge the two dataframes
dd_final_df = dd_pivot_df.merge(dd_agg_df, on="ticker")

dd_final_df.rename(columns={"close": "closes"}, inplace=True)
```

**NOTE: 만약 동일 작업을 lazy evaluation으로 하는 경우, Object 형식의 float64[] 타입이 string 타입으로 변환된다.**

위 코드의 `compute()` 함수 호출 위치를 바꿔보자.

```python
dd_dtissue_df = dd.read_parquet("./data/parquet/*.parquet")
dd_dtissue_df.date = dd_dtissue_df.date.dt.strftime("%Y-%m-%d").astype("category").cat.as_known()
dd_dtissue_pivot_df = dd_dtissue_df.pivot_table(index='ticker', columns='date', values='close')
dd_dtissue_agg_df = dd_dtissue_df.groupby('ticker').agg({"close": list})
dd_dtissue_final_df = dd_dtissue_pivot_df.merge(dd_dtissue_agg_df, on="ticker").compute()
dd_dtissue_final_df.rename(columns={"close": "closes"}, inplace=True)

type(dd_dtissue_final_df.closes.iloc[0])
```

위 코드를 실행하면 다음과 같이 `closes` 컬럼이 `str`로 변환된 것을 확인할 수 있다.

```plain
str
```

DuckDB는 다음과 같이 사용할 수 있다.

```python
TABLE = "./data/parquet/*.parquet"

# Directly join the results of the two queries using subqueries
duckdb.sql(f"""
CREATE OR REPLACE TEMPORARY TABLE final_t AS (
    SELECT pivot_t.*, agg_t.closes
    FROM (
        PIVOT '{TABLE}' ON date USING first(close) GROUP BY ticker
    ) AS pivot_t
    INNER JOIN (
        SELECT ticker, list(close ORDER BY date ASC) AS closes
        FROM '{TABLE}'
        GROUP BY ticker
    ) AS agg_t ON pivot_t.ticker = agg_t.ticker
)
""")
```

다음과 같이 DuckDB의 결과를 Pandas dataframe으로 변환할 수 있다.

```python
duckdb.sql("SELECT * FROM final_t").to_df()
```

그리고 DuckDB의 결과를 다음과 같이 Parquet 파일로 저장할 수 있다.

```python
OUTPUT = "./tmp/duckdb.parquet"

duckdb.sql(f"""COPY final_t TO '{OUTPUT}' (FORMAT PARQUET)""")
```
