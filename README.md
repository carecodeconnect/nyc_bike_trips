# NYC Bike Trips


# Overview

This work-in-progress repo illustrates how Python `polars` can be
applied in a data science project to tidy, analyse, and visualise trips
data using software engineering best practices.

The data are bicycle trips taken in New York City using [Citi
Bike](https://en.wikipedia.org/wiki/Citi_Bike) bicycles. Citi Bike is a
privately owned public bicycle sharing system, using technology from
Lyft. Citi Bike opened in 2013. This project analyses data from the
month of March 2024. The system data for Citi Bike is available
[here](https://citibikenyc.com/system-data).

The analysis develops code written for [Python Polars: The Definitive
Guide](https://github.com/jeroenjanssens/python-polars-the-definitive-guide)
by Janssens and Nieuwdorp. The repo applies software engineering
principles to data science based on [Python Real-World
Projects](https://www.oreilly.com/library/view/python-real-world-projects/9781803246765/)
by Stephen Lott and [Software Engineering for Data
Scientists](https://www.oreilly.com/library/view/software-engineering-for/9781098136192/)
by Catherine Nelson.

![NYC Bike Stations](img/nyc_map.png)

Click here to view the analysis completed so far:

- [Collect & Transform](01_collect_transform.md)

- [Visualise](02_visualise.md)

TODO:

- Add testing

- Add API documentation

- Add statistical modeling and Machine Learning

# Quick Start

- Install
  [`uv`](https://docs.astral.sh/uv/getting-started/installation/)

In your projects directory:

``` {bash}
git clone git@github.com:carecodeconnect/nyc_bike_trips.git
cd nyc_bike_trips
uv sync
```

Run the Python script:

``` {bash}
uv run main.py
uv run main_refactored.py
```

And/or render the Quarto documents to GfM, HTML, PDF:

``` {bash}
uv run quarto render README.qmd
uv run quarto render 01_collect_transform.qmd
uv run quarto render 02_visualise.qmd
```

# Project Dependencies

The following tools are used in this project:

- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) for
  Python, environment, and dependency management.

- [`polars`](https://docs.pola.rs/user-guide/getting-started/) for
  tidying, transforming, analysing data.

- [`plotnine`](https://plotnine.org/guide/install.html) for visualising
  data.

- [`quarto`](https://quarto.org/docs/get-started/) for scientific
  publishing.

- [`sphinx`](https://www.sphinx-doc.org/en/master/usage/installation.html)
  for documentation.

- `polars_geo` plugin which requires Rust. [Install
  Rust](https://www.rust-lang.org/tools/install). Or update Rust with
  `rustup update`.

- `maturin` for installing
  [`polars_geo`](https://github.com/jeroenjanssens/python-polars-the-definitive-guide/tree/main/plugins/polars_geo)
  Rust plugin: [Install
  maturin](https://www.maturin.rs/installation.html). To build the
  plugin, see [Collect & Transform](01_collect_transform.md). NB: we had
  to update the versions of some original dependencies in the
  `polars_geo` plugin to make it work on our Ubuntu system (Ubuntu 25.04
  x86_64).

Testing (TODO):

- [`tox`](https://tox.wiki/en/4.27.0/installation.html) for running test
  suite

- [`behave`](https://behave.readthedocs.io/en/latest/install/) for User
  Acceptance Testing (UAT)

- [`pytest`](https://docs.pytest.org/en/stable/getting-started.html) for
  unit testing

# To Use Polars with GPU Engine

The `polars_geo` Rust plugin does not seem to be compatible with the GPU
acceleration on the testing machine (NVIDIA Quadro RTX 4000 Mobile /
Max-Q). So the following is provided for illustration purposes only:

## Test

``` python
import polars as pl
pl.LazyFrame({"x": [1, 2, 3]}).collect(engine=pl.GPUEngine(raise_on_fail=True))
```

<div>

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (3, 1)</small>

| x   |
|-----|
| i64 |
| 1   |
| 2   |
| 3   |

</div>

</div>

## Execution

``` python
#lf.collect(engine='GPU')
```

# Development

In the `docs` directory, initialise the documentation:

``` {bash}
uv run sphinx-quickstart
```

After populating the `docs` files `index.rst`, `overview.rst`,
`api.rst`, add the following to the `conf.py` file:

``` python
extensions = [
    'sphinx.ext.todo'
]

todo_include_todos = True
```

Then, build the documentation:

``` {bash}
uv run make html
```

Tox:

``` {bash}
uv tool install tox
```
