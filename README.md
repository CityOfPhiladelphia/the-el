# the-el
Command-line tool to **e**xtract and **l**oad SQL tables using a
[JSON Table Schema][table schema]. Wraps [our fork][fork] of
[jsontableschema_sql][jsontableschema_sql] and adds [Carto][carto]
support.

## Usage
```bash
# Extract a table to a CSV file
the_el read WASTE_BASKETS --db-schema GIS_STREETS --geometry-support sde-char --output-file waste_baskets.csv

# Generate a JSON Table Schema file from a table
the_el describe_table WASTE_BASKETS --db-schema GIS_STREETS --geometry-support sde-char --output-file schema.json

# Create a table using a JSON Table Schema file
the_el create_table waste_baskets_new schema.json --db-schema phl --geometry-support postgis

# Load a CSV file into a table
the_el write waste_baskets_new --db-schema phl --table-schema-path schema.json --geometry-support postgis --input-file waste_baskets.csv --skip-headers

# Swap 2 tables
the_el swap_table waste_baskets_new waste_baskets --db-schema phl
```
_Note: Each command also requires a `--connection` parameter providing a
connection string_

## Installation
```bash
pip install git+https://github.com/CityOfPhiladelphia/the-el.git#egg=the_el --process-dependency-links
```

[fork]: https://github.com/frictionlessdata/jsontableschema-sql-py/compare/master...CityOfPhiladelphia:master
[jsontableschema_sql]: https://github.com/frictionlessdata/jsontableschema-sql-py
[table schema]: http://frictionlessdata.io/guides/json-table-schema/
[carto]: https://carto.com
