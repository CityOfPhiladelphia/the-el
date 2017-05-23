# the-el
Command-line tool wrapping [our fork][fork] of [jsontableschema_sql][jsontableschema_sql],
which **e**xtracts and **l**oads SQL tables based on [JSON Table Schema][table schema] files.

## Usage
```bash
# Extract a table to a CSV file
the_el extract WASTE_BASKETS --schema GIS_STREETS --geometry-type oracle > waste_baskets.csv

# Generate a JSON Table Schema file from a table
the_el describe WASTE_BASKETS --schema GIS_STREETS > schema.json

# Create a table using a JSON Table Schema file
the_el create waste_baskets schema.json --schema phl

# Load a CSV file into a table
the_el load waste_baskets --schema phl --schema-file schema.json < waste_baskets.csv
```

## Installation
```bash
pip install git+https://github.com/CityOfPhiladelphia/the-el.git#egg=the_el --process-dependency-links
```

[fork]: https://github.com/frictionlessdata/jsontableschema-sql-py/compare/master...CityOfPhiladelphia:master
[jsontableschema_sql]: https://github.com/frictionlessdata/jsontableschema-sql-py
[table schema]: http://frictionlessdata.io/guides/json-table-schema/
