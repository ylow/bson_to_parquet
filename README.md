# BSON to Parquet
A couple of simple utilities I wrote for my own use to convert BSON dumps from
MongoDB to Parquet files. It is kinda slow, and works just well enough for my
purposes. 

Use `print_bson.py` to inspect the BSON dump (in particular
with the '-f' option) and see what might want to exclude, and what you
might want to be integer column.  All other columns are forced to be strings.
Then use `bson_to_parquet.py` with the appropriate options to -x and -i to
convert the BSON dump to Parquet.

## Print BSON
```
$ python ~/repos/bson_to_parquet/print_bson.py -h
usage: print_bson [-h] [-s SKIP] [-f] [-w] input

Prints a bunch of BSON lines to stdout

positional arguments:
  input

options:
  -h, --help            show this help message and exit
  -s SKIP, --skip SKIP  Prints every nth line. Example: --skip 100 to print every 100th line
  -f, --flatten         Flatten dictionaries
  -w, --wait            Wait for user input before printing the next line
```

## Convert BSON to Parquet
```
$ python ~/repos/bson_to_parquet/bson2parquet.py -h
usage: bson2parquet [-h] [-x EXCLUDE] [-i INTEGER] input output

Converts BSON dump (from mongo) to parquet. 
Automatically recursively flattens dictionaries. Lists are not supported.

positional arguments:
  input
  output

options:
  -h, --help            show this help message and exit
  -x EXCLUDE, --exclude EXCLUDE
                        if column name includes this substring it is excluded. 
                        This option can be repeated. Example: -x secrets
  -i INTEGER, --integer INTEGER
                        if column name is this string it is forced to be integer. 
                        This option can be repeated. Example: -i size
```

