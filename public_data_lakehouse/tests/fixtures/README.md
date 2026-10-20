# Test fixtures (synthetic, test-only)

Everything in this folder is **hand-written synthetic data for unit tests only**. It is not, and
must never be loaded as, part of the public Olist dataset used by the pipeline.

* `orders_malformed.csv` - 4 rows in the Olist `orders` layout: a valid row, a row with an
  unparseable timestamp, a row with a quoted multi-line field, and a row with an extra column.
* `bad_header_orders.csv` - an `orders` file whose header drifted (renamed column), used to test
  schema-drift detection.

Most transformation tests build tiny DataFrames inline (see `tests/test_transformations.py`).
