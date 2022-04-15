
from pathlib import Path

# ../News-Scrapper/databases
DB_DIR = Path(__file__).parent.parent.parent / "databases"

INVALID_DATATYPE = "Error: datatype not accepted. Only int, float, str."
MISSING_ALLOWED_NTS = "Error: found complex_nt, missing allowed_named_tuples."

# default allowed datatypes
PYTHON_TO_SQL_DTYPES = {
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    list: "EXTENDED"
}

DEF_PRIMARY_KEY = "agg_id"
