def format_millions_unit_to_str(val: float) -> str:
    """ Returns a str short hand readable float value in the millions unit. """
    unit = ""
    if val < 1000:
        unit = "M"
        val = val/1
    elif val < 1000000:
        unit = "B"
        val = val/1000
    elif val < 1000000000:
        val = val/1000000
        unit = "T"

    rounded_val = round(val, 2)
    formatted_str = str(rounded_val)+unit
    return formatted_str
