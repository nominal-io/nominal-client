"""
Example tablular datasets for testing and pedagogy.
"""


def penguins():
    """
    Each row represents a penguin.

    https://archive.ics.uci.edu/dataset/690/palmer+penguins-3

    Returns:
        A `polars.DataFrame` with 344 rows and the following columns:

        studyName, Sample Number, Species, Region, Island, Stage, Individual ID, Clutch Completion,
        Date Egg, Culmen Length (mm), Culmen Depth (mm), Flipper Length (mm), Body Mass (g),
        Sex, Delta 15 N (o/oo), Delta 13 C (o/oo), Comments
    """
    return _get_dataset("penguins")


def _get_dataset(d, ext=".csv"):
    import os

    import polars

    return polars.read_csv(
        os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data",
            d + ext,
        )
    )
