import csv

from pydantic import BaseModel, PositiveInt, Field
from pathlib import Path


class MinStableSchema(BaseModel):
    name: str = Field(alias="NAME")
    year: PositiveInt = Field(alias="YEAR")
    month: PositiveInt = Field(alias="MONTH")
    day: PositiveInt = Field(alias="DAY")
    period: PositiveInt = Field(alias="PERIOD")
    band: PositiveInt = Field(alias="BAND")
    value: float = Field(alias="VALUE")


def get_minstablelevel(path_to_csv: Path) -> list[MinStableSchema]:

    with open(path_to_csv) as file:
        reader = csv.DictReader(file)
        min_stable_level = [MinStableSchema.model_validate(row) for row in reader]
    
    return min_stable_level