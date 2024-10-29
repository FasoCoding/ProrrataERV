from pydantic import BaseModel
import datetime as dt

class ObjectRelationshipSchema(BaseModel):
    parent_obj: str
    child_obj: str

class CollectionDataSchema(BaseModel):
    name: str
    property: str
    datetime: dt.datetime
    value: float

class GeneratorsSchema(BaseModel):
    generator: str
    property: str
    datetime: dt.datetime
    data_key: int
    data_period: int
    value: float

class DataSchema(BaseModel):
    key_id: int
    period_id: int
    value: float

class ReservesSchema(BaseModel):
    generator: str
    property: str
    datetime: dt.datetime
    value: float