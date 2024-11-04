import datetime as dt

from pydantic import BaseModel


class ObjectRelationshipSchema(BaseModel):
    parent_obj: str
    child_obj: str


class ResultSchema(BaseModel):
    collection: str
    property: str
    name: str
    datetime: dt.datetime
    value: float


class ResultDataSchema(BaseModel):
    collection: str
    property: str
    name: str
    datetime: dt.datetime
    key_id: int
    period_id: int
    value: float
