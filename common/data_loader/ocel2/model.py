from datetime import datetime
from typing import Optional, List, Union
from pydantic import BaseModel


class Attribute(BaseModel):
    name: str
    type: str


class AttributeValue(BaseModel):
    name: str
    value: Optional[Union[str, int, float, bool]]


class EventType(BaseModel):
    name: str
    attributes: List[Attribute]


class ObjectType(BaseModel):
    name: str
    attributes: List[Attribute]


class ObjectRelationship(BaseModel):
    objectId: str
    qualifier: str


class OCEL2Event(BaseModel):
    id: str
    type: str
    time: datetime
    attributes: Optional[List[AttributeValue]] = []
    relationships: Optional[List[ObjectRelationship]] = []


class OCEL2Object(BaseModel):
    id: str
    type: str
    relationships: Optional[List[ObjectRelationship]] = []
    attributes: Optional[List[AttributeValue]] = []


class OCEL2JSON(BaseModel):
    eventTypes: List[EventType]
    objectTypes: List[ObjectType]
    events: List[OCEL2Event]
    objects: List[OCEL2Object]
