from typing import Self

from pydantic import BaseModel, Field, PositiveInt, model_validator


class Node(BaseModel):
    guid: str
    id: PositiveInt
    name: str
    marginal_cost: float


class Generator(BaseModel):
    guid: str
    id: PositiveInt
    name: str
    #max_capacity: PositiveInt
    #min_capacity: int = Field(default=0, ge=0)
    #node_to: Node


class Line(BaseModel):
    guid: str
    id: PositiveInt
    name: str
    node_from: Node
    node_to: Node

    @model_validator(mode="after")
    @classmethod
    def check_nodes(self) -> Self:
        node_a = self.node_from
        node_b = self.node_to
        if node_a.id == node_b.id:
            raise ValueError(f"Linea {self.name} esta conectada a si misma.")
        if node_a is None:
            raise ValueError(f"Linea {self.name} no tiene barra en lado A.")
        if node_b is None:
            raise ValueError(f"Linea {self.name} no tiene barra en lado B.")

        return self


class GeneratorData(BaseModel): ...
