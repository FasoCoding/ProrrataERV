from pathlib import Path

import sqlalchemy as sa
from pydantic import BaseModel

from prorrataerv.model.accdb import Querier
from prorrataerv.model.accdb.models import (
    ObjectRelationshipSchema,
    ResultDataSchema,
    ResultSchema,
)
from prorrataerv.model.accdb.schema import QuerySchema
from prorrataerv.model.banned import get_banned_list, pmgd_names
from prorrataerv.model.minstable import MinStableSchema, get_minstablelevel
from prorrataerv.model.pmgds import banned_names, get_pmgd_list

import polars as pl


class DataExtractor:
    def __init__(
        self, path_to_prg: str, path_to_pmgd_list: str, path_to_banned_list: str
    ):
        prg: Path = Path(path_to_prg)
        pmgd: Path = Path(path_to_pmgd_list)
        banned: Path = Path(path_to_banned_list)

        if not prg.exists() and not prg.is_dir():
            raise FileNotFoundError("No existe la ruta al prg.")

        if not pmgd.exists() and not pmgd.is_file():
            raise FileNotFoundError("No existe la ruta al archivo de pmgds.")

        if not banned.exists() and not banned.is_file():
            raise FileNotFoundError("No existe la ruta al archivo de centrales vetadas.")

        self.path_prg: Path = prg
        self.path_pmgd: Path = pmgd
        self.path_banned: Path = banned

    def run_accdb_queries(self) -> pl.DataFrame:
        """Inicia proceso de extracción de datos."""
        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            rf"DBQ={self.path_prg.absolute()};"
            r"ExtendedAnsiSQL=1;"
        )
        connection_url = sa.engine.URL.create(
            "access+pyodbc", query={"odbc_connect": connection_string}
        )

        accdb_engine = sa.create_engine(connection_url)
        with accdb_engine.connect() as conn:
            pcp_solution = Querier(conn=conn)
            self.gen_nodes = pcp_solution.get_obj_relationship(collection_id=12)
            self.generation = pcp_solution.get_property_t_data(
                query_enum=QuerySchema.GENERATOR.GENERATION,
                category_list=["Solar Farms"],
            )
            node_categories = [
                item.category_name
                for item in pcp_solution.get_category_list(category_type=22)
                if item.category_id != 22
            ]
            self.cmg = pcp_solution.get_property_data(
                query_enum=QuerySchema.NODE.PRICE, category_list=node_categories
            )

        self.pmgd_list = get_pmgd_list(self.path_pmgd)
        self.banned = get_banned_list(self.path_banned)
        self.min_stable = get_minstablelevel(self.pa)


class ExtractorProcessor(BaseModel):
    generation: ResultSchema
    units_generating: ResultSchema
    available_capacity: ResultSchema
    max_capacity: ResultSchema
    gen_nodes: ResultSchema
    marginal_cost: ResultSchema
    min_stable: MinStableSchema
    pmgd_list: list[pmgd_names]
    banned_list: list[banned_names]
