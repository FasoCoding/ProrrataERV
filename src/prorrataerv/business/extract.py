from prorrataerv.model.accdb import Querier
from prorrataerv.model.accdb.schema import QuerySchema
from prorrataerv.model.minstable import get_minstablelevel
from prorrataerv.model.pmgds import get_pmgd_list
from prorrataerv.model.banned import get_banned_list

from pathlib import Path
import sqlalchemy as sa
import polars as pl

class DataExtractor:
    nodes: pl.DataFrame
    gen: pl.DataFrame
    cmg: pl.DataFrame
    pmgd: pl.DataFrame
    banned: pl.DataFrame
    path_prg: Path
    path_pmgd: Path
    path_banned: Path

    def __init__(self, path_prg: Path, path_pmgd: Path, path_banned: Path):
        self.path_prg = path_prg
        self.path_pmgd = path_pmgd
        self.path_banned = path_banned
        
    def extract_data(self) -> None:
        """Inicia proceso de extracción de datos.
        """
        connection_string = (
            r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
            rf"DBQ={self.path_prg.as_posix()};"
            r"ExtendedAnsiSQL=1;"
        )
        connection_url = sa.engine.URL.create(
            "access+pyodbc", query={"odbc_connect": connection_string}
        )

        engine = sa.create_engine(connection_url)
        with engine.connect() as conn:
            pcp_solution = Querier(conn=conn)
            self.nodes = pcp_solution.get_obj_relationship(collection_id=12)
            self.gen = pcp_solution.get_property_t_data(query_enum=QuerySchema.GENERATOR.GENERATION, category_list=["Solar Farms"])
            node_categories = [item.category_name for item in pcp_solution.get_category_list(category_type=22) if item.category_id != 22]
            self.cmg = pcp_solution.get_property_data(query_enum=QuerySchema.NODE.PRICE, category_list=node_categories)

        self.pmgd = get_pmgd_list(self.path_pmgd)
        self.banned = get_banned_list(self.path_banned)
