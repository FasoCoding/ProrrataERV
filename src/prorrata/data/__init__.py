from prorrata.data.pmgd import PMGDsModel
from prorrata.data.banned import BannedModel
from prorrata.data.plexos import PlexosExtractModel
from prorrata.data.minstablelevel import MinStableLevelModel

from dataclasses import dataclass

import polars as pl

@dataclass
class InputsModel:
    pmgd: PMGDsModel
    banned: BannedModel
    solution: PlexosExtractModel
    min_stable: MinStableLevelModel

    @classmethod
    def from_pcp(cls, path_pcp: str):
        return cls(
            pmgd=PMGDsModel.from_excel(),
            banned=BannedModel.from_excel(),
            solution=PlexosExtractModel.from_accdb(path_pcp),
            min_stable=MinStableLevelModel.from_csv(path_pcp)
        )
    
    @property
    def banned_list(self) -> list[str]:
        return self.pmgd.centrales + self.banned.centrales
    
    @property
    def erv(self) -> pl.DataFrame:
        return self.solution.erv
    
    @property
    def cmg(self) -> pl.DataFrame:
        return self.solution.cmg
    
    @property
    def nodes(self) -> pl.DataFrame:
        return self.solution.nodes
    
    @property
    def inf(self) -> pl.DataFrame:
        return self.solution.inf
    
    @property
    def mt(self) -> pl.DataFrame:
        return self.min_stable.data
