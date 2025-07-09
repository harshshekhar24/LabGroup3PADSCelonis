import uuid
from abc import ABC, abstractmethod
from collections import defaultdict

import pandas as pd

ViewGroupName = str
ViewName = str


class SQLAccessor(ABC):

    views: dict[ViewGroupName, list[str]] = defaultdict(list)

    @abstractmethod
    def execute_query(self, query: str) -> pd.DataFrame:
        pass

    def create_view(self, view_group: ViewGroupName, view_definition: str) -> str:
        view_name = "f" + str(uuid.uuid4()).replace("-", "_")
        self.views[view_group].append(view_name)
        self.execute_query(f"""CREATE VIEW {view_name} AS {view_definition};""")
        return view_name

    def delete_view_subgroup(self, view_group: ViewGroupName) -> None:
        for view_name in self.views[view_group]:
            self._delete_view(view_name)
        del self.views[view_group]

    def _delete_view(self, view_name: ViewName) -> None:
        self.execute_query(f"""DROP VIEW IF EXISTS {view_name};""")

    def __del__(self) -> None:
        for view_group in self.views:
            self.delete_view_subgroup(view_group)
