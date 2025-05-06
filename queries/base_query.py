from abc import ABC, abstractmethod
import hashlib


class BigQueryQuery(ABC):
    def __init__(self, project_id):
        self.project_id = project_id
        self.query = self._build_query()
        self.query_hash = self._hash_query()
        self.schema_definition = self._define_schema()
        self.x_axis = self.schema_definition.get("x_axis")
        self.y_axises = self.schema_definition.get("y_axises", [])

    @abstractmethod
    def _build_query(self):
        pass

    @abstractmethod
    def _define_schema(self):
        pass

    def _hash_query(self):
        return hashlib.md5(self.query.encode()).hexdigest()
