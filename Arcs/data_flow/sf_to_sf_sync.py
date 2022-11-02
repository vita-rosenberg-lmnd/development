# /Users/vita.rosenberg/Development/Arcs/data_flow/sf_to_sf_sync.py

from __future__ import annotations
from typing import TYPE_CHECKING
from abc import abstractmethod

if TYPE_CHECKING:
    from clients.snowflake.snowflake_client import SnowflakeClient
    from data_flow.metric_flusher import MetricFlusher
    from config.table.snowflake_schema import SnowflakeSchema

from clients.snowflake.utils import *
from data_source.snowflake_data_source import SnowflakeSourceSink


class SfToSfSync(SnowflakeSourceSink):
    def __init__(self, client: SnowflakeClient, metric_flusher: MetricFlusher):
        super().__init__(client, metric_flusher)
        ensure_table_created(
            self.client,
            self.sink_table_schema(),
            self.sink_schema_name(),
            self.sink_table_name(),
        )

        self._sql = None

    def sql(self) -> str:
        if self._sql:
            return self._sql

        column_names = list(self.columns().keys())
        sql = f"""
            MERGE INTO
                {self.sink_schema_name()}.{self.sink_table_name()}
            USING (
                {self.staged_data()}
            ) staged
                ON {self.on_sql()}
            WHEN
                NOT matched
            THEN INSERT (
                {get_insert_columns(column_names)}
            )
            VALUES (
                {get_insert_columns(column_names, 'staged')}
            )
        """

        matched_sql = self.matched_sql()
        if matched_sql:
            sql += f"""
                WHEN
                    matched
                    AND {matched_sql}
                THEN
                    UPDATE SET
                        {set_column_values_to_stage_values_sql(column_names, 'staged')}
            """

        self._sql = sql
        print(sql)
        return self._sql

    def staged_data(self) -> str:
        last_synced = self.last_synced()
        last_synced = (
            f"'{last_synced}'" if isinstance(last_synced, str) else last_synced
        )

        sql = f"""
            SELECT
                {get_select_columns(self.columns())}
            FROM
              {self.src_schema_name()}.{self.src_table_name()}
            WHERE
                {self.sync_column()} > {last_synced}
            ORDER BY
                {self.sync_column()} ASC
            LIMIT
                {self.limit()}
        """
        return sql

    def on_sql(self) -> str:
        cols = []
        for col in self.identifier_columns():
            cols.append(
                f"{self.sink_schema_name()}.{self.sink_table_name()}.{col} = staged.{col}\n"
            )

        sql = " AND ".join(cols)
        return sql

    def matched_sql(self) -> Optional[str]:
        sync_col = self.sync_column()
        sql = f"staged.{sync_col} > {self.sink_schema_name()}.{self.sink_table_name()}.{sync_col}"
        return sql

    def sync_column(self) -> str:
        return "updated_at"

    def identifier_columns(self) -> List:
        return ["id"]

    def limit(self) -> int:
        return 1000000  # 1M

    @abstractmethod
    def src_schema_name(self) -> str:
        pass

    @abstractmethod
    def src_table_name(self) -> str:
        pass

    @abstractmethod
    def sink_schema_name(self) -> str:
        pass

    @abstractmethod
    def sink_table_name(self) -> str:
        pass

    @abstractmethod
    def sink_table_schema(self) -> SnowflakeSchema:
        pass

    @abstractmethod
    def last_synced(self):
        pass

    @abstractmethod
    def columns(self) -> dict:
        pass
