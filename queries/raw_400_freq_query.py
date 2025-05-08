from queries.base_query import BigQueryQuery
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

class Frequency400NBHQuery(BigQueryQuery):
    def __init__(self, project_id, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        super().__init__(project_id)

    def _build_query(self):
        return f"""
            SELECT
                DATE(event_time) AS event_date,
                COUNT(*) AS frequency_400_hits
            FROM
                `noonprd-mp-analytics.noon_analytics_tool.raw_events_v2`
            WHERE
                event_date BETWEEN DATE('{self.start_date}') AND DATE('{self.end_date}')
                AND property_code = 'noon'
                AND JSON_EXTRACT_SCALAR(event_misc, "$.mc") = "noon"
                AND event_type = 'page_catalog'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.td.eid")) = 'google'
                AND JSON_EXTRACT_SCALAR(event_misc, "$.nbh") = "400"
                AND JSON_VALUE(event_misc, "$.ed.cd.hit") = "false"
            GROUP BY
                event_date
            ORDER BY
                event_date
        """

    def _define_schema(self):
        return {"x_axis": "event_date", "y_axises": ["frequency_400_hits"]}