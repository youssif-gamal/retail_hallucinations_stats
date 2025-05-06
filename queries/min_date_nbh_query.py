from queries.base_query import BigQueryQuery


class MinDateNBHQuery(BigQueryQuery):
    def __init__(self, project_id, duration_months, search_term, target_nbh):
        self.search_term = search_term
        self.target_nbh = target_nbh
        self.duration_months = duration_months
        super().__init__(project_id)

    def _build_query(self):
        return f"""
            SELECT
                DATE(event_time) AS event_date,
                MIN(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS min_nbh
            FROM
                `noonprd-mp-analytics.noon_analytics_tool.raw_events_v2`
            WHERE
                event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {self.duration_months} MONTH) AND CURRENT_DATE()
                AND property_code = 'noon'
                AND JSON_EXTRACT_SCALAR(event_misc, "$.mc") = "noon"
                AND event_type = 'page_catalog'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.st")) = '{self.search_term}'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.td.eid")) = 'google'
                AND JSON_VALUE(event_misc, "$.ed.cd.hit") = "false"
                AND JSON_EXTRACT_SCALAR(event_misc, "$.st") IS NOT NULL
                AND JSON_EXTRACT_SCALAR(event_misc, "$.curl") LIKE '/search?q={self.search_term}'
                AND JSON_EXTRACT_SCALAR(event_misc, "$.nbh") = "{self.target_nbh}"
            GROUP BY
                event_date
            ORDER BY
                event_date ASC
            LIMIT 1
        """

    def _define_schema(self):
        return {"x_axis": "event_date", "y_axises": ["min_nbh"]}
