from queries.base_query import BigQueryQuery


class NBHTrendDateQuery(BigQueryQuery):
    def __init__(
        self, project_id, base_date, time_delta_days, search_term, direction="after"
    ):
        """
        Initialize the query with a base date, time delta, and direction.

        :param project_id: BigQuery project ID
        :param base_date: The base date for filtering (YYYY-MM-DD format)
        :param time_delta_days: Number of days to look before or after the base date
        :param search_term: The search term to filter the data
        :param direction: "after" for data after the base date, "before" for data before the base date
        """
        self.base_date = base_date
        self.time_delta_days = time_delta_days
        self.search_term = search_term
        self.direction = direction.lower()
        super().__init__(project_id)

    def _build_query(self):
        # Determine the date range based on the direction
        if self.direction == "after":
            date_filter = f"event_date BETWEEN DATE('{self.base_date}') AND DATE_ADD(DATE('{self.base_date}'), INTERVAL {self.time_delta_days} DAY)"
        elif self.direction == "before":
            date_filter = f"event_date BETWEEN DATE_SUB(DATE('{self.base_date}'), INTERVAL {self.time_delta_days} DAY) AND DATE('{self.base_date}')"
        else:
            raise ValueError("Invalid direction. Use 'after' or 'before'.")

        return f"""
            SELECT
                DATE(event_time) AS event_date,
                MIN(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS min_nbh,
                MAX(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS max_nbh,
                AVG(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS avg_nbh
            FROM
                `noonprd-mp-analytics.noon_analytics_tool.raw_events_v2`
            WHERE
                {date_filter}
                AND property_code = 'noon'
                AND JSON_EXTRACT_SCALAR(event_misc, "$.mc") = "noon"
                AND event_type = 'page_catalog'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.st")) = '{self.search_term}'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.td.eid")) = 'google'
                AND JSON_VALUE(event_misc, "$.ed.cd.hit") = "false"
                AND JSON_EXTRACT_SCALAR(event_misc, "$.st") IS NOT NULL
                AND JSON_EXTRACT_SCALAR(event_misc, "$.curl") LIKE '/search?q={self.search_term}'
            GROUP BY
                event_date
            ORDER BY
                event_date
        """

    def _define_schema(self):
        return {"x_axis": "event_date", "y_axises": ["min_nbh", "max_nbh", "avg_nbh"]}
