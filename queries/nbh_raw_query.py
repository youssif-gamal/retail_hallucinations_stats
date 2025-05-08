from queries.base_query import BigQueryQuery
from datetime import date, timedelta
import helpers


class SimpleNBHQuery(BigQueryQuery):
    def __init__(self, project_id, duration_days, search_term, direction):
        self.search_term = search_term
        self.duration_days = duration_days
        self.direction = direction.lower()
        super().__init__(project_id)

    def _build_query(self):
        end_date = date.today()
        if self.direction == "before":
            start_date = end_date - timedelta(days=self.duration_days)
        elif self.direction == "after":
            start_date = end_date
            end_date = start_date + timedelta(days=self.duration_days)
        else:
            raise ValueError("Invalid direction. Must be 'before' or 'after'.")

        return f"""
            SELECT
                DATE(event_time) AS event_date,
                CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC) AS nbh
            FROM
                `noonprd-mp-analytics.noon_analytics_tool.raw_events_v2`
            WHERE
                event_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
                AND property_code = 'noon'
                AND JSON_EXTRACT_SCALAR(event_misc, "$.mc") = "noon"
                AND event_type = 'page_catalog'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.st")) = '{self.search_term}'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.td.eid")) = 'google'
                AND JSON_VALUE(event_misc, "$.ed.cd.hit") = "false"
                AND JSON_EXTRACT_SCALAR(event_misc, "$.st") IS NOT NULL
                AND JSON_EXTRACT_SCALAR(event_misc, "$.curl") LIKE '/search?q={self.search_term}%'
            ORDER BY
                event_date, nbh
        """

    def _define_schema(self):
        return {"x_axis": "event_date", "y_axises": ["nbh"]}


class SimpleNBHDateRangeQuery(BigQueryQuery):
    def __init__(self, project_id, start_date, end_date, search_term , country_code = None , nbh = None): 
        self.search_term = search_term
        self.countyry_code = country_code
        self.nbh = nbh
        self.start_date = start_date
        self.end_date = end_date
        super().__init__(project_id)

    def _build_query(self):
        
        country_code_condition = ""
        nbh_condition = ""
        if self.countyry_code:
            country_code_condition = f"AND locale = 'en-{self.countyry_code}'"

        if self.nbh:
            nbh_condition = f"AND JSON_EXTRACT_SCALAR(event_misc, '$.nbh') = '{self.nbh}'"
            
        return f"""
            SELECT
                DATE(event_time) AS event_date,
                CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC) AS nbh
            FROM
                `noonprd-mp-analytics.noon_analytics_tool.raw_events_v2`
            WHERE
                event_date BETWEEN DATE('{self.start_date}') AND DATE('{self.end_date}')
                AND property_code = 'noon'
                {country_code_condition}
                AND JSON_EXTRACT_SCALAR(event_misc, "$.mc") = "noon"
                AND event_type = 'page_catalog'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.st")) = '{self.search_term}'
                AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.td.eid")) = 'google'
                AND JSON_VALUE(event_misc, "$.ed.cd.hit") = "false"
                {nbh_condition}
                AND JSON_EXTRACT_SCALAR(event_misc, "$.st") IS NOT NULL
                AND JSON_EXTRACT_SCALAR(event_misc, "$.curl") LIKE '/search?q={helpers.url_encode_query(self.search_term)}'
            ORDER BY
                event_date, nbh
        """

    def _define_schema(self):
        return {"x_axis": "event_date", "y_axises": ["nbh"]}
