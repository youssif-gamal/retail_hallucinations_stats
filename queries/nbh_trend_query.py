from queries.base_query import BigQueryQuery
import helpers
class NBHTrendQuery(BigQueryQuery):
    def __init__(self, project_id, duration_months, search_term):
        self.search_term = search_term
        self.duration_months = duration_months
        super().__init__(project_id)

    def _build_query(self):
        return f"""
            SELECT
                DATE(event_time) AS event_date,
                MIN(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS min_nbh,
                MAX(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS max_nbh,
                AVG(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS avg_nbh
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
            GROUP BY
                event_date
            ORDER BY
                event_date
        """

    def _define_schema(self):
        return {
            "x_axis": "event_date",
            "y_axises": ["min_nbh", "max_nbh", "avg_nbh"]
        }
    

class NBHTrendInvalidQueries(BigQueryQuery) : 

    def __init__(self, project_id, duration_months, search_terms , avg_threshold , conditions:list = [] , nbh = 400):
        self.project_id = project_id
        self.search_terms = search_terms
        self.duration_months = duration_months
        self.avg_threshold = avg_threshold
        self.conditions = conditions
        self.nbh = nbh
        super().__init__(project_id)


    def _build_query(self):
        encoded_search_terms = list(f"/search?q={helpers.url_encode_query(term)}" for term in self.search_terms)

        _search_terms = "("
        for term in self.search_terms:
            _search_terms += f"'{term}',"

        _search_terms = _search_terms[:-1] + ")"

        _encoded_search_terms = "("
        for term in encoded_search_terms:
            _encoded_search_terms += f"'{term}',"
        _encoded_search_terms = _encoded_search_terms[:-1] + ")"

        conditions = ""
        if self.conditions:
            for condition in self.conditions:
                conditions += f"AND {condition} ,"
        
        return f"""
                SELECT
                    lower(JSON_EXTRACT_SCALAR(event_misc, "$.st")) AS search_term,
                    ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)), 2) AS avg_nbh,
                    MIN(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) AS min_nbh,
                    ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) * {self.avg_threshold}, 2) AS avg_threshold
                FROM
                    `noonprd-mp-analytics.noon_analytics_tool.raw_events_v2`
                WHERE
                    event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL {self.duration_months} MONTH) AND CURRENT_DATE()
                    AND property_code = 'noon'
                    {conditions}
                    AND JSON_EXTRACT_SCALAR(event_misc, "$.mc") = "noon"
                    AND event_type = 'page_catalog'
                    AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.td.eid")) = 'google'  
                    AND lower(JSON_EXTRACT_SCALAR(event_misc, "$.st")) IN {_search_terms}
                    AND JSON_VALUE(event_misc, "$.ed.cd.hit") = "false" 
                    AND JSON_EXTRACT_SCALAR(event_misc, "$.st") IS NOT NULL
                    AND JSON_EXTRACT_SCALAR(event_misc, "$.curl") IN {_encoded_search_terms}
                GROUP BY
                    search_term
                HAVING
                    AVG(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC))*0.05 > {self.nbh}
                    AND MIN(CAST(JSON_EXTRACT_SCALAR(event_misc, "$.nbh") AS BIGNUMERIC)) <= {self.nbh}
                ORDER BY
                    avg_nbh DESC;
        """
    
    def _define_schema(self):
        return {}