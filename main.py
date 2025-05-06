from queries.nbh_trend_query import NBHTrendQuery
from executors.bigquery_executor import BigQueryExecutor
from visualizations.plotter import visualize_results
from helpers import is_arabic

import concurrent.futures
import pandas as pd

if __name__ == "__main__":
    project_id = "noonbisearch"
    duration_months = 12
    bq_executor = BigQueryExecutor()

    # search_terms = ["shoes", "shoes for men", "clothes"]
    # search_term = "shoes"

    # n_b_h_trend_query = NBHTrendQuery(project_id, duration_months, search_term)
    # results_df = bq_executor.execute_query(n_b_h_trend_query)

    # if results_df is not None:
    #     visualize_results(results_df, n_b_h_trend_query.x_axis, n_b_h_trend_query.y_axises)



    # Read search terms from the Google 400 CSV
    csv_path = "./Google 400 products cases Attribution Tokens - Sheet1.csv"
    search_terms_df = pd.read_csv(csv_path)
    search_terms = search_terms_df['search_query'].tolist()

    # Filter out Arabic search terms
    search_terms = [term for term in search_terms if not is_arabic(term)]

    # Remove duplicates
    search_terms = list(set(search_terms))



    def process_search_term(term):
        n_b_h_trend_query = NBHTrendQuery(project_id, duration_months, term)
        results_df = bq_executor.execute_query(n_b_h_trend_query)

        if results_df is not None:
            # Find the minimum date where the result is >= 400
            filtered_df = results_df[results_df[n_b_h_trend_query.y_axises[0]] >= 400]
            if not filtered_df.empty:
                min_date = filtered_df[n_b_h_trend_query.x_axis].min()
                return {"search_query": term, "min_date": min_date}
        return None

    # # Use ThreadPoolExecutor for parallel processing
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_term = {executor.submit(process_search_term, term): term for term in search_terms}
        for future in concurrent.futures.as_completed(future_to_term):
            result = future.result()
            if result:
                results.append(result)

    # # Print or process the results
    # for result in results:
    #     print(f"Search Term: {result['search_query']}, Min Date: {result['min_date']}")