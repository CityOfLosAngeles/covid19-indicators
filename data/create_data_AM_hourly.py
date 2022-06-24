"""
Schedule this script to update our datasets.
Hourly updates because these depend on Google sheets.
"""
import la_neighborhood

import sync_covid_testing
import sync_la_cases

la_neighborhood.update_neighborhood_data()

sync_la_cases.update_la_cases_data()
sync_covid_testing.update_covid_testing_city_county_data()

print("Successful update of hourly data")