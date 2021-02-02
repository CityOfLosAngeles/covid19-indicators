"""
Schedule this script to update our datasets.
Hourly updates because these depend on Google sheets.
"""
import sync_covid_testing
import sync_la_cases
import ca_hospital
import la_neighborhood

sync_covid_testing.update_covid_testing_city_county_data()
sync_la_cases.update_la_cases_data()

ca_hospital.update_ca_surge_hospital_data()

la_neighborhood.update_neighborhood_data()