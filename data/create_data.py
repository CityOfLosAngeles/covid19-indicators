"""
Schedule this script to update our datasets.
"""
import jhu
import jhu_county
import sync_hospital
import sync_covid_testing
import sync_la_cases
import ca_ppe
import ca_hospital

jhu.load_global_covid_data()
jhu_county.append_county_time_series()

sync_hospital.update_bed_availability_data()
sync_covid_testing.update_covid_testing_city_county_data()
sync_la_cases.update_la_cases_data()

ca_ppe.update_ca_ppe()
ca_hospital.update_ca_surge_hospital_data()