"""
Schedule this script to update our datasets.
Less frequent updates.
"""
import jhu
import jhu_county

import ca_hospital

jhu.load_global_covid_data()
jhu_county.append_county_time_series()

ca_hospital.update_ca_surge_hospital_data()

print("Successful update of occasional data")