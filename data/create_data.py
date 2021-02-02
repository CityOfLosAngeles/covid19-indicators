"""
Schedule this script to update our datasets.
Less frequent updates.
"""
import jhu
import jhu_county

jhu.load_global_covid_data()
jhu_county.append_county_time_series()