"""
Functions to create charts.
"""
import altair as alt
import altair_saver
import os
import pandas as pd
import utils

from IPython.display import display, SVG

alt.renderers.enable('altair_saver', fmts=['svg'])

def show_svg(image_path):
    display(SVG(image_path))
    os.remove(image_path)

    
#---------------------------------------------------------------#
# Chart parameters
#---------------------------------------------------------------#
navy = "#0A4C6A"
maroon = "#A30F23"
green = "#10DE7A"
orange = "#FCA800"

title_font_size = 10
font_name = "Roboto"
grid_opacity = 0.4
domain_opacity = 0.4
stroke_opacity = 0
time_unit = "monthdate"
chart_width = 250
chart_height = 200
bin_spacing = 100
fulldate_format = "%-m/%-d/%y"
monthdate_format = "%-m/%-d"


#---------------------------------------------------------------#
# Case Data (County, State, MSA)
#---------------------------------------------------------------#
def make_cases_deaths_chart(df, geog, name):
    # Define chart titles
    if geog == "county":
        chart_title = f"{name} County"
    if geog == "state":
        chart_title = f"{name}"
    if geog == "msa":
        chart_title = f"{name} MSA"

    # Make cases charts
    cases_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit=time_unit, 
                    title="date", axis=alt.Axis(format=monthdate_format)
                   ),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value(navy),
        )
        .properties(
            title=f"{chart_title}: New Cases", width=chart_width, height=chart_height
        )
    )

    # Make deaths chart
    deaths_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit=time_unit, 
                    title="date", axis=alt.Axis(format=monthdate_format)
                   ),
            y=alt.Y("deaths_avg7", title="7-day avg"),
            color=alt.value(maroon),
        )
        .properties(
            title=f"{chart_title}: New Deaths", width=chart_width, height=chart_height
        )
    )

    combined_chart = (
        alt.hconcat(cases_chart, deaths_chart)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(gridOpacity=grid_opacity, domainOpacity=domain_opacity)
        .configure_view(strokeOpacity=stroke_opacity)
    )
        
    altair_saver.save(combined_chart, "../notebooks/combined_chart.svg")
    show_svg("../notebooks/combined_chart.svg")


#---------------------------------------------------------------#
# Case Data (City of LA)
#---------------------------------------------------------------#
def make_lacity_cases_chart(df):
    # Make cases charts
    cases_chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("date", timeUnit=time_unit, 
                    title="date", axis=alt.Axis(format=monthdate_format)
                   ),
            y=alt.Y("cases_avg7", title="7-day avg"),
            color=alt.value(navy),
        )
        .properties(
            title="City of LA: New Cases", width=chart_width, height=chart_height
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(gridOpacity=grid_opacity, domainOpacity=domain_opacity)
        .configure_view(strokeOpacity=stroke_opacity)
    )

    altair_saver.save(cases_chart, "../notebooks/cases_chart.svg")
    show_svg("../notebooks/cases_chart.svg")   

    
#---------------------------------------------------------------#
# Testing Data (LA County and City of LA)
#---------------------------------------------------------------#
def make_la_testing_chart(df, plot_col, chart_title, lower_bound, upper_bound):
    chart_width = 400
        
    bar = (
        alt.Chart(df)
        .mark_bar(color=navy)
        .encode(
            x=alt.X(
                "date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format=monthdate_format),
            ),
            y=alt.Y(plot_col, title="Tests Performed"),
        )
    )

    line1 = (
        alt.Chart(pd.DataFrame({"y": [lower_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )
    line2 = (
        alt.Chart(pd.DataFrame({"y": [upper_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )

    testing_chart = (
        (bar + line1 + line2)
        .properties(title=chart_title, width=chart_width)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    altair_saver.save(testing_chart, "../notebooks/testing_chart.svg")
    show_svg("../notebooks/testing_chart.svg")   

    
#---------------------------------------------------------------#
# Share of Positive Tests by Week (LA County)
#---------------------------------------------------------------#
def make_la_positive_test_chart(df, positive_lower_bound, positive_upper_bound, chart_title1, chart_title2): 
    chart_width = 200
    positive_bar = (
        alt.Chart(df)
        .mark_bar(color = navy, binSpacing = bin_spacing)
        .encode(
            x=alt.X(
                "week2",
                title="date",
            ),
            y=alt.Y(
                "pct_positive", 
                title="Share of Positive COVID-19 Results",
                axis=alt.Axis(format="%")
            ),
        )

    )
    
    positive_lower_line = (
        alt.Chart(pd.DataFrame({"y": [positive_lower_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )  
    
    positive_upper_line = (
        alt.Chart(pd.DataFrame({"y": [positive_upper_bound]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    ) 
    
    positive_chart = (
        (positive_bar + positive_lower_line + positive_upper_line)
            .properties(title=chart_title1, width = chart_width)
            .configure_title(
                  fontSize=title_font_size, font=font_name, anchor="middle", color="black"
               )
            .configure_axis(
                gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
            )
            .configure_view(strokeOpacity=stroke_opacity)
         )

    
    test_bar = (
        alt.Chart(df)
        .mark_bar(color = orange, binSpacing = bin_spacing)
        .encode(
            x=alt.X(
                "week2",
                title="date",
            ),
            y=alt.Y(
                "weekly_tests", 
                title="# Weekly Tests",
            ),
        )
        .properties(title=chart_title2, width = chart_width)
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )
    
    altair_saver.save(positive_chart, "../notebooks/positive_chart.svg")
    show_svg("../notebooks/positive_chart.svg")  
    altair_saver.save(test_bar, "../notebooks/test_bar.svg")
    show_svg("../notebooks/test_bar.svg")   
    
    
#---------------------------------------------------------------#
# Hospital Equipment Availability (LA County)
#---------------------------------------------------------------#
def make_lacity_hospital_chart(df):
    chart_width = 400
    acute_color = green
    icu_color = navy
    ventilator_color = orange

    base = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(
                "date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format=monthdate_format),
            ),
            y=alt.Y("pct_available", title="% available", 
                    axis=alt.Axis(format="%")
            ),
            color=alt.Color(
                "equipment",
                scale=alt.Scale(
                    domain=["Acute Care Beds", "ICU Beds", "Ventilators"],
                    range=[acute_color, icu_color, ventilator_color],
                ),
            ),
        )
    )

    line1 = (
        alt.Chart(pd.DataFrame({"y": [0.3]}))
        .mark_rule(color=maroon, strokeDash=[5, 2])
        .encode(y="y")
    )

    hospital_pct_chart = (
        (base + line1)
        .properties(
            title="Percent of Available Hospital Equipment by Type",
            width=chart_width,
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    base2 = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X(
                "date",
                timeUnit=time_unit,
                title="date",
                axis=alt.Axis(format=monthdate_format),
            ),
            y=alt.Y("n_available", title="# available"),
            color=alt.Color(
                "equipment",
                scale=alt.Scale(
                    domain=["Acute Care Beds", "ICU Beds", "Ventilators"],
                    range=[acute_color, icu_color, ventilator_color],
                ),
            ),
        )
    )

    hospital_num_chart = (
        base2.properties(
            title="Number of Available Hospital Equipment by Type", width=chart_width
        )
        .configure_title(
            fontSize=title_font_size, font=font_name, anchor="middle", color="black"
        )
        .configure_axis(
            gridOpacity=grid_opacity, domainOpacity=domain_opacity, ticks=False
        )
        .configure_view(strokeOpacity=stroke_opacity)
    )

    altair_saver.save(hospital_pct_chart, "../notebooks/hospital_pct_chart.svg")
    show_svg("../notebooks/hospital_pct_chart.svg") 
    altair_saver.save(hospital_num_chart, "../notebooks/hospital_num_chart.svg")
    show_svg("../notebooks/hospital_num_chart.svg")