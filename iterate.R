library(rmarkdown)
library(stringr)
library(tidyverse)

# create an index
NEIGHBORHOODS <- c("Hacienda Heights",  "Westwood")

# create a data frame with parameters and output file names
reports <- tibble(
  filename = str_c(NEIGHBORHOODS, "-trends", ".html"),
  params = NEIGHBORHOODS
)


# iterate render() along the tibble of parameters and file names
runs %>%
  select(output_file = filename, params) %>%
  pwalk(rmarkdown::render, input = "test-rmarkdown.Rmd", output_dir = "notebooks/")