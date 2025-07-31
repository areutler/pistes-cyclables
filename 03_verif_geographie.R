# Librairies à compléter
library(tidyverse)
library(arrow)
library(knitr)
library(gescodgeo)
library(zonages)

input_dir <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/inter/"

data <- open_dataset(
  sources = input_dir,
  format = "parquet",
  partitioning = schema(DEP = string())
) %>%
  collect()

check_cog(data, cog = 2024, from = code)
# 12218 14581 15031 15035 15047 15171 49126 69114


check_cog(data, cog = 2025, from = code, complete = TRUE)
# 39007 39021 39066 39250 97101 97102 97103 97104 97105 97106 [...]


39007 39021 39066 39250

cog_events("39007")
