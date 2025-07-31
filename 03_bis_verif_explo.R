################################################################################

# Librairies à compléter
library(tidyverse)
library(arrow)
library(knitr)
library(gescodgeo)
library(zonages)

# dir <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/output/"
# data <- open_dataset(sources = dir, format = "parquet") %>% collect()

data %>%
  filter(cyclable_det %in% c("PISTE")) %>%
  mutate(CROSSING = ifelse(cycleway=="crossing", TRUE, NA)) %>%
  group_by(cyclable_det, voie_routiere, CROSSING) %>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT)) %>% View()



data %>%
  mutate(CROSSING = ifelse(cycleway=="crossing", TRUE, NA)) %>%
  filter(CROSSING) %>%
  group_by(cyclable_det, voie_routiere, CROSSING) %>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT)) %>% View()

data %>%
  filter(cyclable_det %in% c("APAISE","APAISE NON ROUTIER")) %>%
  group_by(cyclable_det, voie_routiere) %>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT))

data %>%
  filter(voie_routiere == "AUTRE VOIE POTENTIELLEMENT CYCLABLE") %>%
  group_by(cyclable_det) %>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT))

# Incohérence : normalement quand il y use_sidepath c'est pas cyclable
# On ignore.
data %>%
  filter(bicycle=="use_sidepath") %>%
  group_by(cyclable_det)%>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT))


data %>%
  filter(cyclable_det=="APAISE NON ROUTIER") %>%
  group_by(sens_voiture)%>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT))




data %>%
  filter(sens_velo != 0) %>%
  group_by(cyclable_det, sens_velo) %>%
  summarise(longueur = sum(longueur), .groups = "drop") %>%
  mutate(pct = 100*longueur/sum(longueur)) %>%
  arrange(desc(pct))
