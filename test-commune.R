# Test commune

library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(sfarrow)

library(dplyr)

resultats <- list()

for (i in seq_along(liste_communes)) {
  nom_commune <- liste_communes[i]
  poly_commune <- communes_sf[i, ]
  
  message("Traitement : ", nom_commune)
  
  # Requête OSM sur la bbox de la commune
  req <- opq(bbox = st_bbox(poly_commune)) %>%
    add_osm_feature(key = "highway") %>%
    osmdata_sf()
  
  lignes <- req$osm_lines
  
  # Filtrage géographique strict
  lignes_commune <- st_intersection(lignes, poly_commune)
  
  # Calcul des longueurs par type
  lignes_commune$longueur <- st_length(lignes_commune)
  
  long_total <- sum(lignes_commune$longueur, na.rm = TRUE)
  long_cyclable <- sum(lignes_commune$longueur[lignes_commune$cycleway != "" | lignes_commune$highway == "cycleway"], na.rm = TRUE)
  
  taux <- as.numeric(long_cyclable / long_total)
  
  resultats[[nom_commune]] <- tibble(
    commune = nom_commune,
    long_total = as.numeric(long_total),
    long_cyclable = as.numeric(long_cyclable),
    taux = taux
  )
}

# Résultat final
df_resultats <- bind_rows(resultats)

