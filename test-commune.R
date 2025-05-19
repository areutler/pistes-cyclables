# Test commune

library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(sfarrow)

# Récupère les objets "commune" dans le Bas-Rhin
communes_osm <- opq("Bas-Rhin, France") %>%
  add_osm_feature(key = "boundary", value = "administrative") %>%
  add_osm_feature(key = "admin_level", value = "8") %>%
  osmdata_sf()

# Extraire les polygones
communes_sf <- communes_osm$osm_multipolygons

liste_communes <- communes_sf$name
liste_codes_insee <- communes_sf$`ref:INSEE`

resultats <- list()
for (i in seq_along(liste_communes)) {
  nom_commune <- liste_communes[i]
  code_commune <- liste_codes_insee[i]
  poly_commune <- communes_sf[i, ]
  
  message("Traitement : ", nom_commune)
  
  # Requête OSM sur la bbox de la commune
  req <- opq(bbox = st_bbox(poly_commune)) %>%
    add_osm_feature(key = "highway") %>%
    osmdata_sf()
  
  lignes <- req$osm_lines
  
  # Filtrage géographique strict
  lignes_commune <- st_intersection(lignes, poly_commune)
  
  # Lambert 93
  lignes_commune <- lignes_commune %>% st_transform(crs=2154)
  
  # Calcul des longueurs par type
  lignes_commune$longueur <- st_length(lignes_commune)

  # Longueur selon le type de voirie
  longueur_voirie <- lignes_commune %>% 
    select(any_of(c("highway","oneway","bicycle", "cycleway" , "maxspeed", "longueur"))) %>%
    st_drop_geometry() %>% 
    group_by(pick(-"longueur")) %>% 
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop") %>%
    mutate(code = code_commune, commune = nom_commune, .before = 1)

  resultats[[nom_commune]] <- longueur_voirie
}

# Résultat final
df_resultats <- bind_rows(resultats)
