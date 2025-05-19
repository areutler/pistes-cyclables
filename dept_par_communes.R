# Test commune

library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(arrow)

# Récupère les objets "commune" dans le Bas-Rhin
communes_osm <- opq("Bas-Rhin, France") %>%
  add_osm_feature(key = "boundary", value = "administrative") %>%
  add_osm_feature(key = "admin_level", value = "8") %>%
  osmdata_sf()

# Extraire les polygones
communes_sf <- communes_osm$osm_multipolygons

# Codes et noms de commune
liste_id <- communes_sf$osm_id
liste_communes <- communes_sf$name
liste_codes_insee <- communes_sf$`ref:INSEE`

# Initialise le compteur
compteur_traitement <- 0

# resultats <- list()
for (i in seq_along(liste_communes)) {
  
  # Passe les communes déjà traitées
  if(i < compteur_traitement) next
  
  # Informations de la commune / du polygone
  id <- liste_id[i]
  nom_commune <- liste_communes[i]
  code_commune <- liste_codes_insee[i]
  poly_commune <- communes_sf[i, ]
  
  # Supprimer
  if(!nom_commune %in% c( "Seebach", "Friesenheim")) next
  
  message("Traitement ", i, "/", length(liste_communes), " : ", nom_commune, " osm_id = ", id)
  
  # Requête OSM sur la bbox de la commune
  req <- opq(bbox = st_bbox(poly_commune), timeout = 1000) %>%
    add_osm_feature(key = "highway") %>%
    osmdata_sf()
  
  lignes <- req$osm_lines
  
  # Intersection : en cas d'erreur on corrige avec st_make_valid
  lignes_commune <- tryCatch({
    st_intersection(lignes, poly_commune)
  }, error = function(e) {
    poly_commune <- st_make_valid(poly_commune)
    lignes_commune <- st_intersection(lignes, poly_commune)
    return(lignes_commune)
  })
  
  # Lambert 93
  lignes_commune <- lignes_commune %>% st_transform(crs=2154)
  
  # Calcul des longueurs par type
  lignes_commune$longueur <- st_length(lignes_commune)

  # Longueur selon le type de voirie
  longueur_voirie <- lignes_commune %>% 
    
    # Variables utiles
    select(any_of(c("highway","oneway","bicycle", "cycleway" , "maxspeed", "longueur"))) %>%
    st_drop_geometry() %>%
    
    # Longueur selon la description de la voirie
    group_by(pick(-"longueur")) %>% 
    summarise(longueur = sum(longueur, na.rm = TRUE), .groups = "drop") %>%
    
    # Information de la commune
    mutate(osm_id = !!id, code = !!code_commune, commune = !!nom_commune, .before = 1)
  
  # Résultat
  liste_resultats[[id]] <- longueur_voirie
  compteur_traitement <- i
}

# Résultat final
data_resultats <- bind_rows(liste_resultats)

# Fichier à enregistrer
BUCKET_OUT = "zg6dup"

# Enregistrement
aws.s3::s3write_using(
  df_resultats,
  FUN = arrow::write_parquet,
  object = "pistes_cyclables/longueur_voirie_68_temp.parquet",
  bucket = BUCKET_OUT,
  opts = list("region" = "")
)
