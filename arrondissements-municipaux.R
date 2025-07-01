# Test commune

library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(arrow)

liste_dep_osm <- c("13" = "Bouches-du-Rhône",  "69" = "Rhône", "75" = "Paris")

# Fait 01 - 44, 67, 68

# Amélioration de la méthode à partir de 26
for(code_dep in liste_code_dep) {
  # for(code_dep in c("26","27")) {

  code_dep <-"13"
    
  gc()
  
  message(code_dep, " ", liste_dep_osm[code_dep])
  
  # Fichier à enregistrer
  BUCKET <- "zg6dup"
  FILE <- paste0("pistes_cyclables/longueur_voirie_",code_dep ,".parquet")
  
  # Récupère les objets "arrondissements municipaux" dans le dépt "Nom-du-dépt, France"
  arm_osm <- paste0(liste_dep_osm[code_dep], ", France") %>%
    opq() %>%
    add_osm_feature(key = "boundary", value = "administrative") %>%
    add_osm_feature(key = "admin_level", value = "9") %>%
    osmdata_sf()
  
  arm_sf <- arm_osm$osm_multipolygons %>% 
    filter(`admin_type:FR` == "arrondissement municipal") %>%
    filter(substr(`ref:INSEE`,1,2) == !!code_dep)

  # Visualiser :
  arm_sf %>% st_union() %>% plot()
  
  ##############################################################################
  ##############################################################################
  
  
  # Codes et noms de commune
  liste_id <- communes_sf$osm_id
  liste_communes <- communes_sf$name
  liste_codes_insee <- communes_sf$`ref:INSEE`
  
  # Initialise le compteur et la liste 
  compteur_traitement <- 0
  liste_resultats <- list()
  
  # Boucle sur les communes (peut être relancée si erreur)
  for (i in seq_along(liste_communes)) {
    
    # Passe les communes déjà traitées
    if(i <= compteur_traitement) next
    
    # Informations de la commune / du polygone
    id <- liste_id[i]
    nom_commune <- liste_communes[i]
    code_commune <- liste_codes_insee[i]
    poly_commune <- communes_sf[i, ]
    
    # Communes buggée (à réessayer avec nouvelle version)
    # if(code_commune == "2B159") next
    
    message("Traitement ", i, "/", length(liste_communes), " : ",  code_commune, " ", nom_commune)
    
    # Requête OSM sur la bbox de la commune
    req <- opq(bbox = st_bbox(poly_commune), timeout = 1000) %>%
      add_osm_feature(key = "highway")
    
    # On essaie deux fois pour réduire le risque d'erreur 
    req <- tryCatch(
      expr = {req %>% osmdata_sf() }, 
      error = function(e) {
        message("... Deuxième tentative...")
        tryCatch(
          expr = {req %>% osmdata_sf() }, 
          error = function(e) {
            message("... Troisième tentative...")
            req %>% osmdata_sf() 
          }
        )
      }
    )
    
    lignes <- req$osm_lines
    
    # Intersection : en cas d'erreur on corrige avec st_make_valid + st_buffer
    lignes_commune <- tryCatch({
      st_intersection(lignes, poly_commune)
    }, error = function(e) {
      message("... Nétoyage avec st_make_valid()...")
      poly_commune <- st_make_valid(poly_commune)
      lignes_commune <- tryCatch({
        st_intersection(lignes, poly_commune)
      }, error = function(e) {
        # Nétoyage avec st_buffer
        message("... Nétoyage avec st_buffer()...")
        poly_commune <- st_buffer(poly_commune, 0)
        return(st_intersection(lignes, poly_commune))
      })
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
  
  # Enregistrement
  aws.s3::s3write_using(
    data_resultats,
    FUN = arrow::write_parquet,
    object = FILE,
    bucket = BUCKET,
    opts = list("region" = "")
  )
}

