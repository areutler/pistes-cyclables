library(tidyverse)
library(osmdata)
library(sf)
library(aws.s3)
library(sfarrow)

# Niveau départemental pour OSM
NIV_OSM <- 6

# Boucle sur les départements
# TERRITOIRE <- "Var" ;  CODE <- "83"
TERRITOIRE <- "Ain" ; CODE <- "01"

# Fichier à enregistrer
BUCKET_OUT = "zg6dup"
FILE_KEY_OUT_S3 = paste0("pistes_cyclables/lineaire", CODE, ".csv")

# creation de la requete OSM pour les voiries
requete_voirie <- getbb(TERRITOIRE) %>% 
  opq(timeout = 1000) %>% #augmenter le temps suivant message d'erreur
  add_osm_feature("highway")

names(requete_voirie)

# execution de la requete
voirie <- osmdata_sf(requete_voirie) 

# nettoyage pour ne conserver que la voirie (=lignes ; attention il y aurait peut-etre
# certaines multilines a conserver) et les variables utiles puis re-projection en L93
voirie_lignes <- voirie$osm_lines %>% 
  select(highway,oneway,bicycle, cycleway , maxspeed)%>%
  st_transform(crs=2154) %>% 
  group_by(highway,oneway,bicycle, cycleway , maxspeed) %>% 
  summarise()

# creation de la requete OSM pour le territoire d'interet (!= bbox)
requete_contour <-  getbb(TERRITOIRE) %>% 
  opq() %>%
  add_osm_feature(key="boundary")  

# execution de la requete
territoire <- osmdata_sf(requete_contour)

# nettoyage
territoire_polygone <-   territoire$osm_multipolygons %>% 
  filter(boundary=="administrative" & `ref:INSEE`==CODE & admin_level==NIV_OSM) %>% 
  select(geometry) %>% 
  st_transform(crs=2154)

# base propre restreinte au territoire administratif
base_OSM <- st_intersection(voirie_lignes, territoire_polygone)

# Linéaires agrégés par cyclabilité, sens de circulation et qualité BANDE/PISTE
base_cyclabilite <- base_OSM %>% #resultat48 %>% 
  filter(  
    # on ne retient que les voies potentiellement cyclables, par exemple ni autoroutes,
    # ni voies rapides, ni... via ferrata !
    # la première condition si elle était écrite en négatif et sur Occitanie serait :
    # !highway %in% c("access","access_ramp","alley","bus_stop","corridor","crossing",
    # "disused","dogway","elevator","emergency_access","emergency_access_point",
    # "escape","none","rest_area","ser","stairs","tr","trunk","trunk_link",
    # "yes","motorway","motorway_link","service","steps","raceway",
    # "via_ferrata","construction","proposed","platform","virtual",
    # "path","track","footway") 
    highway %in% c("bridleway","bus_guideway","cycleway","living_street", "pedestrian",
                   "primary","primary_link","residential","road","secondary",
                   "secondary_link", "tertiary","tertiary_link","unclassified") &
      !is.na(highway) |
      # on conserve les trottoirs qui sont cyclables
      (highway %in% c("footway") & (bicycle %in% c("designated","yes") )) | 
      # et aussi les chemins et sentiers qui sont penses pour etre cyclables
      (highway %in% c("path","track") & bicycle %in% c("designated","official"))) %>% 
  mutate(
    # trois categories de cyclabilite : A = amenagement, B = apaise, C = autre
    cyclable = as.factor(case_when(highway %in% c("cycleway") ~ "A",
                                   highway %in% c("pedestrian","footway") & bicycle %in% c("designated","yes") ~ "A",
                                   highway=="unclassified" & bicycle %in% c("designated")~"A",
                                   cycleway %in% c("designated",
                                                   "lane","lane;opposite_lane","left:lane","opposite_lane","opposite",
                                                   "opposite_share_busway","opposite_track",
                                                   "shoulder","segregated",
                                                   "share_busway","shared",
                                                   "shared_lane","sidewalk","track","use_sidepath","yes") ~ "A",
                                   highway %in% c("path","track") & bicycle %in% c("designated","official") ~ "A",
                                   highway %in% c("bridleway","living_street")~ "B",
                                   maxspeed %in% c("30","20") ~ "B",
                                   TRUE ~ "C")),
    # simple ou double sens de circulation des velos ?
    sens_velo = case_when(cyclable == "A" & 
                            highway %in% c("cycleway", "pedestrian", "footway","path","track","unclassified") &
                            oneway %in% c("yes","reversible") ~ 1,
                          cyclable == "A" & 
                            highway %in% c("cycleway", "pedestrian", "footway","path","track","unclassified") &
                            !oneway %in% c("yes","reversible") ~ 2,
                          cyclable == "A" & cycleway %in% c("left:lane","opposite_lane","opposite","opposite_share_busway","opposite_track") ~ 1,
                          cyclable == "A" & !cycleway %in% c("left:lane","opposite_lane","opposite","opposite_share_busway","opposite_track") ~ 2,
                          oneway %in% c("yes","reversible") ~ 1,
                          TRUE ~ 2),
    # simple ou double sens de circulation des voitures ?
    sens_voiture = case_when(oneway %in% c("yes","reversible") ~ 1,
                             TRUE ~ 2),
    # amenagement de type "piste" ou "bande" ?
    qualite = as.factor(case_when(cyclable=="A" & highway %in% c("cycleway","pedestrian","footway","unclassified","path","track") ~ "PISTE",
                                  cyclable=="A" & !highway %in% c("cycleway","pedestrian","footway","unclassified","path","track") ~ "BANDE",
                                  TRUE ~ "AUTRE"))) %>%
  # on réunit alors les géométries pour simplifier les opérations suivantes
  group_by(cyclable,sens_velo,sens_voiture,qualite) %>%
  summarise(.groups = "drop")

# Enregistrement
aws.s3::s3write_using(
  base_cyclabilite,
  FUN = sfarrow::st_write_parquet,
  object = "pistes_cyclables/lineaire84.parquet",
  bucket = FILE_KEY_OUT_S3,
  opts = list("region" = "")
)

