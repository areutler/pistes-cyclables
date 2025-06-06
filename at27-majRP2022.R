################################################################################
#
# 2 : Distances chefs-lieux en 2022
#
################################################################################
# encoding : UTF-8
#
# Utilisation de Metric-OSRM dans le cadre d'AT27, hors AUS
# 
#        
# Entrées : Z:/at27/maj-rp2022/flux_rp_2022.rds
#           Sur AUS : V:\PSAR-AT\AT27\bases_int\2022\ 
#
# Gestion des trajets transfrontaliers :
#          Z:/at27/maj-rp2022/tab_passage_LTEC_LAU.csv
#          Sur AUS : P:\POLE_TRANSFRONTALIER\Outils\Tables de passage\
#
# Sorties : 
#         Z:/at27/maj-rp2022/dist_chx_2022.rds
#         dans V:\PSAR-AT\AT27\bases_int\2022\ sur AUS
#      
################################################################################

# Librairies
library(metric.osrm)
library(dplyr)
library(readODS)
# library(gescodgeo)
## Chargement du pkg :
remotes::install_gitlab(repo = 'metric-osrm/metric-osrm-package',
                        host = 'git.lab.sspcloud.fr', upgrade = 'never',
                        build = TRUE, dependencies = TRUE,force = TRUE)
install.packages("gescodgeo", repos = "https://nexus.insee.fr/repository/r-public/")

# Flux RP
BUCKET = "zg6dup"
flux_rp <- aws.s3::s3read_using(
    FUN = readRDS,
    object = "at27/flux_rp_2022.rds",
    bucket = BUCKET,
    opts = list("region" = "")
  )

# Fichiers pour les frontaliers
tab_passage <- aws.s3::s3read_using(
  FUN = read_ods,
  object = "at27/LTEC_BE_CH_DE_LU_NUTS3_Mai2025.ods",
  bucket = BUCKET,
  opts = list("region" = "")
)

# Code officiel géographique : 
# check_cog(flux_rp, cog = 2023) # 60694 85165 85212 97501 97502 97701 97801
# check_cog(flux_rp, cog = 2024) # 97501 97502 97701 97801
# check_cog(flux_rp, cog = 2025) # 01330 02311 12076 14011 [...]

cog <- "2024"

# On précise l'adresse d'un serveur, ici celle du serveur expérimental
# déployé sur la plateforme du SSPCloud
options(osrm.server = "https://metric-osrm-backend.lab.sspcloud.fr/") 

# On précise le profil de routage, ici "driving" car le serveur SSPCloud
# ne propose que du calcul de trajet routier en voiture
options(osrm.profile = "driving")

################################################################################
# 2022 Monaco
################################################################################

couples <- flux_rp %>%
  filter(substr(LIEU_TRAV,1,2)=="MO") %>%
  filter(!(substr(LIEU_RESID,1,2) %in% c("97","2A","2B"))) %>%
  select(LIEU_RESID,LIEU_TRAV) %>%
  distinct()

coord_src <- codeComToCoord(codeInsee = couples$LIEU_RESID,
                            geo = cog,
                            type = "chx")

metric_src <- data.frame(ID = coord_src$code,
                         LON = coord_src$lon,
                         LAT = coord_src$lat,
                         stringsAsFactors = FALSE)

# Coordonnées de Monaco  : 7.41667, 43.73333
metric_dst <- data.frame(ID = replicate(nrow(metric_src),"MO001"),
                         LON = replicate(nrow(metric_src),7.41667),
                         LAT = replicate(nrow(metric_src),43.73333),
                         stringsAsFactors = FALSE)

distances <- metricOsrmTable(src = metric_src,
                             dst = metric_dst,
                             faceAFace = TRUE,
                             allerRetour = FALSE,
                             stable = TRUE,
                             exclude = "ferry")

dist_2022_MONACO <- distances %>% select(LIEU_RESID = idSrc,
                                         LIEU_TRAV = idDst,
                                         DUREE_CHEFS_LIEUX = duree,
                                         DIST_CHEFS_LIEUX = distance)

################################################################################
# 2022 France métropolitaine
################################################################################

couples <- flux_rp %>%
  filter(
    !(substr(LIEU_TRAV,1,2) %in% c("BE","LU","AL","SU","MO","ZZ","98","  "))
  ) %>%
  filter(substr(LIEU_RESID,1,2)!="97") %>%
  filter(substr(LIEU_TRAV,1,2)!="97") %>%
  filter(LIEU_TRAV != "") %>%
  select(LIEU_RESID,LIEU_TRAV) %>%
  distinct()

coord_src <- codeComToCoord(codeInsee = couples$LIEU_RESID,
                            geo = cog,
                            type = "chx")

metric_src <- data.frame(ID = coord_src$code,
                         LON = coord_src$lon,
                         LAT = coord_src$lat,
                         stringsAsFactors = FALSE)

coord_dst <- codeComToCoord(codeInsee = couples$LIEU_TRAV,
                            geo = cog,
                            type = "chx")

metric_dst <- data.frame(ID = coord_dst$code,
                         LON = coord_dst$lon,
                         LAT = coord_dst$lat,
                         stringsAsFactors = FALSE)

distances <- metricOsrmTable(src = metric_src,
                             dst = metric_dst,
                             faceAFace = TRUE,
                             allerRetour = FALSE,
                             stable = TRUE,
                             exclude = "ferry")

# [WARNING] 1624 couples n'ont pas été calculés.
# Ils sont repérables dans la table des résultats avec une durée et une distance à -999999.
# Pensez à les retirer avant toute analyse de la table.

dist_2022_MET <- distances %>% select(LIEU_RESID = idSrc,
                                      LIEU_TRAV = idDst,
                                      DUREE_CHEFS_LIEUX = duree,
                                      DIST_CHEFS_LIEUX = distance)


# Le calcul étant long on enregistre au cas où
aws.s3::s3write_using(
  dist_2022_MET,
  FUN = saveRDS,
  object = "at27/dist_2022_MET.rds",
  bucket = BUCKET,
  opts = list("region" = "")
)

################################################################################
# 2022 outre-mer
################################################################################

for (dep in as.character(971:974)){
  couples <- flux_rp %>%
    filter(substr(LIEU_TRAV,1,3)==dep) %>%
    filter(substr(LIEU_RESID,1,3)==dep) %>%
    distinct()
  
  coord_src <- codeComToCoord(codeInsee = couples$LIEU_RESID,
                              geo = cog,
                              type = "chx")
  
  metric_src <- data.frame(ID = coord_src$code,
                           LON = coord_src$lon,
                           LAT = coord_src$lat,
                           stringsAsFactors = FALSE)
  
  coord_dst <- codeComToCoord(codeInsee = couples$LIEU_TRAV,
                              geo = cog,
                              type = "chx")
  
  metric_dst <- data.frame(ID = coord_dst$code,
                           LON = coord_dst$lon,
                           LAT = coord_dst$lat,
                           stringsAsFactors = FALSE)
  
  assign(paste0("distances_",dep),metricOsrmTable(src = metric_src,
                                                  dst = metric_dst,
                                                  faceAFace = TRUE,
                                                  allerRetour = FALSE,
                                                  stable = TRUE,
                                                  exclude = "ferry"))
}

# Fusion et suppression des distances négatives
distances <- rbind(distances_971,distances_972,distances_973,distances_974) %>% 
  filter(distance>=0)

# Conservation des résultats
dist_2022_DOM <- distances %>% select(LIEU_TRAV = idDst,
                                      LIEU_RESID = idSrc,
                                      DUREE_CHEFS_LIEUX = duree, 
                                      DIST_CHEFS_LIEUX = distance)

################################################################################
# 2022 vers l'étranger 
################################################################################

# Table de passage des LTEC vers LAU
# tab_passage <- read.csv(paste0(etr,"tab_passage_LTEC_LAU.csv"),
#                         stringsAsFactors = FALSE)
# tab_passage <- read.csv(paste0(etr,"LTEC_BE_CH_DE_LU_NUTS3_novembre2023.csv"),
#                         stringsAsFactors = FALSE)


# tab_passage$LONGITUDE <- as.numeric(gsub(",",".",tab_passage$LONGITUDE))
# tab_passage$LATITUDE <- as.numeric(gsub(",",".",tab_passage$LATITUDE))

# Couples vers l'étranger (25 837 obs)
couples <- flux_rp %>% distinct(LIEU_RESID,LIEU_TRAV) %>%
  filter((substr(LIEU_RESID,1,3) %in% as.character(971:979))==FALSE) %>%
  filter((substr(LIEU_RESID,1,2) %in% c("2A","2B"))==FALSE) %>%
  filter(substr(LIEU_TRAV,1,2) %in% c("BE","LU","AL","SU"))

# Fusion avec la table de passage (25 837 obs, pas de correspondances multiples)
couples <- left_join(x = couples, 
                     y = tab_passage,
                     by = c("LIEU_TRAV"="LTEC"))


# Coordonnées du lieu de résidence (25 837 obs)
coord_habite_0 <- codeComToCoord(codeInsee = couples$LIEU_RESID,
                                 geo = cog,
                                 type = "chx")

# Coordonnées du lieu de travail (25 837 obs)
# coord_trav_0 <- codeLauToCoord(codePays = couples$PAYS,
#                                codeLau = couples$LAU,
#                                type = "chx")
coord_trav_0 <- couples %>%
  select(LIEU_TRAV,LONGITUDE,LATITUDE) %>%
  rename(code = LIEU_TRAV, lon = LONGITUDE, lat = LATITUDE)

# Réapparie pour faire du face à face
couples_2 <- couples %>% 
  
  # Fusion avec les coordonnées de résidence
  left_join(y = coord_habite_0 %>% distinct(),
            by=c("LIEU_RESID"="code")) %>%
  
  # Fusion avec les coordonnées de travail
  # mutate(PAYS_LAU=paste0(PAYS, "_",LAU)) %>%
  # left_join(y = coord_trav_0 %>% distinct(),by = c("PAYS_LAU"="code"),
  #           suffix = c("_R","_T")) 
  left_join(y = coord_trav_0 %>% distinct(),
            by = c("LIEU_TRAV"="code"),
            suffix = c("_R","_T"))


# Supression des valeurs manquantes (25 837 obs)
couples_3 <- couples_2 %>% filter(if_all(.cols=c("lon_R","lat_R","lon_T","lat_T"),
                                         .fns = ~ is.na(.x) == FALSE))

# Sources et destinations
metric_src <- data.frame(ID = couples_3$LIEU_RESID,
                         LON = couples_3$lon_R,
                         LAT = couples_3$lat_R,
                         stringsAsFactors = FALSE)

metric_dst <- data.frame(ID = couples_3$LIEU_TRAV,
                         LON = couples_3$lon_T,
                         LAT = couples_3$lat_T,
                         stringsAsFactors = FALSE)

distances <- metricOsrmTable(src = metric_src,
                             dst = metric_dst,
                             faceAFace = TRUE,
                             allerRetour = FALSE,
                             stable = TRUE,
                             exclude = "ferry")

# Couples avec plusieurs distances
distances %>% group_by(idSrc,idDst) %>% summarise(NB_DIST=n()) %>%
  group_by(NB_DIST) %>% summarise(NB_COUPLES=n())
# NB_DIST NB_COUPLES
# 1       1      25837

# On fait une moyenne simple (24 565 obs, pour 27 228 dans couples)
dist_2022_ETR <- distances

#################################################################################
# Fusion
#dist_2022_MET <- readRDS("Z:/at27/dist_2022_MET.rds")

# 959295 avant la nouvelle table de passage
# 973 697 maintenant
dist_chx <-  
  bind_rows(dist_2022_MET,
            dist_2022_DOM,
            dist_2022_ETR %>%
              select(idSrc, idDst, duree, distance) %>%
              rename(LIEU_RESID = idSrc, LIEU_TRAV = idDst,
                     DUREE_CHEFS_LIEUX = duree,
                     DIST_CHEFS_LIEUX = distance),
            dist_2022_MONACO)


# Couples non trouvés
flux_rp_dist <- left_join(flux_rp,dist_chx,by=c("LIEU_RESID","LIEU_TRAV"))
zone_x <- function(x) {
  x <- case_when(
    substr(x,1,3) %in% as.character(971:999) ~ substr(x,1,3),
    substr(x,1,2) %in%  c("2A","2B") ~ substr(x,1,2),
    TRUE ~ "000"
  )
  return(x)
}

# 813 couples non trouvés sans explication, bcp vers l'étranger
# Seulement 93 avec la nouvelle table de passage LAU-LTEC
flux_rp_dist_na <- flux_rp_dist %>% 
  filter(is.na(DIST_CHEFS_LIEUX),
         LIEU_TRAV!="ZZZZZ",
         zone_x(LIEU_RESID)==zone_x(LIEU_TRAV))


# Enregistrement
aws.s3::s3write_using(
  dist_chx,
  FUN = saveRDS,
  object = "at27/dist_chx_2022.rds",
  bucket = BUCKET,
  opts = list("region" = "")
)

# ################################################################################