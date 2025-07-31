library(sf)
library(tidyverse)
library(gescodgeo)
library(knitr)

# Base "Aménagements cyclables France Métropolitaine"
path <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/base_geovelo/france-20250701.geojson"
sf_cyclable_fce <-  read_sf(path, stringsAsFactors = FALSE, quiet = TRUE)

# Destination
output_dir <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/output/"

# Lambert 93
sf_cyclable_fce <- sf_cyclable_fce %>% st_transform(crs=2154)

# Calcul des longueurs par type
sf_cyclable_fce$longueur <- st_length(sf_cyclable_fce)

# On n'a plus besoin de la géométrie
cyclable_fce <- sf_cyclable_fce %>% st_drop_geometry()

# Département différents ?
cyclable_fce %>%
  com_to_dep(from = code_com_d, to = "dep_d") %>%
  com_to_dep(from = code_com_g, to = "dep_g")  %>%
  mutate(DEP_DIFF = dep_d != dep_g) %>%
  group_by(DEP_DIFF) %>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT)) %>% head()
# DEP_DIFF  longueur      PCT
# 1 FALSE    64973560. 100.0
# 2 TRUE        24097.   0.0371

# Voie de gauche et de droit dans la même colonne
data <- bind_rows(
  cyclable_fce %>%
    select(ends_with("_d"), longueur) %>%
    rename_with(.cols=ends_with("_d"),.fn = ~ substr(.x,0,nchar(.x)-2)),
  cyclable_fce %>%
    select(ends_with("_g"), longueur) %>%
    rename_with(.cols=ends_with("_g"),.fn = ~ substr(.x,0,nchar(.x)-2))
)

data %>% group_by(ame) %>%
  summarise(longueur = sum(longueur), .groups="drop") %>%
  mutate(PCT = 100 * longueur/sum(longueur)) %>%
  arrange(desc(PCT)) %>% kable(format.args = list(scientific=FALSE))
# |ame                                           |         longueur|               PCT|
# |:---------------------------------------------|----------------:|-----------------:|
# |PISTE CYCLABLE                                | 36836651.490 [m]| 28.3369075021 [1]|
# |VOIE VERTE                                    | 26029428.896 [m]| 20.0233595922 [1]|
# |AUTRE                                         | 19330749.889 [m]| 14.8703437853 [1]|
# |AUCUN                                         | 17093866.221 [m]| 13.1496019958 [1]|
# |BANDE CYCLABLE                                | 14884997.839 [m]| 11.4504112039 [1]|
# |AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE |  5229175.526 [m]|  4.0225877544 [1]|
# |DOUBLE SENS CYCLABLE NON MATERIALISE          |  4979734.317 [m]|  3.8307029821 [1]|
# |CHAUSSEE A VOIE CENTRALE BANALISEE            |  1695276.335 [m]|  1.3041057412 [1]|
# |ACCOTEMENT REVETU HORS CVCB                   |  1306288.963 [m]|  1.0048738962 [1]|
# |DOUBLE SENS CYCLABLE BANDE                    |  1273875.819 [m]|  0.9799398100 [1]|
# |COULOIR BUS+VELO                              |   954820.644 [m]|  0.7345039023 [1]|
# |VELO RUE                                      |   272506.173 [m]|  0.2096276912 [1]|
# |DOUBLE SENS CYCLABLE PISTE                    |    74815.365 [m]|  0.0575523558 [1]|
# |GOULOTTE                                      |    32868.286 [m]|  0.0252842088 [1]|
# |NA                                            |      256.843 [m]|  0.0001975787 [1]|

# Pondération
data <- data %>%  mutate(
  sens_velo = case_when(
    sens == "BIDIRECTIONNEL" ~ 2,
    TRUE ~ 1
  ))

################################################################################
# Longueur par ame et par commune

data <- data %>%
  mutate(longueur = longueur * sens_velo) %>%
  group_by(code_com, ame) %>%
  summarise(longueur = sum(longueur, na.rm= TRUE), .groups = "drop")

# Visualisation
head(data)
# code_com ame                                           longueur
# <chr>    <chr>                                              [m]
# 1 01004    AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE    2743.
# 2 01004    AUTRE                                             572.
# 3 01004    BANDE CYCLABLE                                   2683.
# 4 01004    PISTE CYCLABLE                                  10232.
# 5 01007    AMENAGEMENT MIXTE PIETON VELO HORS VOIE VERTE     300.
# 6 01007    PISTE CYCLABLE                                   2616.

# Ajout code du département
data <- data %>%
  com_to_dep(from = code_com, to = "dep") %>%
  dep_to_reg(from = dep, to = "reg") %>%
  relocate(dep, reg, .after = code_com)


# Enregistrement
data %>% write_parquet(paste0(output_dir,"ame-geovelo-20250701.parquet"))

################################################################################
# Indicateurs par commune

# Attribue les catégories bandes ou piste
indicateurs <- data %>%
  mutate(cycable = case_when(
    ame == "PISTE CYCLABLE" ~ "PISTE",
    ame == "DOUBLE SENS CYCLABLE PISTE" ~ "PISTE",
    ame == "VOIE VERTE" ~ "PISTE",
    ame == "BANDE CYCLABLE" ~ "BANDE",
    ame == "DOUBLE SENS CYCLABLE BANDE" ~ "BANDE",
    ame == "COULOIR BUS+VELO" ~ "BANDE",
    ame == "ACCOTEMENT REVETU HORS CVCB" ~ "BANDE",
    TRUE ~ "APAISE"
  ))

# Longueur des voies
indicateurs <- indicateurs %>%
  mutate(
    l_bande = ifelse(cycable=="BANDE", longueur, 0),
    l_piste = ifelse(cycable=="PISTE", longueur, 0) ,
    l_apaise = ifelse(cycable=="APAISE", longueur, 0) ,
    l_voirie = NA,
  )

# Base par commune
indicateurs <- indicateurs %>%
  group_by(code = code_com) %>%
  summarise(l_bande = sum(l_bande),
            l_piste = sum(l_piste),
            l_apaise = sum(l_apaise),
            l_voirie = sum(l_voirie),
            .groups = "drop")

# Ajout code du département
indicateurs <- indicateurs %>%
  com_to_dep(from = code, to = "dep") %>%
  dep_to_reg(from = dep, to = "reg") %>%
  relocate(dep, reg, .after = code)

# Vérification des région
indicateurs %>% distinct(reg) %>% arrange(reg)
indicateurs %>% filter(reg == "99") %>% distinct(code)

# On enlève l'étranger
indicateurs <- indicateurs %>% filter(reg!="99")

# Enregistrer
indicateurs %>%
  write_parquet(paste0(output_dir,"/com_geovelo_20250701.parquet"))
