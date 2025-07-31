################################################################################

# Librairies à compléter
library(tidyverse)
library(arrow)
library(knitr)
library(gescodgeo)
library(zonages)

# Chargement
dir <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/output/"
data <- read_parquet(paste0(dir,"base_cyclabilite_v3.parquet"))

# Libellé des région
lib_regions <- zonages::basecom(2022) %>%
  distinct(REG, LIB_REG)

################################################################################
# 1) taux de cyclabilité par région

# Agrégation par région
tableau <- data %>%
  group_by(reg) %>%
  summarise(
    l_bande = sum(l_bande) / 1000,
    l_piste = sum(l_piste) / 1000,
    l_voirie = sum(l_voirie) / 1000,
    .groups = "drop") %>%

  # Taux de cycabilité
  mutate(tx_cyclabilite = ((l_piste+ l_bande) /(l_voirie))*100) %>%

  # Ajout des libellés de régions
  left_join(lib_regions, by=join_by(reg == REG)) %>%
  relocate(LIB_REG, .after = reg)


# Affichage
tableau %>%
  kable(format.args = list(scientific=FALSE, big.mark = " "),
        digits = c(NA,NA,0,0,0,1))
# |reg |LIB_REG                    | l_bande| l_piste| l_voirie| tx_cyclabilite|
# |:---|:--------------------------|-------:|-------:|--------:|--------------:|
# |11  |Île-de-France              |   4 252|   4 360|   74 040|           11.6|
# |24  |Centre-Val de Loire        |   2 009|   2 210|   86 947|            4.9|
# |27  |Bourgogne-Franche-Comté    |   2 902|   2 027|  105 118|            4.7|
# |28  |Normandie                  |   2 622|   1 212|   99 275|            3.9|
# |32  |Hauts-de-France            |   5 231|   2 762|  104 060|            7.7|
# |44  |Grand Est                  |   5 167|   5 256|  140 399|            7.4|
# |52  |Pays de la Loire           |   4 201|   2 911|  105 271|            6.8|
# |53  |Bretagne                   |   3 223|   2 451|  104 440|            5.4|
# |75  |Nouvelle-Aquitaine         |   3 621|   6 871|  225 088|            4.7|
# |76  |Occitanie                  |   3 685|   4 218|  209 181|            3.8|
# |84  |Auvergne-Rhône-Alpes       |   7 432|   3 913|  200 417|            5.7|
# |93  |Provence-Alpes-Côte d'Azur |   2 861|   1 793|  104 234|            4.5|
# |94  |Corse                      |      42|      54|   13 502|            0.7|

# Enregistrement
tableau %>% write.csv2("Z:/pistes-cycables-par-regions.csv",
                       quote = TRUE,
                       sep = ";",
                       row.names = FALSE,
                       fileEncoding = "UTF-8")

################################################################################
# 2) comparaison des longueurs avec la BNAC

# Base geovelo
data_geovelo <- read_parquet(paste0(dir,"ame-geovelo-20250701.parquet"))

# Attribue les catégories bandes ou piste
data_geovelo <- data_geovelo %>%
  mutate(cycable = case_when(
    ame == "PISTE CYCLABLE" ~ "piste_geovelo",
    ame == "DOUBLE SENS CYCLABLE PISTE" ~ "piste_geovelo",
    ame == "VOIE VERTE" ~ "voie_verte_geovelo",
    ame == "BANDE CYCLABLE" ~ "bande_geovelo",
    ame == "DOUBLE SENS CYCLABLE BANDE" ~ "bande_geovelo",
    ame == "COULOIR BUS+VELO" ~ "bande_geovelo",
    ame == "ACCOTEMENT REVETU HORS CVCB" ~ "bande_geovelo",
    TRUE ~ "autre_geovelo"
  ))

# Agrège par catégorie et par région
reg_geovelo <- data_geovelo %>%
  group_by(reg, cycable) %>%
  summarise(longueur = as.integer(sum(longueur,na.rm=TRUE)) / 1000, .groups = "drop") %>%
  filter(reg != "99")

# Une ligne par région
reg_geovelo <- reg_geovelo %>%
  pivot_wider(names_from = cycable, values_from = longueur, id_cols = reg)

# Longueur dans notre base
reg_cyclable <-  data %>%
  group_by(reg) %>%
  summarise(
    bande = sum(l_bande) / 1000,
    piste = sum(l_piste) / 1000,
    apaise_routier = sum(l_apaise) / 1000,
    apaise_non_routier = sum(l_apaise_non_routier) / 1000,
    .groups = "drop")

# Fusion
reg_cyclable <- left_join(
  x = reg_cyclable,
  y = reg_geovelo,
  by = "reg"
)

# Ordre des colonnes
reg_cyclable <- reg_cyclable %>%
  relocate(autre_geovelo, .after=-1)

# Libellés des régions :
reg_cyclable <- reg_cyclable %>%
  mutate(reg = factor(reg, lib_regions$REG, lib_regions$LIB_REG))

# Affichage
reg_cyclable %>%
  kable(format.args = list(scientific=FALSE, big.mark = " "),
        digits = 0)


# Enregistrement
reg_cyclable %>% write.csv2("Z:/longueurs-par-region-vs-geovelo.csv",
                       quote = TRUE,
                       row.names = FALSE,
                       fileEncoding = "UTF-8")
