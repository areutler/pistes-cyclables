# Package utilRP
# https://espace-charges-etudes.gitlab-pages.insee.fr/utilrp/index.html
install.packages("utilRP", repos = "https://nexus.insee.fr/repository/r-public/")

# Package gescodgeo
# https://psar-at.gitlab-pages.insee.fr/gescodgeo/index.html
install.packages("gescodgeo", repos = "https://nexus.insee.fr/repository/r-public/")

library(utilRP)
library(tidyverse)
library(arrow)

################################################################################
## Plus grandes communes

# install.package("zonages")
library(at36)
bc <- basecom(2022, arm = FALSE)
gdes_villes <- bc %>% arrange(desc(POP_MUN_2019)) %>% head(n=15) %>%
  select(DEPCOM, LIBGEO, LIB_REG, POP_MUN_2019)
gdes_villes
# A tibble: 15 x 4
# DEPCOM LIBGEO        LIB_REG                    POP_MUN_2019
# <chr>  <chr>         <chr>                             <dbl>
# 1 75056  Paris         Île-de-France                   2165423
# 2 13055  Marseille     Provence-Alpes-Côte d'Azur       870731
# 3 69123  Lyon          Auvergne-Rhône-Alpes             522969
# 4 31555  Toulouse      Occitanie                        493465
# 5 06088  Nice          Provence-Alpes-Côte d'Azur       342669
# 6 44109  Nantes        Pays de la Loire                 318808
# 7 34172  Montpellier   Occitanie                        295542
# 8 67482  Strasbourg    Grand Est                        287228
# 9 33063  Bordeaux      Nouvelle-Aquitaine               260958
# 10 59350  Lille         Hauts-de-France                  234475
# 11 35238  Rennes        Bretagne                         220488
# 12 51454  Reims         Grand Est                        181194
# 13 83137  Toulon        Provence-Alpes-Côte d'Azur       178745
# 14 42218  Saint-Étienne Auvergne-Rhône-Alpes             173821
# 15 76351  Le Havre      Normandie                        168290

################################################################################
## Base des déplacements domicile-travail et le RP

# install.packages("at36")
library(at36)
library(gescodgeo)
data <- base_ddt(annee = 2022)

# Extrait pour les grandes villes
data_gdes_villes <- data %>%
  # Passe des arrondissements aux communes pour Paris Lyon Marseille
  arm_to_com(from = LIEU_RESID) %>%
  arm_to_com(from = LIEU_TRAV) %>%

  # Réduit la base à ceux qui vivent ou travaillent dans ces grandes villes
  filter(LIEU_RESID %in% gdes_villes$DEPCOM |
         LIEU_TRAV %in% gdes_villes$DEPCOM)

# Certaines personnes peuvent vivre dans une grande ville et travailler dans une autre
data_gdes_villes %>%
  filter(LIEU_RESID %in% gdes_villes$DEPCOM,
         LIEU_TRAV %in% gdes_villes$DEPCOM,
         LIEU_RESID != LIEU_TRAV,
         CHAMP_CO2)  %>%
  View()

# Pour simplifier on prend prioritairement la ville de résidence et en 2 celle de travail
data_gdes_villes <- data_gdes_villes %>%
  mutate(DEPCOM = ifelse(LIEU_RESID %in% gdes_villes$DEPCOM, LIEU_RESID, LIEU_TRAV), .before = 1)

# Agrégation
result <- data_gdes_villes %>%
  group_by(DEPCOM, MODTRANS) %>%
  agrege_ddt()

# Distance moyenne pour le vélo :
dist_velo <- result %>%
  filter(MODTRANS == "3", CHAMP_CO2) %>%
  select(-contains("CO2"), -CARBU_HEBDO)

dist_velo  %>%
  mutate(LIBCOM = factor(DEPCOM, gdes_villes$DEPCOM, gdes_villes$LIBGEO), .after = DEPCOM) %>%
  kable(digits=1)
# |DEPCOM |LIBCOM        |MODTRANS | DIST| DIST_HEBDO|   IPONDI|
# |:------|:-------------|:--------|----:|----------:|--------:|
# |06088  |Nice          |3        |  5.6|       35.2|   5142.7|
# |13055  |Marseille     |3        |  4.4|       26.0|  10087.6|
# |31555  |Toulouse      |3        |  5.8|       32.3|  33227.3|
# |33063  |Bordeaux      |3        |  4.5|       26.8|  32914.9|
# |34172  |Montpellier   |3        |  4.5|       27.2|  17540.0|
# |35238  |Rennes        |3        |  4.1|       24.1|  16261.5|
# |42218  |Saint-Étienne |3        |  4.8|       31.5|   1780.1|
# |44109  |Nantes        |3        |  4.8|       27.7|  27445.8|
# |51454  |Reims         |3        |  3.6|       24.8|   4035.3|
# |59350  |Lille         |3        |  5.0|       29.4|  13801.2|
# |67482  |Strasbourg    |3        |  4.3|       26.4|  30479.6|
# |69123  |Lyon          |3        |  4.6|       25.2|  40951.8|
# |75056  |Paris         |3        |  5.7|       29.0| 145161.9|
# |76351  |Le Havre      |3        |  3.4|       23.8|   2624.9|
# |83137  |Toulon        |3        |  5.0|       32.8|   4540.6|

# Part d'actifs en emploi qui utilisent le vélo
part_velo <- result %>%
  mutate(VELO = ifelse(MODTRANS=="3", IPONDI, 0)) %>%
  group_by(DEPCOM) %>%
  summarise(PART_VELO = sum(VELO) / sum(IPONDI) * 100)

part_velo %>%
  mutate(LIBCOM = factor(DEPCOM, gdes_villes$DEPCOM, gdes_villes$LIBGEO), .after = DEPCOM) %>%
  kable(digits = 1)
# |DEPCOM |LIBCOM        | PART_VELO|
# |:------|:-------------|---------:|
# |06088  |Nice          |       2.8|
# |13055  |Marseille     |       2.5|
# |31555  |Toulouse      |       8.3|
# |33063  |Bordeaux      |      13.2|
# |34172  |Montpellier   |       8.7|
# |35238  |Rennes        |       8.9|
# |42218  |Saint-Étienne |       1.8|
# |44109  |Nantes        |      10.8|
# |51454  |Reims         |       3.6|
# |59350  |Lille         |       6.1|
# |67482  |Strasbourg    |      14.8|
# |69123  |Lyon          |       9.4|
# |75056  |Paris         |       6.7|
# |76351  |Le Havre      |       3.0|
# |83137  |Toulon        |       4.4|

part_cadre_emploi <- data_gdes_villes %>%
  mutate(CADRE = ifelse(CS1=="3", IPONDI, 0)) %>%
  group_by(DEPCOM) %>%
  summarise(PART_CADRE = sum(CADRE) / sum(IPONDI) * 100)

part_cadre_velo <- data_gdes_villes %>%
  filter(MODTRANS=="3") %>%
  mutate(CADRE = ifelse(CS1=="3", IPONDI, 0)) %>%
  group_by(DEPCOM) %>%
  summarise(PART_CADRE_VELO = sum(CADRE) / sum(IPONDI) * 100)

part_cadre <- left_join(part_cadre_emploi, part_cadre_velo, by = "DEPCOM")

part_cadre %>%
  mutate(LIBCOM = factor(DEPCOM, gdes_villes$DEPCOM, gdes_villes$LIBGEO), .after = DEPCOM) %>%
  arrange(desc(PART_CADRE)) %>%
  kable(digits = 1)
# |DEPCOM |LIBCOM        | PART_CADRE| PART_CADRE_VELO|
# |:------|:-------------|----------:|---------------:|
# |75056  |Paris         |       43.2|            61.1|
# |69123  |Lyon          |       36.7|            54.4|
# |31555  |Toulouse      |       34.7|            55.0|
# |44109  |Nantes        |       33.2|            51.8|
# |59350  |Lille         |       32.0|            49.6|
# |35238  |Rennes        |       30.7|            50.2|
# |33063  |Bordeaux      |       29.6|            44.1|
# |34172  |Montpellier   |       28.6|            46.4|
# |67482  |Strasbourg    |       27.1|            40.8|
# |13055  |Marseille     |       24.1|            43.6|
# |06088  |Nice          |       21.3|            31.8|
# |51454  |Reims         |       21.2|            29.0|
# |42218  |Saint-Étienne |       19.7|            33.7|
# |83137  |Toulon        |       17.2|            26.2|
# |76351  |Le Havre      |       16.2|            29.8|

part_cadre_velo <- data_gdes_villes %>%
  filter(MODTRANS=="3") %>%
  mutate(CADRE = ifelse(CS1=="3", IPONDI, 0)) %>%
  group_by(DEPCOM) %>%
  summarise(PART_CADRE_VELO = sum(CADRE) / sum(IPONDI) * 100)

part_cadre_velo %>%
  mutate(LIBCOM = factor(DEPCOM, gdes_villes$DEPCOM, gdes_villes$LIBGEO), .after = DEPCOM) %>%
  arrange(desc(PART_CADRE_VELO)) %>%
  kable(digits = 1)


################################################################################
# Base pistes cyclables

dir <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/output/"
cyclable <- read_parquet(paste0(dir,"base_cyclabilite_v3.parquet")) %>%
  filter(code %in% gdes_villes$DEPCOM)

# Fusion
tableau <- cyclable %>%
  select(DEPCOM = code, l_bande, l_piste, tx_cyclabilite) %>%
  left_join(part_velo, by = "DEPCOM") %>%
  left_join(dist_velo %>% select(DEPCOM, DIST_HEBDO), by = "DEPCOM") %>%
  left_join(gdes_villes %>% select(-LIB_REG), by = "DEPCOM") %>%
  left_join(part_cadre, by = "DEPCOM")


tableau <- tableau %>% mutate(cyclable_hab = (l_bande + l_piste)/ POP_MUN_2019,
                   part_piste = 100*(l_piste/(l_piste+l_bande)),
                   .before = PART_VELO
                   )%>%
  relocate(LIBGEO, .before = 1) %>%
  select(-DEPCOM, -l_bande, -l_piste, -POP_MUN_2019) %>%
  arrange(desc(tx_cyclabilite))

tableau %>%  knitr::kable(digits = 1)
# |LIBGEO        | tx_cyclabilite| cyclable_hab| part_piste| PART_VELO| DIST_HEBDO| PART_CADRE| PART_CADRE_VELO|
# |:-------------|--------------:|------------:|----------:|---------:|----------:|----------:|---------------:|
# |Paris         |           58.2|          0.6|       58.2|       6.7|       29.0|       43.2|            61.1|
# |Lyon          |           52.5|          1.2|       32.4|       9.4|       25.2|       36.7|            54.4|
# |Nantes        |           40.0|          2.1|       21.5|      10.8|       27.7|       33.2|            51.8|
# |Strasbourg    |           37.8|          1.8|       64.2|      14.8|       26.4|       27.1|            40.8|
# |Lille         |           32.5|          1.3|       29.5|       6.1|       29.4|       32.0|            49.6|
# |Toulouse      |           30.6|          1.8|       41.2|       8.3|       32.3|       34.7|            55.0|
# |Bordeaux      |           29.2|          1.4|       29.8|      13.2|       26.8|       29.6|            44.1|
# |Rennes        |           29.0|          1.6|       33.9|       8.9|       24.1|       30.7|            50.2|
# |Montpellier   |           18.5|          1.0|       61.6|       8.7|       27.2|       28.6|            46.4|
# |Reims         |           16.3|          1.2|       31.1|       3.6|       24.8|       21.2|            29.0|
# |Le Havre      |           15.2|          1.4|       32.6|       3.0|       23.8|       16.2|            29.8|
# |Saint-Étienne |           10.6|          0.7|       39.4|       1.8|       31.5|       19.7|            33.7|
# |Nice          |            9.1|          0.4|       72.0|       2.8|       35.2|       21.3|            31.8|
# |Toulon        |            8.7|          0.4|       43.3|       4.4|       32.8|       17.2|            26.2|
# |Marseille     |            7.8|          0.3|       48.1|       2.5|       26.0|       24.1|            43.6|

# Enregistrement
tableau %>% write.csv2("Z:/cyclabilite_gdes_villes.csv",
                            quote = TRUE,
                            row.names = FALSE,
                            fileEncoding = "UTF-8")

################################################################################
# Comparaison avec geovelo

# Base geovelo
data_geovelo <- read_parquet(paste0(dir,"ame-geovelo-20250701.parquet"))

# Attribue les catégories bandes ou piste
data_geovelo <- data_geovelo %>%
  arm_to_com(code_com) %>%
  filter(code_com %in% gdes_villes$DEPCOM) %>%
  mutate(cyclable = case_when(
    ame == "PISTE CYCLABLE" ~ "piste_geovelo",
    ame == "DOUBLE SENS CYCLABLE PISTE" ~ "piste_geovelo",
    ame == "VOIE VERTE" ~ "voie_verte_geovelo",
    ame == "BANDE CYCLABLE" ~ "bande_geovelo",
    ame == "DOUBLE SENS CYCLABLE BANDE" ~ "bande_geovelo",
    ame == "COULOIR BUS+VELO" ~ "bande_geovelo",
    ame == "ACCOTEMENT REVETU HORS CVCB" ~ "bande_geovelo",
    TRUE ~ "autre_geovelo"
  )) %>%
  group_by(code_com, cyclable) %>%
  summarise(longueur = as.integer(sum(longueur,na.rm=TRUE)) / 1000, .groups = "drop")

# Une ligne par commune
com_geovelo <- data_geovelo %>%
  pivot_wider(names_from = cyclable, values_from = longueur, id_cols = code_com)

# Fusion
com_compar <- cyclable %>%
  select(code_com = code, tx_cyclabilite, l_bande, l_piste, l_apaise_routier = l_apaise, l_apaise_non_routier) %>%
  mutate(across(starts_with("l_"), ~ .x/1000)) %>%
  left_join(y = com_geovelo ,by = "code_com") %>%
  relocate(autre_geovelo, .after=-1)

com_compar <- com_compar  %>%
  mutate(code_com = factor(code_com, gdes_villes$DEPCOM, gdes_villes$LIBGEO))

# Enregistrement
com_compar %>% write.csv2("Z:/comparaisons_gdes_villes.csv",
                       quote = TRUE,
                       row.names = FALSE,
                       fileEncoding = "UTF-8")


# Pour mémoire (avec utilRP) :
if(FALSE) {
  # Base du recensement

  data %>% group_by(pick(c("CS", "STAT_CS", "TACT", "AGEQ", "MODV", "DIPL", "MODTRANS"))) %>%
    summarise(N=n()) %>% nrow()

  RP_dico_vars(2022, base = "ind", exploitation = "compl", viewer = TRUE)

  RP_dico_modas(2022, base = "ind", exploitation = "compl", variables ="TACT", viewer = TRUE)

  # Pour simpligier tu peux recréeer CS1 = substr(CS,1,1)  :
  # CS1             Catégorie socioprofessionnelle (niveau 1)
  # 1   1                          Agriculteurs exploitants
  # 2   2       Artisans, commerçants et chefs d'entreprise
  # 3   3 Cadres et professions intellectuelles supérieures
  # 4   4                        Professions Intermédiaires
  # 5   5                                          Employés
  # 6   6                                          Ouvriers

  c("CS", "STAT_CS", "TACT", "AGEQ", "MODV", "DIPL", "ARM_RESID", "COMMUNE_RESID", "ARM_TRAV", "MODTRANS")


  # Charge la base 2022 de l'exploitation complémentaire (CS, etc.)
  data <- RP_charge(
    millesimes = 2022,
    base = "individu",
    exploitation = "compl",
    variables = c("CS", "STAT_CS", "TACT", "AGEQ", "MODV", "DIPL", "ARM_RESID", "COMMUNE_RESID", "ARM_TRAV", "MODTRANS"),
    suffixe_millesime = FALSE,
    agregation = TRUE,
    parquet = FALSE,
    liste = FALSE,
    lignes_max = Inf
  )

  data %>% check_cog(from = COMMUNE_RESID, cog = 2024)


  ################################################################################
  # Base des pistes cyclables

  path_piste_cyclable <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/inter/"

  data_pc <- open_dataset(
    sources = path_piste_cyclable,
    format = "parquet",
    partitioning = schema(DEP = string())
  ) %>% collect()

  data_pc %>% check_cog(from = code, cog = 2025)

  ################################################################################
  # On passe la base des pistes cyclables en géographie 2024

  cog_transition(2025, 2024) %>% filter(NB_COM_INI>1)

  data_pc_geo2024 <-
    data_pc %>% change_cog(cog_from = 2025, cog_to = 2024, from = code , to = "code_2024", split_ratio = TRUE)

  # On répartit les pistes dans les communes scindées
  data_pc_geo2024 <- data_pc_geo2024 %>%
    select(-starts_with("tx")) %>%
    mutate(across(.cols = c("l_pcyclable", "l_bcyclable", "l_apaise",  "l_voirie","l_cyclable"  ),
                  .fns = ~ .x * SPLIT_RATIO))

  # On fusionne les communes
  nrow(data_pc_geo2024) # 34806
  data_pc_geo2024 <- data_pc_geo2024 %>% group_by(code_2024) %>%
    summarise(across(.cols = c("l_pcyclable", "l_bcyclable", "l_apaise",  "l_voirie","l_cyclable"  ),
                     .fns = ~ sum(.x,na.rm=TRUE)))
  nrow(data_pc_geo2024) # 34802

  data_pc_geo2024 %>% check_cog(from = code_2024, cog = 2024)

  ################################################################################
  # Fusion

  data_complete <- data %>%
    left_join(data_pc_geo2024, by = c("COMMUNE_RESID" = "code_2024"))

  data_complete %>% filter(is.na(l_voirie))

  data_complete %>% saveRDS("V:/PSAR-AT/AT36-stage-pistes-cyclables/data/rp_2022_pistes_cyclables.rds")

}
