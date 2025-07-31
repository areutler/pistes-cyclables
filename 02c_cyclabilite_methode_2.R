##################### Base france entière ######################################
#
#
# INPUT= V:/PSAR-AT/AT36-stage-pistes-cyclables/data/input_20250718_met_com/
# Base des types de voiries par communes
#
# OUTPUT= V:/PSAR-AT/AT36-stage-pistes-cyclables/data/output/
#
################################################################################


# Librairies à compléter
library(tidyverse)
library(arrow)
library(knitr)
library(gescodgeo)

# Données pour toute la France
input_dir <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/input_20250718_met_com/"
data <- open_dataset(sources = input_dir, format = "parquet") %>% collect()

# Output
output_dir <- "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/output/"

# Codage des pistes et bandes cyclables, voies routières.
data <- data %>%

  # Restriction légale
  filter(is.na(access) | !access %in% c("no", "private")) %>%

  mutate(

    voie_routiere =  as.factor(case_when(

      # ROUTIER POTENTIELLEMENT CYCLABLE d'après le tag highway
      highway %in% c("trunk", "living_street", "primary", "secondary", "residential", "service", "road","busway") ~
        "ROUTIER POTENTIELLEMENT CYCLABLE",

      # ROUTIER POTENTIELLEMENT CYCLABLE avec un highway moins routier mais des infos complémentaire
      highway %in% c("tertiary", "unclassified") &
        (motor_vehicle == "yes"| tracktype == "grade1" | surface %in% c("asphalt", "paving_stones", "chipseal", "concrete") | smoothness %in% c("excellent", "good")) ~
        "ROUTIER POTENTIELLEMENT CYCLABLE",

      # NON POTENTIELLEMENT CYCLABLE
      highway == "motorway" ~ "ROUTIER NON POTENTIELLEMENT CYCLABLE",

      # LE CHEMIN NON ROUTIER a UN REVETEMENT POTENTIELLEMENT CYCLABLE
      (tracktype == "grade1" | surface %in% c("asphalt", "paving_stones", "chipseal", "concrete") | smoothness %in% c("excellent", "good"))  ~
        "AUTRE VOIE POTENTIELLEMENT CYCLABLE",

      #Non cyclable et non-potentiellement cyclable
      TRUE ~ "NON ROUTIER"

    )),


    cyclable_det = as.factor(case_when(

      # Mention explicite non cyclable
      bicycle %in% c("no") ~ "NON CYCLABLE",

      # Pistes cyclables : chaussée réservée aux cycles
      # Voie de circulation à part entière, elle peut être adjacente à une
      # chaussée ouverte à la circulation générale ou constituer un cheminement indépendant des autres voies.

      # Pistes cyclables sur une voie spécifique
      highway == "cycleway" ~ "PISTE", # Rue de type piste,
      network %in% c("icn", "ncn", "rcn", "lcn") ~ "PISTE", # Réseau vélo


      # Bande séparée considérée comme piste cyclables
      cycleway %in% c("track", "separate") ~ "PISTE",
      cycleway.left %in% c("track", "separate") ~ "PISTE",
      cycleway.right %in% c("track", "separate") ~ "PISTE",
      cycleway.both %in% c("track", "separate") ~ "PISTE",

      # Bandes cyclables
      # Sur une chaussée à une ou plusieurs voies,
      # une bande cyclable est une voie de circulation réservée aux cyclistes
      cycleway %in% c("lane", "shared_lane", "share_busway", "yes", "shared_lane") ~ "BANDE",
      cycleway.both %in% c("lane", "shared_lane", "share_busway", "yes", "shared_lane") ~ "BANDE",
      cycleway.right %in% c("lane", "shared_lane", "share_busway", "yes", "shared_lane")~ "BANDE",
      cycleway.left %in% c("lane", "shared_lane", "share_busway", "yes", "shared_lane") ~ "BANDE",

      # Codes obsolètes mais encore utilisés signifiant sans doute une bande
      cycleway %in% c("opposite_lane", "opposite_track", "opposite_share_busway") ~ "BANDE",
      cycleway.both %in% c("opposite_lane", "opposite_track", "opposite_share_busway")  ~ "BANDE",
      cycleway.right %in%c("opposite_lane", "opposite_track", "opposite_share_busway") ~ "BANDE",
      cycleway.left %in% c("opposite_lane", "opposite_track", "opposite_share_busway")  ~ "BANDE",

      # Idem mais non pris en compte car suspiscion d'absence de marquage
      # cycleway %in% c("opposite") ~ "BANDE",
      # cycleway.both %in% c("opposite") ~ "BANDE",
      # cycleway.right %in% c("opposite")~ "BANDE",
      # cycleway.left %in% c("opposite") ~ "BANDE",

      # Tag:bicycle=designated : considéré comme une bande (doute sur la qualité de l'aménagement)
      # On vérifie la qualité de la voie pour éviter les chemins de VTT
      voie_routiere != "NON ROUTIER" & bicycle == "designated" ~ "BANDE",

      # Cas zone apaisée
      highway == "living_street" ~ "APAISE",

      voie_routiere %in% c("ROUTIER POTENTIELLEMENT CYCLABLE") &
        (as.numeric(maxspeed) <= 30) ~ "APAISE",

      voie_routiere %in% c("ROUTIER POTENTIELLEMENT CYCLABLE") &
        bicycle %in% c("permissive", "yes") ~ "APAISE",

      voie_routiere %in% c("AUTRE VOIE POTENTIELLEMENT CYCLABLE") &
        bicycle %in% c("permissive", "yes") ~ "APAISE NON ROUTIER",


      TRUE~"NON CYCLABLE"
      ))
    )

# simple ou double sens de circulation des velos ?
data <- data %>%  mutate(
  sens_velo = case_when(

    # 0 si non cyclable
    !cyclable_det %in% c("BANDE", "PISTE") ~ 0,

    # Cas explicite de sens unique vélo pour les pistes / bandes
    oneway.bicycle == "no" ~ 2,
    oneway.bicycle %in% c("yes","-1") ~ 1,

    # Sur une voie non routière, le sens de circulation s'applique au vélo
    voie_routiere != "ROUTIER POTENTIELLEMENT CYCLABLE" & oneway %in% c("yes","-1") ~ 1,
    # La même chose mais avec les valeurs dépréciées
    voie_routiere != "ROUTIER POTENTIELLEMENT CYCLABLE" & oneway %in% c("true","1","reverse") ~ 1,

    # Valeurs dépréciées de cycleway pour les sens unique (quand même utilisée) sur les bandes
    # https://wiki.openstreetmap.org/wiki/FR:Key:cycleway
    cycleway %in% c("opposite", "opposite_lane", "opposite_track", "left:lane","opposite_share_busway") ~ 1,
    # Remarque les deux cas ci-dessous sont bien exclusifs l'un de l'autre.
    cycleway.left %in% c("opposite", "opposite_lane", "opposite_track", "left:lane","opposite_share_busway") ~ 1,
    cycleway.right %in% c("opposite", "opposite_lane", "opposite_track", "left:lane","opposite_share_busway") ~ 1,

    TRUE ~ 2
  ),

# simple ou double sens de circulation des voitures ?
  sens_voiture = case_when(
    voie_routiere == "NON ROUTIER" ~ 0,

    oneway %in% c("yes","reversible","-1") ~ 1,
    oneway %in%  c("true","1", "reverse") ~ 1, # Valeurs obsolètes

    TRUE ~ 2
  )
)

# Longueur des voies
data <- data %>%
  mutate(
    l_bande = ifelse(cyclable_det=="BANDE", longueur, 0) * sens_velo,
    l_piste = ifelse(cyclable_det=="PISTE", longueur, 0) * sens_velo,
    l_apaise = ifelse(cyclable_det=="APAISE", longueur, 0) * sens_voiture,
    l_apaise_non_routier = ifelse(cyclable_det=="APAISE NON ROUTIER", longueur, 0) * sens_voiture,
    l_voirie = ifelse(voie_routiere=="ROUTIER POTENTIELLEMENT CYCLABLE", longueur, 0) * sens_voiture,

    # Pour info on garde bandes et les pistes qui sont sur les même lignes OSM
    # que des voies routière
    l_bande_routiere = ifelse(voie_routiere=="ROUTIER POTENTIELLEMENT CYCLABLE", l_bande, 0),
    l_piste_routiere = ifelse(voie_routiere=="ROUTIER POTENTIELLEMENT CYCLABLE", l_piste, 0)
  )

# Base par commune
data_com <- data %>%
  group_by(code, commune) %>%
  summarise(l_bande = sum(l_bande),
            l_piste = sum(l_piste),
            l_apaise = sum(l_apaise),
            l_apaise_non_routier = sum(l_apaise_non_routier),
            l_voirie = sum(l_voirie),
            l_bande_routiere = sum(l_bande_routiere),
            l_piste_routiere = sum(l_piste_routiere),
            .groups = "drop") %>%

  mutate(
    #Taux de cyclabilité
    tx_cyclabilite = ((l_piste+ l_bande) /(l_voirie))*100,

    #Taux d'apaisé
    tx_apaise = ((l_piste + l_bande + l_apaise) / (l_voirie)) * 100,

    # Taux d'apaisé total
    tx_apaise_total = ((l_piste + l_bande + l_apaise + l_apaise_non_routier) / (l_voirie)) * 100,

    #Part des pistes cyclables dans les amenagements cyclables
    tx_piste_amenagement = ((l_piste) / (l_piste + l_bande)) * 100,
    tx_amenagement_apaise = ((l_piste + l_bande)/(l_piste + l_bande + l_apaise)) * 100
  )


# Ajout code du département
data_com <- data_com %>%
  com_to_dep(from = code, to = "dep") %>%
  dep_to_reg(from = dep, to = "reg") %>%
  relocate(dep, reg, .after = commune)

# Enregistrer
data_com %>%
  write_parquet(paste0(output_dir,"/base_cyclabilite_v3.parquet"))
