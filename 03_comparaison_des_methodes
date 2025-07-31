# Librairies
library(tidyverse)
library(arrow)
library(knitr)
library(gescodgeo)

dir <-  "V:/PSAR-AT/AT36-stage-pistes-cyclables/data/output/"

# Fonction pour agreger
agrege_velo <- function(data) {
  data %>% summarise(
    l_piste = sum(l_piste, na.rm = TRUE),
    l_bande = sum(l_bande, na.rm = TRUE),
    l_apaise = sum(l_apaise, na.rm = TRUE),
    l_voirie = sum(l_voirie, na.rm = TRUE),
    .groups = "drop") %>%

    # Taux de cyclabilité
    mutate(tx_cyclable = (l_piste + l_bande) / l_voirie * 100)%>%

    # Longueur en km
    mutate(across(starts_with("l_"), ~ round(.x/1000,0))) %>%

    #Taux d'apaisé
    mutate(tx_apaise = (l_piste + l_bande + l_apaise) / (l_voirie) * 100) %>%

    # Total cyclable
    mutate(l_cyclable = (l_piste + l_bande + l_apaise))


}

# Tableau de synthèse par départements
synthese_dep <- function(data) {
  data %>%
    group_by(dep) %>% agrege_velo() %>%
    arrange(desc(tx_cyclable)) %>%
    head() %>%
    bind_rows(
      data %>%
        agrege_velo() %>%
        mutate(dep = "TOTAL")
    )
}

# Base produite avec la méthode de Rémi
data_vRemi <- read_parquet(paste0(dir,"base_cyclabilite_v0.parquet")) %>%
  # Harmonisation des variables
  mutate(l_bande = l_bcyclable,
         l_piste = l_pcyclable,
         l_voirie = l_bande + l_apaise + l_voirie)

# Base produite avec la méthode d'Arthur I
data_vArthur1 <- read_parquet(paste0(dir,"base_cyclabilite_v1.parquet"))

# Base produite avec la méthode d'Arthur II
data_vArthur2 <- read_parquet(paste0(dir,"base_cyclabilite_v2.parquet"))

# Ajout de : opposite_share_busway et filtre sur bicycle=no/use_sidepath
data_vArthur3 <- read_parquet(paste0(dir,"base_cyclabilite_v3.parquet"))

# Base geovelo
data_geovelo <- read_parquet(paste0(dir,"com_geovelo_20250701.parquet"))

### RESULTATS

data_vRemi %>% synthese_dep() %>% kable(format.args=list(big.mark=" "))
# |dep   | l_piste| l_bande| l_apaise|  l_voirie| tx_cyclable|  tx_apaise| l_cyclable|
# |:-----|-------:|-------:|--------:|---------:|-----------:|----------:|----------:|
# |75    |     626|     160|    1 504|     1 964|   40.015719| 116.598778|      2 290|
# |92    |     526|     212|    1 522|     3 239|   22.758035|  69.774622|      2 260|
# |94    |     704|     120|    1 193|     4 066|   20.255177|  49.606493|      2 017|
# |93    |     660|     117|      732|     4 310|   18.012191|  35.011601|      1 509|
# |68    |   1 943|     235|    1 113|    12 703|   17.146198|  25.907266|      3 291|
# |67    |   2 664|     252|    1 514|    18 152|   16.064900|  24.405024|      4 430|
# |TOTAL |  86 458|  11 667|  106 521| 2 192 124|    4.476285|   9.335512|    204 646|

data_vArthur1 %>% synthese_dep() %>% kable(format.args=list(big.mark=" "))
# |dep   | l_piste| l_bande| l_apaise|  l_voirie| tx_cyclable| tx_apaise| l_cyclable|
# |:-----|-------:|-------:|--------:|---------:|-----------:|---------:|----------:|
# |75    |     792|     540|    1 118|     2 607|   51.082419|  93.97775|      2 450|
# |92    |     343|     615|    1 352|     4 770|   20.089696|  48.42767|      2 310|
# |68    |     933|   1 585|      905|    15 345|   16.411537|  22.30694|      3 423|
# |67    |   1 781|   1 215|    1 232|    19 851|   15.095896|  21.29868|      4 228|
# |59    |   1 494|   3 892|    8 183|    36 651|   14.698036|  37.02218|     13 569|
# |93    |     478|     400|      543|     6 212|   14.132660|  22.87508|      1 421|
# |TOTAL |  40 634|  65 512|   82 549| 1 655 523|    6.411614|  11.39791|    188 695|


data_vArthur2 %>% synthese_dep() %>% kable()
# |dep   | l_piste| l_bande| l_apaise| l_voirie| tx_cyclable| tx_apaise| l_cyclable|
# |:-----|-------:|-------:|--------:|--------:|-----------:|---------:|----------:|
# |75    |     781|     555|     1292|     2290|   58.334367| 114.75983|       2628|
# |92    |     340|     653|     1510|     4049|   24.508851|  61.81773|       2503|
# |68    |     900|    1586|     1393|    14871|   16.720595|  26.08433|       3879|
# |93    |     474|     416|      738|     5398|   16.493510|  30.15932|       1628|
# |67    |    1775|    1239|     1771|    18942|   15.911110|  25.26132|       4785|
# |94    |     374|     428|     1232|     5164|   15.529715|  39.38807|       2034|
# |TOTAL |   40089|   65136|   121085|  1571971|    6.693823|  14.39658|     226310|

data_vArthur3 %>% synthese_dep() %>% kable()
# |dep   | l_piste| l_bande| l_apaise| l_voirie| tx_cyclable| tx_apaise| l_cyclable|
# |:-----|-------:|-------:|--------:|--------:|-----------:|---------:|----------:|
# |75    |     776|     557|     1248|     2290|   58.185289| 112.70742|       2581|
# |92    |     338|     600|     1508|     4049|   23.156227|  60.40998|       2446|
# |93    |     474|     350|      738|     5398|   15.263871|  28.93664|       1562|
# |67    |    1774|    1025|     1768|    18942|   14.775696|  24.11044|       4567|
# |94    |     374|     374|     1232|     5164|   14.493966|  38.34237|       1980|
# |68    |     900|    1212|     1391|    14871|   14.206070|  23.55591|       3503|
# |TOTAL |   40038|   47248|   120948|  1571971|    5.552637|  13.24668|     208234|

# Les mêmes départements avec geovelo
data_vArthur3 %>% synthese_dep() %>% select(dep) %>%
  inner_join(
  data_geovelo %>%
    group_by(dep) %>% agrege_velo(), by = "dep"
  )%>%
  bind_rows(
    data_geovelo %>%
      agrege_velo() %>%
      mutate(dep = "TOTAL")
  ) %>%
  kable()
# |dep   | l_piste| l_bande| l_apaise| l_voirie| tx_cyclable| tx_apaise|
# |:-----|-------:|-------:|--------:|--------:|-----------:|---------:|
# |75    |     478|     139|     1705|        0|         Inf|       Inf|
# |92    |     316|     217|     1374|        0|         Inf|       Inf|
# |68    |    1237|     295|     1269|        0|         Inf|       Inf|
# |93    |     472|     143|      725|        0|         Inf|       Inf|
# |67    |    2090|     294|     1376|        0|         Inf|       Inf|
# |94    |     433|     122|      767|        0|         Inf|       Inf|
# |TOTAL |   63048|   16249|    50896|        0|         Inf|       Inf|
