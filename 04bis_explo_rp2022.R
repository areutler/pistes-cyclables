data <- readRDS("V:/PSAR-AT/AT36-stage-pistes-cyclables/data/rp_2022_pistes_cyclables.rds")

# Part des pistes cyclables moyenne dans la commune selon la CS ?

data %>% group_by(CS) %>%
  summarise(l_voirie = weighted.mean(l_voirie, w = IPONDIAMF, na.rm = TRUE))
