

# Fichier à enregistrer
BUCKET = "zg6dup"
FILE = paste0("pistes_cyclables/longueur_voirie_","68" ,".parquet")


df2 <-
  aws.s3::s3read_using(
    FUN = arrow::read_parquet,
    # Mettre les options de FUN ici
    object = FILE,
    bucket = BUCKET,
    opts = list("region" = "")
  )

all.equal(df,df2)
df %>% distinct(osm_id) %>% nrow()
communes_sf %>% distinct(osm_id) %>% nrow()
