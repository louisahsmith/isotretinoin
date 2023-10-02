# Packages
library(tidyverse)
library(DatabaseConnector)
library(ROhdsiWebApi)
library(ohdsilab)

# ==== setup ===================================================================

# Credentials
usr <- keyring::key_get("lab_user")
pw <- keyring::key_get("lab_password")

# DB Connections
atlas_url <- "https://atlas.roux-ohdsi-prod.aws.northeastern.edu/WebAPI"
cdm_schema <- "omop_cdm_53_pmtx_202203"
my_schema <- paste0("work_", usr)

# Create the connection
con <- connect(
  dbms = "redshift",
  server = "ohdsi-lab-redshift-cluster-prod.clsyktjhufn7.us-east-1.redshift.amazonaws.com/ohdsi_lab",
  port = 5439,
  user = usr,
  password = pw
)

options(con.default.value = con)
options(schema.default.value = cdm_schema)
options(write_schema.default.value = my_schema)
options(atlas_url.default.value = atlas_url)

authorizeWebApi(
  atlas_url,
  authMethod = "db",
  webApiUsername = usr,
  webApiPassword = pw
)

# ===== index events ===========================================================

my_cohort <- "isotretinoin"

person <- tbl(con, inDatabaseSchema(cdm_schema, "person")) |> 
  select(person_id, year_of_birth, location_id)

location <- tbl(con, inDatabaseSchema(cdm_schema, "location")) |> 
  select(location_id, state)

cohort <- tbl(con, inDatabaseSchema(my_schema, my_cohort)) |>
  select(person_id = subject_id, cohort_start_date, cohort_end_date) |> 
  left_join(person, by = join_by(person_id)) |> 
  left_join(location, by = join_by(location_id)) |> 
  collect()

cohort |> 
  mutate(drug_start = lubridate::floor_date(cohort_start_date, "month")) |> 
  count(state, drug_start) |> 
  ggplot(aes(drug_start, n)) +
  geom_line() +
  facet_wrap(vars(state))
