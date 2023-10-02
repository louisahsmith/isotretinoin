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

authorizeWebApi(
  atlas_url,
  authMethod = "db",
  webApiUsername = usr,
  webApiPassword = pw
)

options(con.default.value = con)
options(schema.default.value = cdm_schema)
options(write_schema.default.value = my_schema)
options(atlas_url.default.value = atlas_url)

icu_rev <- tbl(con, inDatabaseSchema(cdm_schema, "cost")) |> 
  filter(revenue_code_concept_id %in% c(38003105, 38003107, 38003106, 38003112, 38003113))

icu_rev_obs <- filter(icu_rev, cost_domain_id == "Observation") |> 
  left_join(filter(tbl(con, inDatabaseSchema(cdm_schema, "observation")),
                   between(observation_date, as.Date("2020/01/01"), as.Date("2020/12/31"))),
            by = join_by(cost_event_id == observation_id)) |> 
  left_join(tbl(con, inDatabaseSchema(cdm_schema, "concept")),
            by = join_by(observation_concept_id == concept_id)) |> 
  mutate(date = observation_date) |> 
  distinct(person_id, date)

icu_rev_dev <- filter(icu_rev, cost_domain_id == "Device") |> 
  left_join(filter(tbl(con, inDatabaseSchema(cdm_schema, "device_exposure")),
                   between(device_exposure_start_date, as.Date("2020/01/01"), as.Date("2020/12/31"))),
            by = join_by(cost_event_id == device_exposure_id)) |> 
  left_join(tbl(con, inDatabaseSchema(cdm_schema, "concept")),
            by = join_by(device_concept_id == concept_id)) |> 
  mutate(date = device_exposure_start_date) |> 
  distinct(person_id, date)

icu_rev_prc <- filter(icu_rev, cost_domain_id == "Procedure") |> 
  left_join(filter(tbl(con, inDatabaseSchema(cdm_schema, "procedure_occurrence")),
                   between(procedure_date, as.Date("2020/01/01"), as.Date("2020/12/31"))),
            by = join_by(cost_event_id == procedure_occurrence_id)) |> 
  left_join(tbl(con, inDatabaseSchema(cdm_schema, "concept")),
            by = join_by(procedure_concept_id == concept_id)) |> 
  mutate(date = procedure_date) |> 
  distinct(person_id, date)

icu_rev_drg <- filter(icu_rev, cost_domain_id == "Drug") |> 
  left_join(filter(tbl(con, inDatabaseSchema(cdm_schema, "drug_exposure")),
                   between(drug_exposure_start_date, as.Date("2020/01/01"), as.Date("2020/12/31"))),
            by = join_by(cost_event_id == drug_exposure_id)) |> 
  left_join(tbl(con, inDatabaseSchema(cdm_schema, "concept")),
            by = join_by(drug_concept_id == concept_id)) |> 
  mutate(date = drug_exposure_start_date) |> 
  distinct(person_id, date)

new_cohort <- list(icu_rev_obs, icu_rev_dev, icu_rev_prc, icu_rev_drg) |> 
  reduce(union_all) |> 
  group_by(person_id) |> 
  slice_min(order_by = date, n = 1, with_ties = FALSE) |> 
  ungroup() |> 
  mutate(cohort_start_date = date, 
         vent_end_date = dateAdd("day", 30, cohort_start_date))

new_cohort_ventilation <- pull_concept_set(new_cohort,
                                           concept_set_id = 5118, start_date = cohort_start_date,
                                           end_date = vent_end_date, concept_set_name = "ventilation",
                                           keep_all = TRUE)

icu_cohort <- tbl(con, inDatabaseSchema(my_schema, "icu")) |> 
                    mutate(vent_end_date = dateAdd("day", 30, cohort_start_date),
                           person_id = subject_id)

icu_cohort_ventilation <- pull_concept_set(icu_cohort,
                 concept_set_id = 5019, start_date = cohort_start_date,
                 end_date = vent_end_date, concept_set_name = "ventilation",
                 keep_all = TRUE)

tally(icu_cohort)
tally(new_cohort)
length(unique(icu_cohort_ventilation$person_id))
length(unique(new_cohort_ventilation$person_id))

       