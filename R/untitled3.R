# Packages
library(tidyverse)
library(DatabaseConnector)

# ==== setup ===================================================================

# Credentials
usr <- keyring::key_get("lab_user")
pw <- keyring::key_get("lab_password")

# DB Connections
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

target_ref <- tibble(
  target_cohort_definition_id = 2511,
  target_name = "Reproductive age"
)
copy_to(con, target_ref, temporary = TRUE)

tar_ref <- tibble(
  tar_id = 1,
  tar_start_with = "start",
  tar_start_offset = 0,
  tar_end_with = "end",
  tar_end_offset = 0
)
copy_to(con, tar_ref, temporary = TRUE)

outcome_ref <- tibble(
  outcome_id = 1,
  outcome_cohort_definition_id = 2510,
  outcome_name = "Isotretinoin",
  clean_window = 9999,
  excluded_cohort_definition_id = 0
)
copy_to(con, outcome_ref, temporary = TRUE)

subgroup_ref <- tibble(
  subgroup_id = 0,
  subgroup_name = "All"
)
copy_to(con, subgroup_ref, temporary = TRUE)

age_group <- tibble(
  age_id = integer(),
  group_name = character(),
  min_age = integer(),
  max_age = integer()
)
copy_to(con, age_group, temporary = TRUE)

# Create the #TTAR temporary table
ttar_data <- tbl(con, "#tar_ref") %>%
  filter(tar_id == 1) %>%
  select(tar_id, tar_start_with, tar_start_offset, tar_end_with, tar_end_offset)

ttar <- ttar_data %>%
  inner_join(tbl(con, "work_usr9.repro_age"), by = c("tar_id" = "cohort_definition_id")) %>%
  inner_join(tbl(con, "omop_cdm_53_pmtx_202203.observation_period"), by = c("subject_id" = "person_id")) %>%
  mutate(start_date = case_when(
    tar_start_with == "start" ~ case_when(
      as.Date(cohort_start_date) + tar_start_offset < observation_period_end_date ~
        as.Date(cohort_start_date) + tar_start_offset,
      as.Date(cohort_start_date) + tar_start_offset >= observation_period_end_date ~ observation_period_end_date
    ),
    tar_start_with == "end" ~ case_when(
      as.Date(cohort_end_date) + tar_start_offset < observation_period_end_date ~
        as.Date(cohort_end_date) + tar_start_offset,
      as.Date(cohort_end_date) + tar_start_offset >= observation_period_end_date ~ observation_period_end_date
    ),
    TRUE ~ NA_Date_
  )) %>%
  mutate(end_date = case_when(
    tar_end_with == "start" ~ case_when(
      as.Date(cohort_start_date) + tar_end_offset < observation_period_end_date ~
        as.Date(cohort_start_date) + tar_end_offset,
      as.Date(cohort_start_date) + tar_end_offset >= observation_period_end_date ~ observation_period_end_date
    ),
    tar_end_with == "end" ~ case_when(
      as.Date(cohort_end_date) + tar_end_offset < observation_period_end_date ~
        as.Date(cohort_end_date) + tar_end_offset,
      as.Date(cohort_end_date) + tar_end_offset >= observation_period_end_date ~ observation_period_end_date
    ),
    TRUE ~ NA_Date_
  )) %>%
  filter(!is.na(start_date) & !is.na(end_date) & start_date <= end_date) %>%
  select(cohort_definition_id, tar_id, subject_id, start_date, end_date)

# Create the #TTAR_to_erafy temporary table
ttar_to_erafy_data <- ttar_data %>%
  select(cohort_definition_id, tar_id, subject_id, start_date, end_date)

ttar_to_erafy <- ttar_to_erafy_data %>%
  inner_join(ttar_to_erafy_data, by = c("cohort_definition_id", "tar_id", "subject_id")) %>%
  filter(start_date <= end_date & end_date >= start_date & start_date != start_date)

# Create the #TTAR_era_overlaps temporary table
ttar_era_overlaps_data <- ttar_to_erafy_data %>%
  union_all(
    select(cohort_definition_id, tar_id, subject_id, end_date = start_date, event_type = -1) %>%
      distinct(),
    select(cohort_definition_id, tar_id, subject_id, end_date, event_type = 1) %>%
      distinct()
  ) %>%
  group_by(cohort_definition_id, tar_id, subject_id) %>%
  mutate(interval_status = cumsum(event_type)) %>%
  ungroup() %>%
  filter(interval_status == 0)

cteEndDates_data <- ttar_to_erafy %>%
  select(cohort_definition_id, tar_id, subject_id, start_date) %>%
  inner_join(ttar_era_overlaps_data, by = c("subject_id", "cohort_definition_id", "tar_id")) %>%
  filter(end_date >= start_date) %>%
  group_by(cohort_definition_id, tar_id, subject_id, start_date) %>%
  summarise(end_date = min(end_date))

ttar_era_overlaps <- cteEndDates_data %>%
  group_by(cohort_definition_id, tar_id, subject_id, end_date) %>%
  summarise(start_date = min(start_date))

db_write_table(con, "#TTAR_era_overlaps", ttar_era_overlaps_final_data, temporary = TRUE)

# Create the #TTAR_erafied temporary table
ttar_era_overlaps_data <- ttar_era_overlaps %>%
  select(cohort_definition_id, tar_id, subject_id, start_date, end_date)

ttar_data <- ttar %>%
  left_join(ttar, by = c(
    "cohort_definition_id" = "cohort_definition_id",
    "tar_id" = "tar_id",
    "subject_id" = "subject_id",
    "start_date" = "start_date",
    "end_date" = "end_date"
  )) %>%
  filter(is.na(subject_id))

ttar_erafied <- bind_rows(ttar_era_overlaps_data, ttar_data)

# Create the #subgroup_person table
subgroup_person <- tibble(
  subgroup_id = as.integer(),
  subject_id = as.integer(),
  start_date = as.Date()
)

copy_to(con, subgroup_person, temporary = TRUE)

# Create the #excluded_tar_cohort temporary table
outcome_data <- tbl(con, "#outcome_ref") %>%
  filter(outcome_id == 1) %>%
  select(outcome_id, outcome_cohort_definition_id, clean_window)

excluded_tar_cohort_data <- tbl(con, "work_usr9.repro_age") %>%
  inner_join(outcome_data, by = c("cohort_definition_id" = "outcome_cohort_definition_id")) %>%
  filter(as.Date(cohort_end_date) + clean_window >= as.Date(cohort_start_date)) %>%
  select(outcome_id, subject_id,
         cohort_start_date = as.Date(cohort_start_date) + 1,
         cohort_end_date = as.Date(cohort_end_date) + clean_window
  )

excluded_tar_cohort <- bind_rows(
  excluded_tar_cohort_data,
  tbl(con, "work_usr9.repro_age") %>%
    inner_join(outcome_data, by = c("cohort_definition_id" = "excluded_cohort_definition_id")) %>%
    select(outcome_id, subject_id, cohort_start_date, cohort_end_date)
)

# Create the #exc_TTAR_o temporary table
exc_ttar_o <- ttar_erafied %>%
  inner_join(excluded_tar_cohort, by = c("subject_id")) %>%
  filter(cohort_start_date <= end_date, cohort_end_date >= start_date) %>%
  mutate(
    start_date = if_else(cohort_start_date > start_date, cohort_start_date, start_date),
    end_date = if_else(cohort_end_date < end_date, cohort_end_date, end_date)
  ) %>%
  select(
    target_cohort_definition_id = cohort_definition_id,
    tar_id,
    outcome_id,
    subject_id,
    start_date,
    end_date
  )

# Create the #exc_TTAR_o_to_erafy temporary table
exc_ttar_o_to_erafy <- exc_ttar_o %>%
  inner_join(exc_ttar_o, by = c(
    "target_cohort_definition_id" = "target_cohort_definition_id",
    "tar_id" = "tar_id",
    "outcome_id" = "outcome_id",
    "subject_id" = "subject_id"
  )) %>%
  filter(start_date < end_date & start_date != start_date | end_date != end_date)

# Create the #ex_TTAR_o_overlaps temporary table
exc_ttar_o_to_erafy_data <- exc_ttar_o_to_erafy %>%
  select(
    target_cohort_definition_id,
    tar_id,
    outcome_id,
    subject_id,
    start_date = as.Date(start_date),
    end_date = as.Date(end_date)
  )

ex_ttar_o_overlaps_data <- exc_ttar_o_to_erafy_data %>%
  union_all(
    select(
      target_cohort_definition_id,
      tar_id,
      outcome_id,
      subject_id,
      end_date = as.Date(start_date),
      event_type = -1
    ) %>%
      distinct(),
    select(
      target_cohort_definition_id,
      tar_id,
      outcome_id,
      subject_id,
      end_date,
      event_type = 1
    ) %>%
      distinct()
  ) %>%
  group_by(
    target_cohort_definition_id,
    tar_id,
    outcome_id,
    subject_id
  ) %>%
  mutate(interval_status = cumsum(event_type)) %>%
  ungroup() %>%
  filter(interval_status == 0)

cteEnds_data <- exc_ttar_o_to_erafy_data %>%
  group_by(
    target_cohort_definition_id,
    tar_id,
    outcome_id,
    subject_id,
    start_date
  ) %>%
  summarise(end_date = min(end_date))

ex_ttar_o_overlaps <- cteEnds_data %>%
  group_by(
    target_cohort_definition_id,
    tar_id,
    outcome_id,
    subject_id,
    end_date
  ) %>%
  summarise(start_date = min(start_date))

# Create the #exc_TTAR_o_erafied temporary table
ex_ttar_o_overlaps_data <- ex_ttar_o_overlaps %>%
  select(
    target_cohort_definition_id,
    tar_id,
    outcome_id,
    subject_id,
    start_date,
    end_date
  )

ex_ttar_o_erafied_data <- exc_ttar_o %>%
  left_join(exc_ttar_o, by = c(
    "target_cohort_definition_id" = "target_cohort_definition_id",
    "tar_id" = "tar_id",
    "outcome_id" = "outcome_id",
    "subject_id" = "subject_id"
  )) %>%
  filter(
    is.null(subject_id) |
      (start_date < end_date & start_date != start_date) |
      end_date != end_date
  ) %>%
  distinct(
    target_cohort_definition_id,
    tar_id,
    outcome_id,
    subject_id,
    start_date,
    end_date
  )

exc_ttar_o_erafied <- bind_rows(ex_ttar_o_overlaps_data, ex_ttar_o_erafied_data)

# Create the #outcome_smry temporary table
outcome_ref_data <- outcome_ref %>%
  filter(outcome_id == 1) %>%
  select(outcome_id, subject_id, cohort_start_date)

outcome_smry <- ttar_erafied %>%
  inner_join(outcome_ref_data, by = c("subject_id")) %>%
  filter(start_date <= cohort_start_date, end_date >= cohort_start_date) %>%
  left_join(exc_ttar_o_erafied, by = c(
    "cohort_definition_id" = "target_cohort_definition_id",
    "tar_id" = "tar_id",
    "outcome_id" = "outcome_id",
    "subject_id" = "subject_id"
  )) %>%
  filter(start_date <= cohort_start_date, end_date >= cohort_start_date) %>%
  group_by(
    target_cohort_definition_id,
    tar_id,
    subject_id,
    start_date,
    outcome_id
  ) %>%
  summarise(
    pe_outcomes = n(),
    num_outcomes = sum(is.na(target_cohort_definition_id))
  )

# Create the #excluded_person_days temporary table
excluded_person_days <- ttar_erafied %>%
  inner_join(exc_ttar_o_erafied, by = c(
    "cohort_definition_id" = "target_cohort_definition_id",
    "tar_id" = "tar_id",
    "subject_id" = "subject_id"
  )) %>%
  filter(start_date <= start_date, end_date >= end_date) %>%
  group_by(
    cohort_definition_id = target_cohort_definition_id,
    tar_id,
    subject_id,
    start_date,
    outcome_id
  ) %>%
  summarise(
    person_days = sum(as.numeric(end_date - start_date) + 1)
  )

# Create the #incidence_raw temporary table
outcome_ref_data <- outcome_ref %>%
  filter(outcome_id == 1) %>%
  select(outcome_id)

age_group_data <- age_group %>%
  select(age_id, min_age, max_age)

incidence_raw_data <- ttar_erafied %>%
  inner_join(tbl(con, "omop_cdm_53_pmtx_202203.person"), by = c("subject_id" = "person_id")) %>%
  select(
    target_cohort_definition_id,
    tar_id,
    subject_id,
    start_date,
    end_date,
    age = extract(year, start_date) - year_of_birth,
    gender_id = gender_concept_id,
    start_year = extract(year, start_date)
  ) %>%
  cross_join(outcome_ref_data) %>%
  left_join(excluded_person_days, by = c(
    "target_cohort_definition_id",
    "tar_id",
    "subject_id",
    "start_date",
    "outcome_id"
  )) %>%
  left_join(outcome_smry, by = c(
    "target_cohort_definition_id",
    "tar_id",
    "subject_id",
    "start_date",
    "outcome_id"
  )) %>%
  left_join(age_group_data, on = c("age" >= min_age, "age" < max_age))

incidence_raw <- incidence_raw_data %>%
  mutate(
    pe_person_days = as.numeric(end_date - start_date) + 1,
    person_days = pe_person_days - coalesce(person_days, 0),
    pe_outcomes = coalesce(pe_outcomes, 0),
    outcomes = coalesce(outcomes, 0)
  )

