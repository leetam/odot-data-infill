library(dplyr)
library(tidyr)
library(lubridate)
library(readr)

incidents <- read.csv("data/Incidents_20250901-20251112.csv", stringsAsFactors = F)
responders <- read.csv("data/Incidents_Responders_20250901-20251112.csv", stringsAsFactors = F)
vehicles <- read_csv("data/Incidents_Vehicles_20250901-20251112.csv")

# max_lanes_affected
# max_priority
# max_impact
# status
# source_name
# begin_milepoint
# end_milepoint
# begin_latitude
# end_longitude
# route_name
# route_direction
# city
# county
# recoverty_begin_time
# last_updated
# max_vehicle_count
# vehicles_make (list)
# vehicles_model (list)
# vehicles_color (list)
# vehicles_tow_company (list)
# responders_id
# responders_action (list)
# responders_timestamp (list)

incidents_data_1 <- incidents |>
  select(
    incident_id = EventID,
    incident_type = EventTypeID,
    subtype = EventSubtypeID,
    create_time = CreationTime,
    clear_time = ClearTime,
    event_source = EventSource,
    max_lanes_affected = LanesAffected,
    max_priority = EventPriority,
    max_impact = ImpactDisplayOrder,
    status = Status,
    source_name = SourceName,
    begin_milepoint = BeginMP,
    end_milepoint = EndMP,
    begin_latitude = BeginLatitude,
    begin_longitude = BeginLongitude,
    end_latitude = EndLatitude,
    end_longitude = EndLongitude,
    route_name = Route,
    route_direction = Direction,
    city = City,
    county = County,
    recovery_begin_time = RecoveryBeginTime,
    last_updated = LastUpdateTime,
    max_vehicle_count = VehicleCount
  )

duplicates <- incidents_data_1[duplicated(incidents_data_1$incident_id), ]
duplicates_all <- incidents_data_1 |>
  filter(incident_id %in% duplicates$incident_id)

incidents_data_2 <- incidents_data_1[!duplicated(incidents_data_1), ]

no_incident_ids <- incidents_data_2 |>
  filter(is.na(subtype))
#193

#### Vehicles file ####
vehicles_1 <- vehicles[duplicated(vehicles), ]
vehicles_2 <- vehicles[!duplicated(vehicles), ]
vehicles_3 <- vehicles_2 |>
  group_by(EVNT_ID) |>
  mutate(
    vehicles_make = paste0(VechicleMake, collapse = ", "),
    vehicles_model = paste0(VehicleModel, collapse = ", "),
    vehicles_color = paste0(VehicleColor, collapse = ", "),
    vehicles_tow_company = paste0(TowCompany, collapse = ", ")
  ) |>
  distinct(EVNT_ID, .keep_all = T) |>
  select(
    incident_id = EVNT_ID,
    vehicles_make,
    vehicles_model,
    vehicles_color,
    vehicles_tow_company
  )

#### Responders file ####
responders_1 <- responders[duplicated(responders), ]
responders_2 <- responders[!duplicated(responders), ]
responders_3 <- responders_2 |>
  group_by(EVNT_ID) |>
  mutate(
    responders_id = paste0(ResponderId, collapse = ", "),
    responders_action = paste0(RADIO_TRAF_TXT, collapse = ", "),
    responders_timestamp = paste0(CMMNC_DTTM, collapse = ", ")
  ) |>
  distinct(EVNT_ID, .keep_all = T) |>
  select(
    incident_id = EVNT_ID,
    responders_id,
    responders_action,
    responders_timestamp
  )

#### Build file to insert into incidents table
incidents_upload <- incidents_data_2 |>
  left_join(vehicles_3, by = "incident_id") |>
  left_join(responders_3, by = "incident_id") |>
  filter(!incident_id %in% no_incident_ids$incident_id)

saveRDS(incidents_upload, "data/incidents_upload.rds")


#### Incidents notes to follow-up with Chad and Greyson and updates to the Wiki
# ~193 entries with no eventid, instead timestampe - do we delete those
# duplicates where all attributes match - delete duplicates
#  incidents: 11
#  vehicles: 31
#  responders: 0
# other cases where it looks like a duplicate, but tow company name is different, does this imply that
#  multiple tow companies were called for an incident until one showed up?
# What's the case for when the create time is two years ago but recently closed?
# Documentation of event type and event sub type?