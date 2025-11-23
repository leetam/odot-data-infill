library(dplyr)
library(tidyr)
library(lubridate)
library(readr)

incidents <- read.csv("data/Incidents_20250901-20251112.csv", stringsAsFactors = F)
responders <- read.csv("data/Incidents_Responders_20250901-20251112.csv", stringsAsFactors = F)
vehicles <- read_csv("data/Incidents_Vehicles_20250901-20251112.csv")

max_lanes_affected
max_priority
max_impact
status
source_name
begin_milepoint
end_milepoint
begin_latitude
end_longitude
route_name
route_direction
city
county
recoverty_begin_time
last_updated
max_vehicle_count
vehicles_make (list)
vehicles_model (list)
vehicles_color (list)
vehicles_tow_company (list)
responders_id
responders_action (list)
responders_timestamp (list)

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

#### Vehicles file ####
vehicles_1 <- vehicles[duplicated(vehicles), ]


#### Responders file ####
responders_1 <- responders[duplicated(responders), ]
