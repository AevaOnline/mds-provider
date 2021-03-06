"""
SQL scripts for MDS Provider database CRUD.
"""

import mds


def on_conflict_statement(on_conflict_update=None):
    """
    Generate an appropriate ON CONFLICT... statement.

    :on_conflict_update: a tuple of (condition, actions) used to generate a statement like
    ON CONFLICT :condition: DO UPDATE SET :actions:
    """
    if on_conflict_update:
        condition, actions = on_conflict_update
        if isinstance(actions, list):
            actions = ",".join(actions)
        elif isinstance(actions, dict):
            actions = ",".join([f"{k} = {v}" for k,v in actions.items()])
        return f"ON CONFLICT {condition} DO UPDATE SET {actions}"
    else:
        return "ON CONFLICT DO NOTHING"


def insert_status_changes_from(source_table, dest_table=mds.STATUS_CHANGES, on_conflict_update=None):
    """
    Generate an INSERT INTO statement from :source_table: to the Status Changes table.
    """
    on_conflict = on_conflict_statement(on_conflict_update)

    return f"""
    INSERT INTO "{dest_table}"
    (
        provider_id,
        provider_name,
        device_id,
        vehicle_id,
        vehicle_type,
        propulsion_type,
        event_type,
        event_type_reason,
        event_time,
        event_location,
        battery_pct,
        associated_trips
    )
    SELECT
        cast(provider_id as uuid),
        provider_name,
        cast(device_id as uuid),
        vehicle_id,
        cast(vehicle_type as vehicle_types),
        cast(propulsion_type as propulsion_types[]),
        cast(event_type as event_types),
        cast(event_type_reason as event_type_reasons),
        to_timestamp(event_time) at time zone 'UTC',
        cast(event_location as json),
        battery_pct,
        cast(associated_trips as uuid[])
    FROM "{source_table}"
    { on_conflict }
    ;
    """

def insert_trips_from(source_table, dest_table=mds.TRIPS, on_conflict_update=None):
    """
    Generate an INSERT INTO statement from :source_table: to the Trips table.
    """
    on_conflict = on_conflict_statement(on_conflict_update)

    return f"""
    INSERT INTO "{dest_table}"
    (
        provider_id,
        provider_name,
        device_id,
        vehicle_id,
        vehicle_type,
        propulsion_type,
        trip_id,
        trip_duration,
        trip_distance,
        route,
        accuracy,
        start_time,
        end_time,
        parking_verification_url,
        standard_cost,
        actual_cost
    )
    SELECT
        cast(provider_id as uuid),
        provider_name,
        cast(device_id as uuid),
        vehicle_id,
        cast(vehicle_type as vehicle_types),
        cast(propulsion_type as propulsion_types[]),
        cast(trip_id as uuid),
        trip_duration,
        trip_distance,
        cast(route as json),
        accuracy,
        to_timestamp(start_time) at time zone 'UTC',
        to_timestamp(end_time) at time zone 'UTC',
        parking_verification_url,
        standard_cost,
        actual_cost
    FROM "{source_table}"
    { on_conflict }
    ;
    """
