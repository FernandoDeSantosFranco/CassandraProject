#!/usr/bin/env python3
import logging
import pandas as pd
import matplotlib.pyplot as plt

month_names = {
    1: 'Enero',
    2: 'Febrero',
    3: 'Marzo',
    4: 'Abril',
    5: 'Mayo',
    6: 'Junio',
    7: 'Julio',
    8: 'Agosto',
    9: 'Septiembre',
    10: 'Octubre',
    11: 'Noviembre',
    12: 'Diciembre'
}

# Set logger
log = logging.getLogger()


CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""


CREATE_FLIGHT_DATA_TABLE = """
    CREATE TABLE IF NOT EXISTS flight_data (
        airline TEXT,
        departure TEXT,
        destination TEXT,
        day INT,
        month INT,
        year INT,
        duration INT,
        ticket_type TEXT,
        age INT,
        gender TEXT,
        carry_on BOOLEAN,
        checked_bags INT,
        reason TEXT,
        stay TEXT,
        transit TEXT,
        connection BOOLEAN,
        wait INT,
        PRIMARY KEY ((airline), departure, destination, year, month, day)
    )
"""

CREATE_INDEX_ON_DESTINATION = """
    CREATE INDEX IF NOT EXISTS destination_index ON flight_data(destination);
"""

CREATE_INDEX_ON_REASON = """
    CREATE INDEX IF NOT EXISTS reason_index ON flight_data(reason);
"""

CREATE_INDEX_ON_STAY = """
    CREATE INDEX IF NOT EXISTS stay_index ON flight_data(stay);
"""

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_FLIGHT_DATA_TABLE)
    session.execute(CREATE_INDEX_ON_DESTINATION)
    session.execute(CREATE_INDEX_ON_REASON)
    session.execute(CREATE_INDEX_ON_STAY)

  
# ----Consultas-----

def get_data(session):
    stmt = session.prepare("SELECT * FROM flight_data")
    rows = session.execute(stmt)
    
    flight_data = []
    for row in rows:
        flight_data.append({
            'airline': row.airline,
            'departure': row.departure,
            'destination': row.destination,
            'day': row.day,
            'month': row.month,
            'year': row.year,
            'duration': row.duration,
            'ticket_type': row.ticket_type,
            'age': row.age,
            'gender': row.gender,
            'carry_on': row.carry_on,
            'checked_bags': row.checked_bags,
            'reason': row.reason,
            'stay': row.stay,
            'transit': row.transit,
            'connection': row.connection,
            'wait': row.wait
        })

    df = pd.DataFrame(flight_data)
    return pd.DataFrame(df)



def get_data_by_destination(session, destination, year=None):
    stmt = session.prepare("SELECT * FROM flight_data WHERE destination = ?")
    rows = session.execute(stmt, [destination])

    flight_data = []
    for row in rows:
        flight_data.append({
            'airline': row.airline,
            'departure': row.departure,
            'destination': row.destination,
            'day': row.day,
            'month': row.month,
            'year': row.year,
            'duration': row.duration,
            'ticket_type': row.ticket_type,
            'age': row.age,
            'gender': row.gender,
            'carry_on': row.carry_on,
            'checked_bags': row.checked_bags,
            'reason': row.reason,
            'stay': row.stay,
            'transit': row.transit,
            'connection': row.connection,
            'wait': row.wait
        })

    df = pd.DataFrame(flight_data)

    if year is not None:
        df = df[df['year'] == year]  # Filtrar por año

    flights_by_month = df.groupby('month').size()

    flights_by_month.plot(kind='bar')
    plt.xlabel('Mes')
    plt.ylabel('Cantidad de vuelos')
    if year is not None:
        plt.title(f'Cantidad de vuelos por mes en el destino {destination} en el año {year}')
    else:
        plt.title(f'Cantidad de vuelos por mes en el destino {destination} (2016-2024)')
    plt.xticks(rotation=0)
    plt.show()

    return df



def get_data_by_month(session, month, year=None):
    stmt = session.prepare("SELECT * FROM flight_data WHERE month = ?")
    rows = session.execute(stmt, [month])

    flight_data = []
    for row in rows:
        flight_data.append({
            'airline': row.airline,
            'departure': row.departure,
            'destination': row.destination,
            'day': row.day,
            'month': row.month,
            'year': row.year,
            'duration': row.duration,
            'ticket_type': row.ticket_type,
            'age': row.age,
            'gender': row.gender,
            'carry_on': row.carry_on,
            'checked_bags': row.checked_bags,
            'reason': row.reason,
            'stay': row.stay,
            'transit': row.transit,
            'connection': row.connection,
            'wait': row.wait
        })

    df = pd.DataFrame(flight_data)

    if year is not None:
        df = df[df['year'] == year]  # Filtrar por año

    flights_by_destination = df.groupby('destination').size()

    flights_by_destination.plot(kind='bar')
    plt.xlabel('Destino')
    plt.ylabel('Cantidad de vuelos')
    if year is not None:
        plt.title(f'Cantidad de vuelos por destino en el mes {month_names[month]} del año {year}')
    else:
        plt.title(f'Cantidad de vuelos por destino en el mes {month_names[month]}. (2016-2024)')
    plt.xticks(rotation=45, ha='right') 
    plt.show()
    

    return df



def get_data_by_reason_and_month(session, year=None, month=None):
    stmt = session.prepare("SELECT * FROM flight_data")
    rows = session.execute(stmt)

    flight_data = []
    for row in rows:
        flight_data.append({
            'airline': row.airline,
            'departure': row.departure,
            'destination': row.destination,
            'day': row.day,
            'month': row.month,
            'year': row.year,
            'duration': row.duration,
            'ticket_type': row.ticket_type,
            'age': row.age,
            'gender': row.gender,
            'carry_on': row.carry_on,
            'checked_bags': row.checked_bags,
            'reason': row.reason,
            'stay': row.stay,
            'transit': row.transit,
            'connection': row.connection,
            'wait': row.wait
        })

    df = pd.DataFrame(flight_data)

    if year is not None:
        df = df[df['year'] == year]

    if month is not None:
        df = df[df['month'] == month]

    flights_by_reason = df.groupby('reason').size()

    flights_by_reason.plot(kind='bar')
    plt.xlabel('Razón')
    plt.ylabel('Cantidad de vuelos')
    if year is not None and month is not None:
        plt.title(f'Cantidad de vuelos por razón en {month_names[month]} del año {year}')
    elif month is not None:
        plt.title(f'Cantidad de vuelos por razón en {month_names[month]}. (2016-2024)')
    elif year is not None:
        plt.title(f'Cantidad de vuelos por razón en el año {year}')
    else:
        plt.title('Cantidad de vuelos por razón')
    plt.xticks(rotation=45, ha='right')
    plt.show()

    return df



def get_data_by_reason(session, reason, year=None):
    stmt = session.prepare("SELECT * FROM flight_data WHERE reason = ?")
    rows = session.execute(stmt, [reason])

    flight_data = []
    for row in rows:
        flight_data.append({
            'airline': row.airline,
            'departure': row.departure,
            'destination': row.destination,
            'day': row.day,
            'month': row.month,
            'year': row.year,
            'duration': row.duration,
            'ticket_type': row.ticket_type,
            'age': row.age,
            'gender': row.gender,
            'carry_on': row.carry_on,
            'checked_bags': row.checked_bags,
            'reason': row.reason,
            'stay': row.stay,
            'transit': row.transit,
            'connection': row.connection,
            'wait': row.wait
        })

    df = pd.DataFrame(flight_data)

    if year is not None:
        df = df[df['year'] == year]

    flights_by_month = df.groupby('month').size()

    flights_by_month.plot(kind='bar')
    plt.xlabel('Mes')
    plt.ylabel('Cantidad de vuelos')
    if year is not None:
        plt.title(f'Distribución de vuelos por mes para {reason} en el año {year}')
    else:
        plt.title(f'Distribución de vuelos por mes para {reason}. (2016-2024)')
    plt.xticks(range(1, 13), [month_names[i] for i in range(1, 13)], rotation=45, ha='right')
    plt.show()

    return df



def get_data_by_stay(session, stay, year=None):
    stmt = session.prepare("SELECT * FROM flight_data WHERE stay = ?")
    rows = session.execute(stmt, [stay])

    flight_data = []
    for row in rows:
        flight_data.append({
            'airline': row.airline,
            'departure': row.departure,
            'destination': row.destination,
            'day': row.day,
            'month': row.month,
            'year': row.year,
            'duration': row.duration,
            'ticket_type': row.ticket_type,
            'age': row.age,
            'gender': row.gender,
            'carry_on': row.carry_on,
            'checked_bags': row.checked_bags,
            'reason': row.reason,
            'stay': row.stay,
            'transit': row.transit,
            'connection': row.connection,
            'wait': row.wait
        })

    df = pd.DataFrame(flight_data)

    if year is not None:
        df = df[df['year'] == year]

    flights_by_month = df.groupby('month').size()

    flights_by_month.plot(kind='bar')
    plt.xlabel('Mes')
    plt.ylabel('Cantidad de vuelos')
    if year is not None:
        plt.title(f'Distribución de vuelos por mes para {stay} en el año {year}')
    else:
        plt.title(f'Distribución de vuelos por mes para {stay}. (2016-2024)')
    plt.xticks(range(1, 13), [month_names[i] for i in range(1, 13)], rotation=45, ha='right')
    plt.show()

    return df