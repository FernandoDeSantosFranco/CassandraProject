#!/usr/bin/env python3
import logging
import os
import argparse

from cassandra.cluster import Cluster
import matplotlib.pyplot as plt

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('flight_app.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'flights_project')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_data(session):
    print(model.get_data(session))

def print_data_by_destination(session, destination, year=None):
    df =model.get_data_by_destination(session, destination, year=year)
    print(df)

def print_data_by_month(session, month, year=None):
    df = model.get_data_by_month(session, month, year=year)
    print(df)

def print_data_by_reason__and_month(session, year=None, month=None):
    df = model.get_data_by_reason_and_month(session, year=year, month=month)
    print(df)

def print_data_by_reason(session, reason, year=None):
    df = model.get_data_by_reason(session, reason, year=year)
    print(df)

def print_data_by_stay(session, stay, year=None):
    df = model.get_data_by_stay(session, stay, year=year)
    print(df)

def main():
    parser = argparse.ArgumentParser(description='Flight Information App')
    parser.add_argument('command', choices=['1', '2', '3', '4','5','6'],
                        help='Command to execute')
    parser.add_argument('--airline', help='Airline name')
    parser.add_argument('--customer_id', help='Customer ID')
    parser.add_argument('--departure', help='Departure airport')
    parser.add_argument('--destination', help='Destination airport')
    parser.add_argument('--year', type=int, help='Year')
    parser.add_argument('--month', type=int, help='Month')
    parser.add_argument('--day', type=int, help='Day')
    parser.add_argument('--reason', help="Reason of flight")
    parser.add_argument('--stay', help="Where is the traveler staying")

    args = parser.parse_args()

    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)


    if args.command == '1':
        print_data(session)
    if args.command == '2':
        print_data_by_destination(session, args.destination, year=args.year)
    if args.command == '3':
        print_data_by_month(session, args.month, year=args.year)
    if args.command == '4':
        print_data_by_reason__and_month(session, year=args.year, month=args.month)
    if args.command == '5':
        print_data_by_reason(session, args.reason, year=args.year)
    if args.command == '6':
        print_data_by_stay(session, args.stay, year=args.year)

if __name__ == '__main__':
    main()
