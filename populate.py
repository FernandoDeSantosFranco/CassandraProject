#!/usr/bin/env python3
import datetime
import random
import uuid

CQL_FILE = 'data.cql'

def cql_stmt_generator(flights_num=10000):
    with open(CQL_FILE, "w") as fd:
        # Generate data for flight_data
        # Generate flights
        for i in range(flights_num):
            airline = random.choice(["Aeromexico", "Delta", "American Airlines", "United", "Volaris", "Alaska","Delta Airlines"])
            departure = random.choice(["SFO", "LAX", "JFK", "ORD", "ATL"])
            destination = random.choice(["LHR", "CDG", "MAD", "FCO", "SYD", "GDL", "SJC"])
            day = random.randint(1, 30)
            month = random.randint(1, 12)
            year = random.randint(2016, 2024)
            duration = random.randint(1, 1000)
            ticket_type = random.choice(["Business", "Economy", "First Class"])
            age = random.randint(1, 100)
            gender = random.choice(["male", "female"])
            carry_on = random.choice([True, False])
            if carry_on == False:
                checked_bags = 0
            else:
                checked_bags = random.randint(1, 5)
            reason = random.choice(["On Vacation/Pleasure", "Business/Work", "Back home"])
            stay = random.choice(["Hotel", "Friend/Family", "Home", "Airbnb"])
            transit = random.choice(["Airport cab", "Car rental", "Uber or similar", "Public transportation", "Pickup"])
            connection = random.choice([True, False])
            if connection == True:
                wait = random.randint(80, 900)
            else:
                wait = 0

            fd.write(f"INSERT INTO flight_data (airline, departure, destination, day, month, year, duration, ticket_type, age, gender, carry_on, checked_bags, reason, stay, transit, connection, wait) VALUES ('{airline}', '{departure}', '{destination}', {day}, {month}, {year}, {duration}, '{ticket_type}', {age}, '{gender}', {carry_on}, {checked_bags}, '{reason}', '{stay}', '{transit}', {connection}, {wait});\n")


def main():
    cql_stmt_generator()


if __name__ == "__main__":
    main()
