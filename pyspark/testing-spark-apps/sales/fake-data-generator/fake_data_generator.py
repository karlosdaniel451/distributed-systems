import csv
import datetime
import sys

import numpy as np
from faker import Faker

fake = Faker()

DATA_FIELDS = ('date', 'store_location', 'number_of_sales')

POSSIBLE_LOCATIONS = ('Bangalore', 'Amsterdam', 'Warsaw',
                      'London', 'Paris', 'New York', 'Los Angeles',
                      'São Paulo', 'Buenos Aires', 'Melbourne', 'Shanghai',
                      'Tokyo', 'Seoul', 'Johannesburg')


def main():
    print(sys.argv)
    INITIAL_DATE = datetime.date.fromisoformat(sys.argv[1])
    NUMBER_OF_DAYS = int(sys.argv[2])
    FILE_PATH = sys.argv[3]

    fake_data = generate_fake_data(INITIAL_DATE, NUMBER_OF_DAYS)
    fake_data = sort_data_by_date(fake_data)
    export_data(fake_data, FILE_PATH)


def generate_fake_data(
    initial_date: datetime.date,
    number_of_days: int
) -> list[dict]:

    data: list[dict] = []

    for i in range(number_of_days):
        date = initial_date + datetime.timedelta(days=i)

        for location in POSSIBLE_LOCATIONS:
            number_of_sales = np.clip(np.random.normal(10, 5, 1).astype(int), 1, None)[0]
            data.append(
                {
                    'store_location': location,
                    'date': date,
                    'number_of_sales': number_of_sales
                }
            )

    return data


def export_data(data: list[dict], filepath: str):
    with open(filepath, mode='w') as f:
        writer = csv.DictWriter(f, fieldnames=DATA_FIELDS)

        writer.writeheader()
        writer.writerows(data)


def sort_data_by_date(data: list[dict]) -> list[dict]:
    return sorted(data, key=lambda element: element['date'])


if __name__ == '__main__':
    main()
