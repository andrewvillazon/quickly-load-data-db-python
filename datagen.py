from faker import Faker
import pandas as pd


FAKE = Faker()
Faker.seed(42)
FIELDS = ["job", "company", "name", "sex", "mail", "birthdate"]


def create_dummy_data():
    return {k: v for k, v in FAKE.profile().items() if k in FIELDS}


def create_dummy_file(file_path, n_records=10000):
    data = [create_dummy_data() for _ in range(n_records)]

    df = pd.DataFrame(columns=FIELDS).from_dict(data)
    df.to_csv(file_path, sep="|", index_label="id")


if __name__ == "__main__":
    create_dummy_file("dummy_data.csv", n_records=150366)
