import argparse

import numpy as np
import pandas as pd
from faker import Faker

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", help="rows count", type=int, default=100)
    parser.add_argument("-o", "--output", help="output filename", default="testset.csv")
    args = parser.parse_args()

    fake = Faker()

    columns = ["date", "name", "nation", "score"]

    rows = []
    for i in range(args.count):
        date = fake.date_this_year()
        rows.append(
            [
                date,
                fake.name(),
                fake.country(),
                np.random.randint(100),
            ]
        )
    df = pd.DataFrame(rows, columns=columns)
    df.to_csv(args.output, header=True, index=False)
