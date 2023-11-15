import argparse

from deltastore.reader import load_as_pandas

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", help="profile path", default="profile.json")
    parser.add_argument("--share", help="share name", default="deltastore")
    parser.add_argument("--schema", help="schema name", default="testsets")
    parser.add_argument("--table", help="table name", default="testset100")
    parser.add_argument("--date", help="date partition")
    parser.add_argument("--limit", help="limit count", type=int)
    parser.add_argument("--columns", help="column names")
    parser.add_argument("--version", help="version number", type=int)
    args = parser.parse_args()

    url = "{}#{}.{}.{}".format(args.profile, args.share, args.schema, args.table)

    predicates = []
    if args.date:
        predicates.append(("date", "=", args.date))

    df = load_as_pandas(
        url,
        predicates=predicates,
        columns=args.columns.split(",") if args.columns else None,
        limit=args.limit,
        version=args.version,
    )
    print(df.count())
