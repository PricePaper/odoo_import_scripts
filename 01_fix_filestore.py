#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

import psycopg2

from scriptconfig import DB_DSN


# DB_DSN needs to be set in the import_script.conf JSON config file
# add a line to the dictionary:
# "db_dsn": "postgresql://[user[:password]@][host][:port][,...][/dbname][?param1=value1&...]"


def delete_files_from_db(files: list) -> None:
    with psycopg2.connect(DB_DSN) as conn:
        cur = conn.cursor()
        sql = "DELETE FROM ir_attachment WHERE store_fname = (%s)"

        for file in files:
            cur.execute(sql, (file,))

        conn.commit()


def main():
    parser = argparse.ArgumentParser(description='Remove files that are missing in filestore from the database')
    parser.add_argument('files', metavar='N', nargs='+', help='one or more file objects to be deleted')
    files = parser.parse_args().files

    delete_files_from_db(files)


if __name__ == "__main__":
    main()
