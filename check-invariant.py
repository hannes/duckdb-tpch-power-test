import multiprocessing
import time
import os
import signal 
import duckdb
import pathlib
import subprocess
import random

scale_factor = 1

datadir = f'/Users/hannes/source/tpch/gen/invariant-sf{scale_factor}'
db_file = f'tpch-invariant-sf{scale_factor}.duckdb'
diffdir = 'invariant-checking' # TODO
refdir = 'reference-tables' # TODO\
ext = ''

def export(con, dir, n):
    con.execute(f"COPY (SELECT * FROM lineitem ORDER BY l_orderkey, l_linenumber) TO '{dir}/lineitem.tbl.{n}' (FORMAT CSV, DELIMITER '|', HEADER FALSE)")
    con.execute(f"COPY (SELECT * FROM orders ORDER BY o_orderkey) TO '{dir}/order.tbl.{n}' (FORMAT CSV, DELIMITER '|', HEADER FALSE)")

def refresh(con, n):    
    con.execute("BEGIN TRANSACTION")

    lineitem = f"{datadir}/lineitem.tbl.u{n}{ext}"
    orders = f"{datadir}/orders.tbl.u{n}{ext}"
    con.execute(f"COPY lineitem FROM '{lineitem}' (FORMAT CSV, HEADER FALSE, DELIMITER '|')")
    con.execute(f"COPY orders FROM '{orders}' (FORMAT CSV, HEADER FALSE, DELIMITER '|')")
    delete = f"{datadir}/delete.{n}{ext}"
    con.execute(f"CREATE TEMPORARY TABLE deletes (pk INTEGER, gunk varchar(1))")
    con.execute(f"COPY deletes FROM '{delete}' (FORMAT CSV, HEADER FALSE, DELIMITER '|')")

    con.execute("DELETE FROM orders WHERE o_orderkey IN (SELECT pk FROM deletes)")
    con.execute("DELETE FROM lineitem WHERE l_orderkey IN (SELECT pk FROM deletes)")
    con.execute("DROP TABLE deletes")
    con.execute("DELETE FROM refresh")
    con.execute(f"INSERT INTO refresh VALUES ({n})")
    con.execute("COMMIT")


def diff(f1, f2):
    res = subprocess.call(f'cmp -s {f1} {f2}', shell=True)
    if res > 0:
      raise ValueError(f'Found diff in {f1} <> {f2}!')

# child process, runs updates, gets killed every now and then
def sub():
    con = duckdb.connect(db_file)
    con.execute('SET enable_progress_bar = false')
    while True:
        last_refresh = con.execute("SELECT last_refresh FROM refresh").fetchone()[0]
        #print(last_refresh)

        if last_refresh > 0 and last_refresh % 100 == 0:
            # TODO not kill now, can we communicate with parent?
            export(con, diffdir, last_refresh)
            diff(f'{refdir}/order.tbl.{last_refresh}', f'{diffdir}/order.tbl.{last_refresh}')
            diff(f'{refdir}/lineitem.tbl.{last_refresh}', f'{diffdir}/lineitem.tbl.{last_refresh}')
            print(f'✅ {last_refresh}')

        next_refresh = last_refresh + 1
        if next_refresh > 4000:
            return
        refresh(con, next_refresh)

    con.close()
    del con

# setup, load, control, main process
if __name__ == '__main__':
    print(os.getpid())
    if os.path.exists(db_file):
        os.remove(db_file)

    con = duckdb.connect(db_file)
    schema = pathlib.Path('schema-gunk.sql').read_text()
    con.begin()
    con.execute(schema)
    for t in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
        con.execute(f"COPY {t} FROM '{datadir}/{t}.tbl' (FORMAT CSV, HEADER FALSE, DELIMITER '|')")
    con.execute("CREATE TABLE refresh(last_refresh INTEGER)")
    con.execute("INSERT INTO refresh VALUES (0)")
    export(con, refdir, 0) # TODO
    con.commit()
    con.close()

    while True:
        # con = duckdb.connect(db_file, read_only=True)
        # last_refresh = con.execute("SELECT last_refresh FROM refresh").fetchone()[0]
        # con.close()
        # if last_refresh > 1000:
        #     exit(0)

        p = multiprocessing.Process(target=sub)
        p.start()
        sleep_seconds = random.randint(1, 10)
        print(f'😴 {sleep_seconds}s')
        time.sleep(sleep_seconds)
        print('💀')
        os.kill(p.pid, signal.SIGKILL)
        time.sleep(5)
