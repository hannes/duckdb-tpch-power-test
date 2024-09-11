import time
import os
import signal 
import duckdb
import pathlib
import subprocess
import random
import tempfile
import multiprocessing
import multiprocessing.shared_memory

scale_factor = 1

datadir = f'/Users/hannes/source/tpch/gen/invariant-sf{scale_factor}'
db_file = f'tpch-invariant-sf{scale_factor}.duckdb'
diffdir = 'invariant-checking' #tempfile.mkdtemp()
refdir = 'reference-tables' # TODO
ext = ''
shm_size = 4
shm_name = "shm_last_refresh"
refresh_count = 4000


schema = '''
CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    VARCHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  VARCHAR(15) NOT NULL,  
                           O_CLERK          VARCHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL,
                            gunk varchar(1));

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  VARCHAR(1) NOT NULL,
                             L_LINESTATUS  VARCHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT VARCHAR(25) NOT NULL,
                             L_SHIPMODE     VARCHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL,
                            gunk varchar(1));
'''

schema_with_pk = '''
CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    VARCHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  VARCHAR(15) NOT NULL,  
                           O_CLERK          VARCHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL,
                            gunk varchar(1),
                            primary key (o_orderkey));

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  VARCHAR(1) NOT NULL,
                             L_LINESTATUS  VARCHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT VARCHAR(25) NOT NULL,
                             L_SHIPMODE     VARCHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL,
                            gunk varchar(1),
                            primary key (L_ORDERKEY, L_LINENUMBER));
'''

def export(con, dir, n):
    con.execute(f"COPY (SELECT * FROM lineitem ORDER BY l_orderkey, l_linenumber) TO '{dir}/lineitem.tbl.{n}' (FORMAT CSV, DELIMITER '|', HEADER FALSE)")
    con.execute(f"COPY (SELECT * FROM orders ORDER BY o_orderkey) TO '{dir}/order.tbl.{n}' (FORMAT CSV, DELIMITER '|', HEADER FALSE)")

def refresh(con, n):    
    con.execute("BEGIN TRANSACTION")
    # print(n)
    # lineitem = f"{datadir}/lineitem.tbl.u{n}{ext}"
    # orders = f"{datadir}/orders.tbl.u{n}{ext}"
    # con.execute(f"COPY lineitem FROM '{lineitem}' (FORMAT CSV, HEADER FALSE, DELIMITER '|')")
    # con.execute(f"COPY orders FROM '{orders}' (FORMAT CSV, HEADER FALSE, DELIMITER '|')")
    # delete = f"{datadir}/delete.{n}{ext}"
    # con.execute(f"CREATE TEMPORARY TABLE deletes (pk INTEGER, gunk varchar(1))")
    # con.execute(f"COPY deletes FROM '{delete}' (FORMAT CSV, HEADER FALSE, DELIMITER '|')")

    # con.execute("DELETE FROM orders WHERE o_orderkey IN (SELECT pk FROM deletes)")
    # con.execute("DELETE FROM lineitem WHERE l_orderkey IN (SELECT pk FROM deletes)")
    # con.execute("DROP TABLE deletes")

    con.execute(f"INSERT INTO lineitem SELECT * EXCLUDE refresh_set FROM '{datadir}/lineitem_refresh.parquet' WHERE refresh_set={n}")
    con.execute(f"INSERT INTO orders SELECT * EXCLUDE refresh_set FROM '{datadir}/orders_refresh.parquet' WHERE refresh_set={n}")
    con.execute(f"CREATE TEMPORARY TABLE deletes (pk INTEGER)")
    con.execute(f"INSERT INTO deletes SELECT * EXCLUDE refresh_set FROM '{datadir}/delete_refresh.parquet' WHERE refresh_set={n}")

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
    shm = multiprocessing.shared_memory.SharedMemory(shm_name)
    con = duckdb.connect(db_file)
    con.execute('SET enable_progress_bar = false')
    con.execute('SET checkpoint_threshold = "100MB"')

    while True:
        last_refresh = con.execute("SELECT last_refresh FROM refresh").fetchone()[0]
        shm.buf[:shm_size] = last_refresh.to_bytes(shm_size, 'big')

        next_refresh = last_refresh + 1
        if next_refresh > refresh_count:
            break
        refresh(con, next_refresh)
    con.close()
    shm.unlink()

# setup, load, control, main process
if __name__ == '__main__':
    if os.path.exists(db_file):
        os.remove(db_file)

    con = duckdb.connect(db_file)
    con.begin()
    con.execute(schema)
    for t in ['lineitem', 'orders']:
        con.execute(f"COPY {t} FROM '{datadir}/{t}.tbl.parquet'")
    con.execute("CREATE TABLE refresh(last_refresh INTEGER)")
    con.execute("INSERT INTO refresh VALUES (0)")

    last_refresh = 0
    # export(con, diffdir, last_refresh)
    # diff(f'{refdir}/order.tbl.{last_refresh}', f'{diffdir}/order.tbl.{last_refresh}')
    # diff(f'{refdir}/lineitem.tbl.{last_refresh}', f'{diffdir}/lineitem.tbl.{last_refresh}')

    con.commit()
    con.close()

    # we use shared memory to report the progress back, we need to adjust kill interval based on progress
    shm = multiprocessing.shared_memory.SharedMemory(name=shm_name, create=True, size=shm_size)
    shm.buf[:shm_size] = last_refresh.to_bytes(shm_size, 'big')

    while True:
        p = multiprocessing.Process(target=sub)
        p.start()
        # we sleep until a random number of refreshes has completed
        refreshes_this_round = random.randint(0,10)
        while True:
            time.sleep(0.01)
            last_reported_refresh = int.from_bytes(shm.buf[:shm_size], 'big')
            refresh_progress = last_reported_refresh - last_refresh
            if refresh_progress > refreshes_this_round or last_reported_refresh == refresh_count:
                last_refresh = last_reported_refresh
                break

        # if the process has exited cleanly we are done
        if p.exitcode == 0 or last_refresh == refresh_count:
            break

        # if not we kill it now
        print(f'💀 {last_refresh}')
        os.kill(p.pid, signal.SIGKILL)
        p.join()

    print('🏁')
    time.sleep(1)
    con = duckdb.connect(db_file, read_only=True)
    last_refresh = refresh_count
    export(con, diffdir, last_refresh)
    con.close()
    diff(f'{refdir}/order.tbl.{last_refresh}', f'{diffdir}/order.tbl.{last_refresh}')
    diff(f'{refdir}/lineitem.tbl.{last_refresh}', f'{diffdir}/lineitem.tbl.{last_refresh}')
    print('✅')
