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
import shutil
import threading
import datetime
import math
import sqlite3

scale_factor = 1 # always 1?

datadir = f'gen/sf{scale_factor}'
template_db_file = f'{datadir}/tpch_template.duckdb'
db_file = f'{datadir}/acid-tpch-durability.duckdb'

consistency_threads = 10
min_transactions_limit = 1000
shm_size = 4
shm_name = "shm_min_transactions"


def trunc(n, p):
    return (math.floor(n*(10^p))) / (10^p)

def random_order(con):
    return con.execute("SELECT o_orderkey FROM orders USING SAMPLE 1").fetchone()[0]

def l_key_for_o_key(con, o_key):
    return con.execute(f"SELECT max(l_linenumber) FROM lineitem where l_orderkey={o_key}").fetchone()[0]

def random_delta():
    return random.randint(1,100)

def get_timestamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_state(con, l_key, o_key):
    ototal = con.execute(f"SELECT o_totalprice FROM orders where o_orderkey={o_key}").fetchone()[0]
    extprice, quantity, p_key, s_key = con.execute(f"SELECT l_extendedprice, l_quantity,  l_partkey, l_suppkey FROM lineitem WHERE l_orderkey={o_key} AND l_linenumber={l_key}").fetchone()
    return {'l_key' : l_key, 'o_key':o_key, 'ototal': ototal, 'extprice' : extprice, 'quantity': quantity, 'p_key': p_key, 's_key' : s_key}


def acid_transaction(con, o_key, l_key, delta, date):
    con.begin()
    ototal = con.execute(f"SELECT o_totalprice FROM orders WHERE o_orderkey={o_key}").fetchone()[0]
    quantity, extprice, pkey, skey, tax, disc = con.execute(f"SELECT l_quantity, l_extendedprice, l_partkey, l_suppkey, l_tax, l_discount FROM lineitem WHERE l_orderkey={o_key} AND l_linenumber={l_key}").fetchone()

    ototal = ototal - trunc(trunc(extprice * (1 - disc), 2) * (1 + tax), 2)
    rprice = trunc(extprice/quantity, 2)
    cost = trunc(rprice * delta, 2)
    new_extprice = extprice + cost
    new_ototal = trunc(new_extprice * (1.0 - disc), 2)
    new_ototal = trunc(new_ototal * (1.0 + tax), 2)
    new_ototal = ototal + new_ototal
    new_quantity = quantity + delta
    con.execute(f"UPDATE lineitem SET l_extendedprice = {new_extprice}, l_quantity = {new_quantity} WHERE l_orderkey={o_key} AND l_linenumber={l_key}")
    con.execute(f"UPDATE orders SET o_totalprice = {new_ototal} WHERE o_orderkey={o_key}")
    con.execute(f"INSERT INTO history VALUES ({pkey}, {skey}, {o_key}, {l_key}, {delta}, '{date}')")
    return {'rprice':rprice, 'quantity':quantity, 'tax':tax, 'disc':disc, 'extprice':extprice, 'ototal':ototal, 'new_extprice' : new_extprice, 'new_quantity' : new_quantity, 'new_ototal' : new_ototal, 'delta' : delta, 'date' : date, 'p_key': pkey, 's_key' : skey} 


def check_consistency_condition(con):
    # spec says at least ten orders but we can just do all because we're duckdb
    assert con.execute("SELECT bool_and(condition_holds) is_consistent from (SELECT o_totalprice, sum(trunc(trunc(l_extendedprice *(1 - l_discount),2) * (1+l_tax),2)) totalprice_derived, abs(o_totalprice - totalprice_derived) < 1 condition_holds FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY l_orderkey, o_totalprice)").fetchone()[0]

# child process, runs updates, gets killed every now and then
def child_process():
    shm = multiprocessing.shared_memory.SharedMemory(shm_name)

    con = duckdb.connect(db_file)

    transaction_count = {}

    # thread in cild process, runs transactions
    def refresh_thread(i, con0):
        con = con0.cursor()
        sqlite_log_file = f'{datadir}/acid-tpch-durability-{i}.sqlite'
        if os.path.exists(sqlite_log_file):
            os.remove(sqlite_log_file)
        sqlite = sqlite3.connect(sqlite_log_file)
        sqlite.execute("CREATE TABLE log (p_key integer, s_key integer, o_key integer, l_key integer, delta integer, date_t timestamp)")

        while True:
            delta = random_delta()
            date = get_timestamp()
            o_key = random_order(con)
            l_key = l_key_for_o_key(con, o_key)
            transaction_result = acid_transaction(con, o_key, l_key, delta, date)
            con.commit()   
            sqlite.execute("INSERT INTO log VALUES (?, ?, ?, ?, ?, ?)", [transaction_result['p_key'],transaction_result['s_key'],o_key,l_key,delta,date])
            sqlite.commit()
            transaction_count[i] = transaction_count[i] + 1
        # never happens
        con.close()
        sqlite.close()

    threads = []

    for i in range(consistency_threads):
        transaction_count[i] = 0
        t = threading.Thread(target=refresh_thread, args=[i, con])
        t.start()
        threads.append(t)

    while True:
        time.sleep(0.1)
        min_transactions = min(transaction_count.values())
        shm.buf[:shm_size] = min_transactions.to_bytes(shm_size, 'big')

    # never gonna happen, but well
    for t in threads:
        t.join()

    con.close()
    shm.unlink()


# setup, load, control, main process
if __name__ == '__main__':
    
    # create db template file if not exists
    if os.path.exists(db_file):
        os.remove(db_file)

    wal_file = f"{db_file}.wal"
    if os.path.exists(wal_file):
        os.remove(wal_file)

    if not os.path.exists(template_db_file):
        print(f"begin loading into {template_db_file}")
        con = duckdb.connect(template_db_file)
        schema = pathlib.Path('schema.sql').read_text()
        con.execute(schema)
        for t in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
            con.execute(f"COPY {t} FROM '{datadir}/{t}.tbl'")
        con.commit()
        con.execute("CHECKPOINT")
        con.execute("CHECKPOINT")
        con.close()
        print("done loading")

    shutil.copyfile(template_db_file, db_file)
    con = duckdb.connect(db_file)

    # need to create the history table and the trunc macro
    con.execute("CREATE TABLE IF NOT EXISTS history (h_p_key bigint, h_s_key bigint, h_o_key bigint, h_l_key bigint, h_delta integer, h_date_t datetime)")
    con.execute("CREATE MACRO IF NOT EXISTS trunc(n, p) as (floor(n*(10^p)))/(10^p)::DECIMAL(15,2)")

    # 1. Verify that the ORDERS, and LINEITEM tables are initially consistent as defined in Clause 3.3.2.1, based on a random sample of at least 10 distinct values of O_ORDERKEY
    check_consistency_condition(con)
    con.close()

    # 2. Submit ACID transactions from a number of concurrent streams. 
    # we use shared memory to report the progress back
    shm = multiprocessing.shared_memory.SharedMemory(name=shm_name, create=True, size=shm_size)
    shm.buf[:shm_size] = (0).to_bytes(shm_size, 'big')
    p = multiprocessing.Process(target=child_process)
    p.start()

    # 3. Wait until at least 100 of the ACID transactions from each stream submitted in Step 2 have completed. 
    while True:
        time.sleep(0.1)
        min_transactions = int.from_bytes(shm.buf[:shm_size], 'big')
        if min_transactions > min_transactions_limit:
            break

    # Cause the failure selected from the list in Clause 3.5.3. 
    os.kill(p.pid, signal.SIGKILL)
    p.join()

    time.sleep(1)

    # 4. Restart the system under test using normal recovery procedures.
    con = duckdb.connect(db_file)

    # each thread had its own success log sqlite file to avoid contention, so attach them all
    log_databases = []
    for i in range(consistency_threads):
        sqlite_log_file = f'{datadir}/acid-tpch-durability-{i}.sqlite'
        con.execute(f"ATTACH '{sqlite_log_file}' AS success_{i} (TYPE SQLITE)")
        log_databases.append(f'success_{i}')
    con.execute('CREATE TEMPORARY VIEW success AS ' + ' UNION ALL '.join([f'FROM {d}.log' for d in log_databases]))

    # 5. Compare the contents of the durability success file and the HISTORY table to verify that records in the success file for a committed ACID Transaction have a corresponding record in the HISTORY table and that no success record exists for uncommitted transactions. 
    log_entries_without_history_entry = con.execute("SELECT count(*) FROM (from success except from history)").fetchone()[0]
    assert log_entries_without_history_entry == 0

    # Count the number of entries in the success file and in the HISTORY table and report any difference. Can't be more than threads.
    history_entry_without_log_entry = con.execute("SELECT (SELECT count(*) FROM history) - (SELECT count(*) FROM success)").fetchone()[0]
    assert history_entry_without_log_entry < consistency_threads

    # 6. Re-verify the consistency of the ORDERS, and LINEITEM tables as defined in Clause 3.3.2.1.
    check_consistency_condition(con)

    con.close()    
    shm.unlink()
    print('✅')
