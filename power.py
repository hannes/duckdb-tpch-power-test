# 2024-08-21, hannes@duckdblabs.com

import threading
import duckdb
import pathlib
import tempfile
import time
import functools
import operator

path = tempfile.mkdtemp() + '/tpch.duckdb'
con0 = duckdb.connect(path)
scale_factor = 100
datadir = f'tpch_tools_3.0.1/dbgen/sf{scale_factor}'
schema = pathlib.Path('schema.sql').read_text()
use_parquet = False
reader = 'read_csv'
ext = ''
if use_parquet:
	ext = '.parquet'
	reader = 'read_parquet'

def LOAD(n):
	con = con0.cursor()
	con.begin()
	con.execute(schema)
	for t in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
		con.execute(f"COPY {t} FROM '{datadir}/{t}.tbl{ext}'")
	con.commit()
	con.execute("CHECKPOINT")
	con.close()

def query(n):
	con = con0.cursor()
	queries = pathlib.Path(f'{datadir}/queries{n}.sql').read_text().split(";")
	timings = []
	for q in queries:
		if not 'select' in q:
			continue
		start = time.time()
		con.execute(q)
		duration = time.time() - start
		timings.append(duration)
	# print(f"done q {n}")
	con.close()

	return functools.reduce(operator.mul, timings)

def RF1(n):
	con = con0.cursor()
	con.begin()
	lineitem = f"{datadir}/lineitem.tbl.u{n}{ext}"
	orders = f"{datadir}/orders.tbl.u{n}{ext}"
	con.execute(f"COPY lineitem FROM '{lineitem}'")
	con.execute(f"COPY orders FROM '{orders}'")
	con.commit()
	con.close()


def RF2(n):
	con = con0.cursor()
	con.begin()
	delete = f"{datadir}/delete.{n}{ext}"
	con.execute(f"CREATE OR REPLACE TEMPORARY TABLE deletes AS SELECT column0 FROM {reader}('{delete}')")
	con.execute("DELETE FROM orders WHERE o_orderkey IN (FROM deletes)")
	con.execute("DELETE FROM lineitem WHERE l_orderkey IN (FROM deletes)")
	con.commit()
	con.close()


def timeit(fun, p):
	start = time.time()
	fun(p)
	return time.time() - start

def refresh(ns):
	for n in ns:
		RF1(n)
		RF2(n)
		# print(f"done r {n}")

n_refresh = max(round(scale_factor * 0.1), 1)

print(reader)

LOAD(1)
print("done load")
time_rf1 = timeit(RF1, 1)
time_q = query(1)
time_rf2 = timeit(RF2, 1)

tpch_power_at_size = round((3600*scale_factor)/ ((time_q*time_rf1*time_rf2)**(1/24)), 1)
print(tpch_power_at_size)

# from section 5.3.4 of tpch spec
scale_factor_streams_map = {1:2, 10: 3, 100:5, 1000:7}

streams = scale_factor_streams_map[scale_factor]

start = time.time()

threads = []
for i in range(1, streams+1):
	t = threading.Thread(target=query, args=[i])
	t.start()
	threads.append(t)

r = threading.Thread(target=refresh, args=[range(2, n_refresh+2)])
r.start()
threads.append(r)

for t in threads:
	t.join()

throughput_measurement_interval = time.time() - start

tpch_throughput_at_size = round((streams * 22 * 3600) / throughput_measurement_interval * scale_factor, 1)

print(round((tpch_power_at_size * tpch_throughput_at_size)**(1/2), 1))
