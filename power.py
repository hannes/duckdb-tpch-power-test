# 2024-08-21, hannes@duckdblabs.com

import threading
import duckdb
import pathlib
import tempfile
import time
import functools
import operator
import os
import shutil
import psutil
import datetime

scale_factor = 100

datadir = f'gen/sf{scale_factor}'
template_db_file = f'{datadir}/tpch_template.duckdb'
db_file = f'{datadir}/tpch.duckdb'

# from section 5.3.4 of tpch spec
scale_factor_streams_map = {1: 2, 10: 3, 30: 4, 100: 5, 300: 6, 1000: 7, 3000: 8, 10000: 9}
streams = scale_factor_streams_map[scale_factor]

print(f"Scale factor {scale_factor}")
use_parquet = True
reader = 'read_csv'
ext = ''
if use_parquet:
	ext = '.parquet'
	reader = 'read_parquet'
	print("Parquet refresh")
else:
	print("CSV refresh")

logfile = f'log-sf{scale_factor}-{str(datetime.datetime.now(datetime.UTC)).replace(' ','T')}.tsv'
proceed = True

def monitor():
	log = open(logfile, 'wb')
	print(f"Logging to {logfile}")

	proc = psutil.Process()
	log.write('\t'.join([
		'time_offset',
		'cpu_percent',
		'cpu_user',
		'cpu_system',
		'memory_rss',
		'memory_vms'
		]).encode('utf8') + b'\n')
	start = time.time()
	while proceed:
		cpu_times = proc.cpu_times()
		memory_info = proc.memory_info()
		log.write('\t'.join(str(x) for x in [
			round(time.time()-start,2),
			round(proc.cpu_percent()),
			round(cpu_times.user,2),
			round(cpu_times.system,2),
			memory_info.rss,
			memory_info.vms
			]).encode('utf8') + b'\n')
		time.sleep(1)
		log.flush()

threading.Thread(target=monitor).start()

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
else:
	print(f"cached db from {template_db_file}")

shutil.copyfile(template_db_file, db_file)
con0 = duckdb.connect(db_file)
con0.execute(f"SET wal_autocheckpoint='{scale_factor}MB'")
# con0.execute("SET threads='1'")

def query(n):
	print(f"starting query stream {n}")
	con = con0.cursor()
	queries = pathlib.Path(f'{datadir}/queries{n}.sql').read_text().split(";")
	timings = []
	query_idx = 1
	for q in queries:
		if not 'select' in q:
			continue
		start = time.time()
		con.execute("BEGIN TRANSACTION READ ONLY")
		con.execute(q)
		con.execute("COMMIT")
		duration = time.time() - start
		print(f"done query {n} {query_idx} {round(duration, 2)}")
		timings.append(duration)
		query_idx = query_idx + 1
	con.close()
	time_prod = functools.reduce(operator.mul, timings)
	print(f"done query stream {n}")
	return time_prod

def RF1(n):
	con = con0.cursor()
	con.begin()
	lineitem = f"{datadir}/lineitem.tbl.u{n}{ext}"
	orders = f"{datadir}/orders.tbl.u{n}{ext}"
	con.execute(f"INSERT INTO lineitem FROM '{lineitem}'")
	con.execute(f"INSERT INTO orders FROM '{orders}'")
	con.commit()
	con.close()


def RF2(n):
	con = con0.cursor()
	con.begin()
	delete = f"{datadir}/delete.{n}{ext}"
	con.execute(f"DELETE FROM orders WHERE o_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con.execute(f"DELETE FROM lineitem WHERE l_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con.commit()
	con.close()


def RF(con, n):	
	print(f"start refresh {n}")
	con.begin()
	lineitem = f"{datadir}/lineitem.tbl.u{n}{ext}"
	orders = f"{datadir}/orders.tbl.u{n}{ext}"
	delete = f"{datadir}/delete.{n}{ext}"
	con.execute(f"INSERT INTO lineitem FROM '{lineitem}'")
	con.execute(f"INSERT INTO orders FROM '{orders}'")
	con.execute(f"DELETE FROM orders WHERE o_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con.execute(f"DELETE FROM lineitem WHERE l_orderkey IN (SELECT column0 FROM {reader}('{delete}'))")
	con.commit()
	print(f"done refresh {n}")


def timeit(fun, p):
	start = time.time()
	fun(p)
	return time.time() - start

def refresh(ns):
	con = con0.cursor()
	for n in ns:
		RF(con, n)

n_refresh = max(round(scale_factor * 0.1), 1)

time_rf1 = timeit(RF1, 1)
time_q = query(1)
time_rf2 = timeit(RF2, 1)

tpch_power_at_size = round((3600*scale_factor)/ ((time_q*time_rf1*time_rf2)**(1/24)), 2)
print(f"tpch_power_at_size              = {tpch_power_at_size}")

start = time.time()


threads = []
print(f"running {streams} query streams, {n_refresh} refresh sets")

for i in range(1, streams+1):
	t = threading.Thread(target=query, args=[i])
	t.start()
	threads.append(t)

r = threading.Thread(target=refresh, args=[range(2, n_refresh+2)])
r.start()
threads.append(r)

for t in threads:
	t.join()

proceed = False

throughput_measurement_interval = round(time.time() - start, 2)
tpch_throughput_at_size = round((streams * 22 * 3600) / throughput_measurement_interval * scale_factor, 2)
tpch_qphh_at_size = round((tpch_power_at_size * tpch_throughput_at_size)**(1/2), 2)

print(f"throughput_measurement_interval = {throughput_measurement_interval}")
print(f"tpch_throughput_at_size         = {tpch_throughput_at_size}")
print(f"tpch_qphh_at_size               = {tpch_qphh_at_size}")
