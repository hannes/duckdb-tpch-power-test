import threading
import duckdb
import pathlib
import tempfile
import time

path = tempfile.mkdtemp() + '/tpch.duckdb'
con = duckdb.connect(path)
datadir = 'tpch_tools_3.0.1/dbgen'
schema = pathlib.Path('schema.sql').read_text()
refresh_n = 100
loop = True

con.begin()
con.execute(schema)
for t in ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']:
	con.execute(f"COPY {t} FROM '{datadir}/{t}.tbl'")
con.commit()

def query(n):
	local_con = con.cursor()
	queries = pathlib.Path(f'{datadir}/queries{n}.sql').read_text().split(";")
	it = 0
	while loop:
		query_idx = 0
		for q in queries:
			start = time.time()
			local_con.execute(q)
			duration = round(time.time() - start, 3)
			print(f"query\t{n}\t{it}\t{query_idx}\t{duration}")
			it = it + 1
			query_idx = query_idx + 1


for i in range(16):
	t = threading.Thread(target=query, args=[i])
	t.start()

# run refresh
for n in range(1, refresh_n+1):
	start = time.time()
	con.begin()
	delete = f"{datadir}/delete.{n}"
	orders = f"{datadir}/orders.tbl.u{n}"
	lineitem = f"{datadir}/lineitem.tbl.u{n}"
	con.execute(f"CREATE OR REPLACE TEMPORARY TABLE deletes AS SELECT column0 FROM read_csv('{delete}')")
	con.execute("DELETE FROM orders WHERE o_orderkey IN (FROM deletes)")
	con.execute("DELETE FROM lineitem WHERE l_orderkey IN (FROM deletes)")
	con.execute(f"COPY lineitem FROM '{lineitem}'")
	con.execute(f"COPY orders FROM '{orders}'")
	con.commit()
	duration = round(time.time() - start, 3)
	print(f"refresh\t{n}\t{duration}")

loop = False