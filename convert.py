import duckdb
import glob

con = duckdb.connect('fuu.db')
con.execute('SET preserve_insertion_order = false')

for f in (glob.glob("*.tbl.u*") + glob.glob("delete.*")):
	print(f)
	rel = con.from_query(f"FROM read_csv('{f}', sep='|')")
	rel2 = rel.project(','.join(rel.columns[:-1])).to_parquet(f"{f}.parquet")
