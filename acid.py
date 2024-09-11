# 2024-09-10, hannes@duckdblabs.com

import threading
import duckdb
import pathlib
import tempfile
import time
import functools
import operator
import os
import shutil
import datetime
import random
import math
import datetime

scale_factor = 1 # always 1

datadir = f'gen/sf{scale_factor}'
template_db_file = f'{datadir}/tpch_template.duckdb'
db_file = f'{datadir}/acid-tpch.duckdb'

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

# need to create the history table
con.execute("CREATE TABLE IF NOT EXISTS history (h_p_key bigint, h_s_key bigint, h_o_key bigint, h_l_key bigint, h_delta integer, h_date_t datetime)")

con.execute("CREATE MACRO IF NOT EXISTS trunc(n, p) as (floor(n*(10^p)))/(10^p)")

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

	return {'rprice':rprice, 'quantity':quantity, 'tax':tax, 'disc':disc, 'extprice':extprice, 'ototal':ototal, 'new_extprice' : new_extprice, 'new_quantity' : new_quantity, 'new_ototal' : new_ototal, 'delta' : delta, 'date' : date}


def acid_query(con, o_key):
	res = con.execute(f"SELECT SUM(trunc(trunc(L_EXTENDEDPRICE * (1 - L_DISCOUNT),2) * (1 + L_TAX),2)) FROM LINEITEM WHERE L_ORDERKEY = {o_key}").fetchone()[0]
	return res



def get_state(con, l_key, o_key):
	ototal = con.execute(f"SELECT o_totalprice FROM orders where o_orderkey={o_key}").fetchone()[0]
	extprice, quantity, p_key, s_key = con.execute(f"SELECT l_extendedprice, l_quantity,  l_partkey, l_suppkey FROM lineitem WHERE l_orderkey={o_key} AND l_linenumber={l_key}").fetchone()
	return {'l_key' : l_key, 'o_key':o_key, 'ototal': ototal, 'extprice' : extprice, 'quantity': quantity, 'p_key': p_key, 's_key' : s_key}


def verify_state(con, previous_state, transaction_result, updated):
	new_state = get_state(con, previous_state['l_key'], previous_state['o_key'])
	if updated:
		assert new_state != previous_state
		assert new_state['ototal'] == transaction_result['new_ototal']
		assert new_state['extprice'] == transaction_result['new_extprice']
		assert new_state['quantity'] == transaction_result['new_quantity']
		h_delta, h_date_t = con.execute(f"SELECT h_delta, h_date_t::VARCHAR FROM history WHERE h_p_key={previous_state['p_key']} AND h_s_key={previous_state['s_key']} AND h_o_key={previous_state['o_key']} AND h_l_key={previous_state['l_key']}").fetchone()
		assert h_delta == transaction_result['delta'] and h_date_t == transaction_result['date']
	else:
		assert new_state == previous_state
		history_count = con.execute(f"SELECT count(*) FROM history WHERE h_p_key={previous_state['p_key']} AND h_s_key={previous_state['s_key']} AND h_o_key={previous_state['o_key']} AND h_l_key={previous_state['l_key']}").fetchone()[0]
		assert history_count == 0

# 3.2.2.1 Perform the ACID Transaction (see Clause 3.1.5) for a randomly selected set of input data and verify that the appropriate rows have been changed in the ORDERS, LINEITEM, and HISTORY tables.
def acid_3_2_2_1():
	delta = random_delta()
	date = get_timestamp()
	o_key = random_order(con)
	l_key = l_key_for_o_key(con, o_key)
	previous_state = get_state(con, l_key, o_key)
	transaction_result = acid_transaction(con, o_key, l_key, delta, date)
	con.commit()
	verify_state(con, previous_state, transaction_result, True)

acid_3_2_2_1()

# 3.2.2.2 Perform the ACID Transaction for a randomly selected set of input data, substituting a ROLLBACK of the transaction for the COMMIT of the transaction. Verify that the appropriate rows have NOT been changed in the ORDERS, LINEITEM, and HISTORY tables.
def acid_3_2_2_2():
	delta = random_delta()
	date = get_timestamp()
	o_key = random_order(con)
	l_key = l_key_for_o_key(con, o_key)
	previous_state = get_state(con, l_key, o_key)
	transaction_result = acid_transaction(con, o_key, l_key, delta, date)
	con.rollback()
	verify_state(con, previous_state, transaction_result, False)

acid_3_2_2_2()


# consistency tests
def acid_3_3():
	def check_consistency_condition(con):
		# spec says at least ten orders but we can just do all because we're duckdb
		assert con.execute("SELECT bool_and(condition_holds) is_consistent from (SELECT o_totalprice, sum(trunc(trunc(l_extendedprice *(1 - l_discount),2) * (1+l_tax),2)) totalprice_derived, abs(o_totalprice - totalprice_derived) < 1 condition_holds FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY l_orderkey, o_totalprice)").fetchone()[0]

	check_consistency_condition(con)
	consistency_threads = 10 # because why not

	def consistency_check_thread(con0):
		con = con0.cursor()

		for i in range(100):
			delta = random_delta()
			date = get_timestamp()
			o_key = random_order(con)
			l_key = l_key_for_o_key(con, o_key)
			acid_transaction(con, o_key, l_key, delta, date)
			con.commit()

		con.close()

	threads = []

	for i in range(consistency_threads):
		t = threading.Thread(target=consistency_check_thread, args=[con])
		t.start()
		threads.append(t)

	for t in threads:
		t.join()
			
	check_consistency_condition(con)

acid_3_3()

# 3.4.2.1 Isolation Test 1
def acid_3_4_2_1():
	delta = random_delta()
	date = get_timestamp()
	o_key = random_order(con)
	l_key = l_key_for_o_key(con, o_key)
	initial_result = acid_query(con, o_key)
	previous_state = get_state(con, l_key, o_key)

	con1 = con.cursor()
	con2 = con.cursor()

	# 1. Start an ACID Transaction Txn1 for a randomly selected O_KEY, L_KEY, and DELTA.

	# 2. Suspend Txn1 immediately prior to COMMIT (no commit)
	transaction_result = acid_transaction(con1, o_key, l_key, delta, date)
	# 3. Start an ACID Query Txn2 for the same O_KEY as in Step 1. 
	con2.begin()
	con2_result = acid_query(con2, o_key)
	# 4. Verify that Txn2 does not see Txn1's updates.
	assert initial_result == con2_result
	# 5. Allow Txn1 to complete.
	con1.commit()
	# 6. Txn2 should now have completed.
	con2.commit()

	con1.close()
	con2.close()

	# final check, the changes are now visible
	final_result = acid_query(con, o_key)
	assert final_result != initial_result
	# additional verification that the update happened
	verify_state(con, previous_state, transaction_result, True)

acid_3_4_2_1()


# 3.4.2.2 Isolation Test 2
def acid_3_4_2_2():
	delta = random_delta()
	date = get_timestamp()
	o_key = random_order(con)
	l_key = l_key_for_o_key(con, o_key)
	initial_result = acid_query(con, o_key)
	previous_state = get_state(con, l_key, o_key)

	con1 = con.cursor()
	con2 = con.cursor()

	# 1. Start an ACID Transaction Txn1 for a randomly selected O_KEY, L_KEY, and DELTA.

	# 2. Suspend Txn1 immediately prior to COMMIT (no commit)
	transaction_result = acid_transaction(con1, o_key, l_key, delta, date)
	# 3. Start an ACID Query Txn2 for the same O_KEY as in Step 1. 
	con2.begin()
	con2_result = acid_query(con2, o_key)
	# 4. Verify that Txn2 does not see Txn1's updates.
	assert initial_result == con2_result
	# 5. Force Txn1 to rollback.
	con1.rollback()
	# 6. Txn2 should now have completed.
	con2.commit()

	con1.close()
	con2.close()

	# final check, the changes are now NOT visible
	final_result = acid_query(con, o_key)
	assert final_result == initial_result
	# additional verification that the update happened
	verify_state(con, previous_state, transaction_result, False)

acid_3_4_2_2()


# 3.4.2.3 Isolation Test 3
def acid_3_4_2_3():
	delta1 = random_delta()
	delta2 = random_delta()
	date = get_timestamp()
	o_key = random_order(con)
	l_key = l_key_for_o_key(con, o_key)
	initial_result = acid_query(con, o_key)
	previous_state = get_state(con, l_key, o_key)

	con1 = con.cursor()
	con2 = con.cursor()

	# 1. Start an ACID Transaction Txn1 for a randomly selected O_KEY, L_KEY, and DELTA.

	# 2. Suspend Txn1 immediately prior to COMMIT (no commit)
	transaction_result = acid_transaction(con1, o_key, l_key, delta1, date)

	# 3. Start another ACID Transaction Txn2 for the same O_KEY, L_KEY and for a randomly selected DELTA2.
	txn2_was_aborted = False
	try: 
		acid_transaction(con2, o_key, l_key, delta2, date)
	# verify that the second transacation was aborted, as in e.g. https://www.tpc.org/results/fdr/tpch/hpe~tpch~1000~hpe_dl325_gen10~fdr~2021-04-02~v02.pdf
	except duckdb.duckdb.TransactionException:
		txn2_was_aborted = True
		pass
	assert txn2_was_aborted
	con1.commit()
	con2.rollback()

	con1.close()
	con2.close()

	# final check, the changes are now visible
	final_result = acid_query(con, o_key)
	assert final_result != initial_result
	# additional verification that the update happened
	verify_state(con, previous_state, transaction_result, True)


acid_3_4_2_3()

# 3.4.2.4 Isolation Test 4
def acid_3_4_2_4():
	delta1 = random_delta()
	delta2 = random_delta()
	date = get_timestamp()
	o_key = random_order(con)
	l_key = l_key_for_o_key(con, o_key)
	initial_result = acid_query(con, o_key)
	previous_state = get_state(con, l_key, o_key)

	con1 = con.cursor()
	con2 = con.cursor()

	# 1. Start an ACID Transaction Txn1 for a randomly selected O_KEY, L_KEY, and DELTA.

	# 2. Suspend Txn1 immediately prior to COMMIT (no commit)
	transaction_result = acid_transaction(con1, o_key, l_key, delta1, date)

	# 3. Start another ACID Transaction Txn2 for the same O_KEY, L_KEY and for a randomly selected DELTA2.
	txn2_was_aborted = False
	try: 
		acid_transaction(con2, o_key, l_key, delta2, date)
	# verify that the second transacation was aborted, as in e.g. https://www.tpc.org/results/fdr/tpch/hpe~tpch~1000~hpe_dl325_gen10~fdr~2021-04-02~v02.pdf
	except duckdb.duckdb.TransactionException:
		txn2_was_aborted = True
		pass
	assert txn2_was_aborted
	con1.rollback()
	con2.rollback()

	con1.close()
	con2.close()

	# final check, the changes are now NOT visible
	final_result = acid_query(con, o_key)
	assert final_result == initial_result
	# additional verification that the update happened
	verify_state(con, previous_state, transaction_result, False)


acid_3_4_2_4()



# 3.4.2.5 Isolation Test 5
def acid_3_4_2_5():
	delta = random_delta()
	date = get_timestamp()
	o_key = random_order(con)
	l_key = l_key_for_o_key(con, o_key)
	previous_state = get_state(con, l_key, o_key)

	con1 = con.cursor()
	con2 = con.cursor()

	# 1. Start an ACID Transaction Txn1 for a randomly selected O_KEY, L_KEY, and DELTA.
	# 2. Suspend Txn1 immediately prior to COMMIT (no commit)
	transaction_result = acid_transaction(con1, o_key, l_key, delta, date)

	# 3. Start a transaction Txn2 that does the following:
	con2.begin()

	# 4. Select random values of PS_PARTKEY and PS_SUPPKEY. Return all columns of the PARTSUPP table for which PS_PARTKEY and PS_SUPPKEY are equal to the selected values.

	random_ps_partkey = con.execute("SELECT ps_partkey FROM partsupp USING SAMPLE 1").fetchone()[0]
	random_ps_suppkey = con.execute("SELECT ps_suppkey FROM partsupp USING SAMPLE 1").fetchone()[0]
	partsupp_subset = con.execute(f"SELECT * FROM partsupp WHERE ps_partkey = {random_ps_partkey} OR ps_suppkey = {random_ps_suppkey}").fetchall()

	# 5. Verify that Txn2 completes.
	con2.commit()

	# 6. Allow Txn1 to complete. Verify that the appropriate rows in the ORDERS, LINEITEM and HISTORY tables
	con1.commit()

	con1.close()
	con2.close()

	verify_state(con, previous_state, transaction_result, True)



acid_3_4_2_5()

# 3.4.2.6 Isolation Test 6
# TODO

print('✅')
