# DuckDB TPC-H implementation

Install the required packages from `requirements.txt`.

Set the desired scale factor, e.g.:

```bash
export SF=100
```

To generate the data, run:

```bash
./generate.sh
```

Run the benchmark as follows:

```bash
python benchmark.py
```
