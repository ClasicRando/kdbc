## Machine

- CPU: AMD Ryzen 5 5600G with Radeon Graphics
- Memory: 32GB DDR4 3600 MT/s
- Postgres Database: docker.io/postgis/postgis

## Results

| Benchmark                                       | Mode | Cnt | Score      | Error      | Units | % difference vs JDBC |
|-------------------------------------------------|------|-----|------------|------------|-------|----------------------|
| PgBenchmarkAsyncCopyKdbc.copyInSource           | avgt | 40  | 105147.312 | ± 2626.505 | us/op | 1.10                 |
| PgBenchmarkAsyncCopyKdbc.copyInStream           | avgt | 40  | 104728.308 | ± 2659.264 | us/op | 0.70                 |
| PgBenchmarkAsyncCopyKdbc.copyOutSink            | avgt | 40  | 21589.895  | ±  546.753 | us/op | 6.27                 |
| PgBenchmarkAsyncCopyKdbc.copyOutStream          | avgt | 40  | 26039.432  | ±  322.212 | us/op | 27.17                |
| PgBenchmarkAsyncMultiJdbc.querySingleRow        | avgt | 40  | 1601.057   | ±   13.369 | us/op | N/A                  |
| PgBenchmarkAsyncMultiKdbc.querySingleRow        | avgt | 40  | 1552.295   | ±    9.725 | us/op | -3.05                |
| PgBenchmarkAsyncSingleJdbc.queryMultipleRows    | avgt | 40  | 87.995     | ±    1.209 | us/op | N/A                  |
| PgBenchmarkAsyncSingleJdbc.querySingleRow       | avgt | 40  | 60.667     | ±    1.016 | us/op | N/A                  |
| PgBenchmarkAsyncSingleKdbc.queryMultipleRows    | avgt | 40  | 97.166     | ±    1.238 | us/op | 10.42                |
| PgBenchmarkAsyncSingleKdbc.querySingleRow       | avgt | 40  | 58.485     | ±    1.306 | us/op | -3.60                |
| PgBenchmarkBlockingCopyJdbc.copyIn              | avgt | 40  | 104005.027 | ± 2765.355 | us/op | N/A                  |
| PgBenchmarkBlockingCopyJdbc.copyOut             | avgt | 40  | 20316.418  | ±  437.487 | us/op | N/A                  |
| PgBenchmarkBlockingCopyKdbc.copyInSource        | avgt | 40  | 102305.251 | ± 2798.997 | us/op | -1.63                |
| PgBenchmarkBlockingCopyKdbc.copyInStream        | avgt | 40  | 102484.494 | ± 2858.945 | us/op | -1.46                |
| PgBenchmarkBlockingCopyKdbc.copyOutSink         | avgt | 40  | 19953.363  | ±  347.143 | us/op | -1.82                |
| PgBenchmarkBlockingCopyKdbc.copyOutStream       | avgt | 40  | 20825.255  | ±  429.950 | us/op | 2.50                 |
| PgBenchmarkBlockingMultiJdbc.querySingleRow     | avgt | 40  | 1446.570   | ±    4.228 | us/op | N/A                  |
| PgBenchmarkBlockingMultiKdbc.querySingleRow     | avgt | 40  | 1244.352   | ±    1.265 | us/op | -13.98               |
| PgBenchmarkBlockingSingleJdbc.queryMultipleRows | avgt | 40  | 73.162     | ±    0.618 | us/op | N/A                  |
| PgBenchmarkBlockingSingleJdbc.querySingleRow    | avgt | 40  | 47.948     | ±    0.753 | us/op | N/A                  |
| PgBenchmarkBlockingSingleKdbc.queryMultipleRows | avgt | 40  | 71.661     | ±    1.023 | us/op | -2.05                |
| PgBenchmarkBlockingSingleKdbc.querySingleRow    | avgt | 40  | 47.371     | ±    0.717 | us/op | -1.20                |