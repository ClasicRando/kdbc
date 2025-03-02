## Machine

- CPU: AMD Ryzen 5 5600G with Radeon Graphics
- Memory: 32GB DDR4 3600 MT/s
- Postgres Database: docker.io/postgis/postgis

## Results
Difference is `(KDBC - JDBC)/JDBC * 100`

| Benchmark                                          | Mode | Cnt | Score ± Error         | Units | % difference vs JDBC             |
|----------------------------------------------------|------|-----|-----------------------|-------|----------------------------------|
| MySqlBenchmarkAsyncMultiJdbc.querySingleRow        | avgt | 40  | 2588.814 ± 8.564      | us/op | N/A                              |
| MySqlBenchmarkAsyncMultiKdbc.querySingleRow        | avgt | 40  | 2544.995 ± 16.722     | us/op | -1.69 (async) 1.37 (blocking)    |
| MySqlBenchmarkAsyncSingleJdbc.queryMultipleRows    | avgt | 40  | 177.407 ± 1.774       | us/op | N/A                              |
| MySqlBenchmarkAsyncSingleJdbc.querySingleRow       | avgt | 40  | 123.892 ± 1.747       | us/op | N/A                              |
| MySqlBenchmarkAsyncSingleKdbc.queryMultipleRows    | avgt | 40  | 136.299 ±  0.304      | us/op | −23.17 (async) -16.51 (blocking) |
| MySqlBenchmarkAsyncSingleKdbc.querySingleRow       | avgt | 40  | 100.974 ±  1.170      | us/op | −18.50 (async) -7.52 (blocking)  |
| MySqlBenchmarkBlockingMultiJdbc.querySingleRow     | avgt | 40  | 2510.550 ± 2.173      | us/op | N/A                              |
| MySqlBenchmarkBlockingSingleJdbc.queryMultipleRows | avgt | 40  | 163.245 ± 1.754       | us/op | N/A                              |
| MySqlBenchmarkBlockingSingleJdbc.querySingleRow    | avgt | 40  | 109.182 ± 1.773       | us/op | N/A                              |
| PgBenchmarkAsyncCopyKdbc.copyInSource              | avgt | 40  | 105982.126 ± 2828.646 | us/op | ~0 (blocking)                    |
| PgBenchmarkAsyncCopyKdbc.copyInStream              | avgt | 40  | 107876.161 ± 2030.854 | us/op | 1.75 (blocking)                  |
| PgBenchmarkAsyncCopyKdbc.copyOutSink               | avgt | 40  | 23360.839 ± 495.592   | us/op | 15.65 (blocking)                 |
| PgBenchmarkAsyncCopyKdbc.copyOutStream             | avgt | 40  | 23620.399 ± 541.365   | us/op | 16.93 (blocking)                 |
| PgBenchmarkAsyncMultiJdbc.querySingleRow           | avgt | 40  | 1563.172 ± 11.539     | us/op | N/A                              |
| PgBenchmarkAsyncMultiKdbc.querySingleRow           | avgt | 40  | 1807.106 ± 5.080      | us/op | 15.61 (async) 26.65 (blocking)   |
| PgBenchmarkAsyncSingleJdbc.queryMultipleRows       | avgt | 40  | 83.479 ± 0.845        | us/op | N/A                              |
| PgBenchmarkAsyncSingleJdbc.querySingleRow          | avgt | 40  | 57.973 ± 1.076        | us/op | N/A                              |
| PgBenchmarkAsyncSingleKdbc.queryMultipleRows       | avgt | 40  | 90.329 ± 0.533        | us/op | 8.20 (async) 28.18 (blocking)    |
| PgBenchmarkAsyncSingleKdbc.querySingleRow          | avgt | 40  | 69.828 ± 1.123        | us/op | 20.45 (async) 45.93 (blocking)   |
| PgBenchmarkBlockingCopyJdbc.copyIn                 | avgt | 40  | 106018.518 ± 2905.935 | us/op | N/A                              |
| PgBenchmarkBlockingCopyJdbc.copyOut                | avgt | 40  | 20200.309 ± 438.845   | us/op | N/A                              |
| PgBenchmarkBlockingMultiJdbc.querySingleRow        | avgt | 40  | 1426.848 ± 3.417      | us/op | N/A                              |
| PgBenchmarkBlockingSingleJdbc.queryMultipleRows    | avgt | 40  | 70.470 ± 0.741        | us/op | N/A                              |
| PgBenchmarkBlockingSingleJdbc.querySingleRow       | avgt | 40  | 47.850 ± 1.047        | us/op | N/A                              |
