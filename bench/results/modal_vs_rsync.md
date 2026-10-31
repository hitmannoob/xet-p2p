{"platform": "Linux x86_64", "reps": 5}
BASELINE
| Repetition | TCP connect RTT | raw upload |
|---|---:|---:|
| 1 | 141 ms | probe failed |
| 2 | 146 ms | probe failed |
| 3 | 148 ms | probe failed |
| 4 | 142 ms | probe failed |
| 5 | 147 ms | probe failed |

BYTES_SENT
| Scenario | rsync -a | rsync -az | xet-p2p | xet-p2p --keep-agent |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 8193 KiB | 8196 KiB | 8228 KiB [8228 KiB–8229 KiB] | 8228 KiB [8228 KiB–8229 KiB] |
| warm, 4 KiB inserted mid 8 MiB file | 18 KiB | 21 KiB | 106 KiB [42 KiB–172 KiB] | 106 KiB [42 KiB–172 KiB] |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 1039 KiB | 1043 KiB | 1281 KiB [1165 KiB–1387 KiB] | 1281 KiB [1165 KiB–1387 KiB] |
| cold, 8 MiB compressible log | 8193 KiB | 1661 KiB [1659 KiB–1661 KiB] | 3149 KiB [3143 KiB–3160 KiB] | 3149 KiB [3143 KiB–3161 KiB] |
| warm, 256 KiB appended to 8 MiB log | 269 KiB | 67 KiB [66 KiB–67 KiB] | 124 KiB [108 KiB–146 KiB] | 124 KiB [108 KiB–146 KiB] |
| cold, 500 small files (1-16 KiB) | 4294 KiB [4201 KiB–4400 KiB] | 1117 KiB [1098 KiB–1143 KiB] | 1861 KiB [1825 KiB–1904 KiB] | 1861 KiB [1825 KiB–1904 KiB] |
| warm, 10 of 500 small files edited | 24 KiB [23 KiB–25 KiB] | 21 KiB [21 KiB–22 KiB] | 77 KiB [65 KiB–84 KiB] | 77 KiB [65 KiB–84 KiB] |
| warm, 500 small files unchanged | 19 KiB | 19 KiB | 39 KiB [39 KiB–39 KiB] | 39 KiB [39 KiB–39 KiB] |

BYTES_BOTH
| Scenario | rsync -a | rsync -az | xet-p2p | xet-p2p --keep-agent |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 8193 KiB | 8196 KiB | 8231 KiB [8231 KiB–8231 KiB] | 8231 KiB [8231 KiB–8231 KiB] |
| warm, 4 KiB inserted mid 8 MiB file | 35 KiB | 38 KiB | 109 KiB [44 KiB–174 KiB] | 109 KiB [44 KiB–174 KiB] |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 1063 KiB | 1068 KiB | 1283 KiB [1168 KiB–1389 KiB] | 1283 KiB [1168 KiB–1389 KiB] |
| cold, 8 MiB compressible log | 8193 KiB | 1661 KiB [1660 KiB–1661 KiB] | 3152 KiB [3145 KiB–3163 KiB] | 3152 KiB [3145 KiB–3163 KiB] |
| warm, 256 KiB appended to 8 MiB log | 286 KiB | 84 KiB [84 KiB–84 KiB] | 127 KiB [110 KiB–149 KiB] | 127 KiB [110 KiB–149 KiB] |
| cold, 500 small files (1-16 KiB) | 4305 KiB [4212 KiB–4411 KiB] | 1128 KiB [1109 KiB–1154 KiB] | 1936 KiB [1900 KiB–1979 KiB] | 1936 KiB [1900 KiB–1979 KiB] |
| warm, 10 of 500 small files edited | 25 KiB [24 KiB–26 KiB] | 22 KiB [22 KiB–23 KiB] | 81 KiB [69 KiB–88 KiB] | 81 KiB [69 KiB–88 KiB] |
| warm, 500 small files unchanged | 19 KiB | 19 KiB | 40 KiB [40 KiB–40 KiB] | 40 KiB [40 KiB–40 KiB] |

WALL
| Scenario | rsync -a | rsync -az | xet-p2p | xet-p2p --keep-agent |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 3.8 [3.6–11.3] | 3.7 [3.4–3.8] | 7.7 [6.9–12.0] | 4.2 [4.0–6.4] |
| warm, 4 KiB inserted mid 8 MiB file | 2.2 [2.1–2.2] | 2.1 [2.1–2.2] | 5.8 [5.4–6.0] | 2.5 [2.3–2.8] |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 3.0 [2.8–3.3] | 3.0 [2.8–3.1] | 6.4 [6.1–7.0] | 3.3 [3.0–3.5] |
| cold, 8 MiB compressible log | 3.9 [3.5–4.0] | 3.1 [2.9–3.3] | 6.6 [6.4–7.1] | 3.6 [3.6–3.8] |
| warm, 256 KiB appended to 8 MiB log | 2.6 [2.5–2.6] | 2.3 [2.2–2.4] | 5.8 [5.4–6.2] | 2.6 [2.4–2.9] |
| cold, 500 small files (1-16 KiB) | 3.0 [2.8–3.3] | 2.7 [2.7–3.1] | 7.6 [7.1–13.9] | 4.0 [3.5–4.1] |
| warm, 10 of 500 small files edited | 2.1 [2.0–2.2] | 2.1 [2.0–2.2] | 6.0 [5.6–6.5] | 2.7 [2.6–3.2] |
| warm, 500 small files unchanged | 1.8 [1.7–1.9] | 1.8 [1.7–1.8] | 5.3 [5.2–5.7] | 2.1 [2.0–2.3] |

XET_SPLIT
| Scenario | xet-p2p transfer | xet-p2p bootstrap | --keep-agent transfer | --keep-agent bootstrap |
|---|---:|---:|---:|---:|
| cold, 8 MiB random file | 2.9 [2.8–3.4] | 3.7 [3.4–7.7] | 3.1 [3.0–3.4] | 0.0 [0.0–3.3] |
| warm, 4 KiB inserted mid 8 MiB file | 1.6 [1.3–1.9] | 3.5 [3.4–3.7] | 1.6 [1.3–1.7] | 0.0 |
| warm, checkpoint: 2 of 32 tensors rewritten (16 MiB) | 2.3 [2.0–2.3] | 3.5 [3.4–4.1] | 2.3 [2.0–2.5] | 0.0 |
| cold, 8 MiB compressible log | 2.5 [2.5–2.8] | 3.4 [3.3–3.9] | 2.6 [2.5–2.8] | 0.0 |
| warm, 256 KiB appended to 8 MiB log | 1.6 [1.4–1.7] | 3.6 [3.3–3.9] | 1.6 [1.5–1.9] | 0.0 |
| cold, 500 small files (1-16 KiB) | 3.0 [2.9–9.4] | 3.7 [3.6–4.0] | 3.0 [2.6–3.1] | 0.0 |
| warm, 10 of 500 small files edited | 1.7 [1.6–2.1] | 3.6 [3.4–3.7] | 1.8 [1.6–2.2] | 0.0 |
| warm, 500 small files unchanged | 1.1 [1.0–1.3] | 3.6 [3.6–3.8] | 1.1 [1.0–1.2] | 0.0 |

