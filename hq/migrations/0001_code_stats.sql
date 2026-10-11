-- GET /stats 原查询每次全表扫描 300 万行；改读汇总表，插入时增量维护
CREATE TABLE IF NOT EXISTS code_stats (
  code TEXT PRIMARY KEY,
  record_count INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO code_stats (code, record_count)
SELECT code, COUNT(*) FROM minute_klines GROUP BY code;
