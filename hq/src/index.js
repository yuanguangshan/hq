//可连接github进行构建,并在github中进行修改
const code_to_mkt = {
	"a": "dce",
	"ag": "shfe",
	"al": "shfe",
	"ao": "shfe",
	"ap": "czce",
	"au": "shfe",
	"b": "dce",
	"bb": "dce",
	"bc": "ine",
	"br": "shfe",
	"bu": "shfe",
	"c": "dce",
	"cf": "czce",
	"cj": "czce",
	"cs": "dce",
	"cu": "shfe",
	"cy": "czce",
	"eb": "dce",
	"ec": "ine",
	"eg": "dce",
	"fb": "dce",
	"fg": "czce",
	"fu": "shfe",
	"hc": "shfe",
	"i": "dce",
	"ic": "cffex",
	"if": "cffex",
	"ih": "cffex",
	"im": "cffex",
	"j": "dce",
	"jd": "dce",
	"jm": "dce",
	"jr": "czce",
	"l": "dce",
	"lc": "gfex",
	"lg": "dce",
	"lh": "dce",
	"lr": "czce",
	"lu": "ine",
	"m": "dce",
	"ma": "czce",
	"ni": "shfe",
	"nr": "ine",
	"oi": "czce",
	"p": "dce",
	"pb": "shfe",
	"pf": "czce",
	"pg": "dce",
	"pk": "czce",
	"pm": "czce",
	"pp": "dce",
	"pr": "czce",
	"ps": "gfex",
	"px": "czce",
	"rb": "shfe",
	"ri": "czce",
	"rm": "czce",
	"rr": "dce",
	"rs": "czce",
	"ru": "shfe",
	"sa": "czce",
	"sc": "ine",
	"sf": "czce",
	"sh": "czce",
	"si": "gfex",
	"sm": "czce",
	"sn": "shfe",
	"sp": "shfe",
	"sr": "czce",
	"ss": "shfe",
	"t": "cffex",
	"ta": "czce",
	"tf": "cffex",
	"tl": "cffex",
	"ts": "cffex",
	"ur": "czce",
	"v": "dce",
	"wh": "czce",
	"wr": "shfe",
	"y": "dce",
	"zc": "czce",
	"zn": "shfe",
  }; 
  function getMarket(code) {
	// 移除数字
	let code2 = code.replace(/\d/g, '');
	// 移除月份标识
	let code3 = code2.replace(/[mM]$/, '');
	// 郑商所的期货代码通常是大写的，尝试大写版本
	let market = code_to_mkt[code3] || code_to_mkt[code3.toUpperCase()];
	return market;
  }
  
  // 带重试机制的fetchKline函数
async function fetchKline(code, retryCount = 0) {
	const mkt = getMarket(code);
	if (!mkt) return null;
	
	const url = `http://futsse.eastmoney.com/static/rtdata/${mkt}_${code}`;
	
	try {
		// 对tam合约特殊处理，增加额外延时
		if (code === 'tam' && retryCount === 0) {
			await delay(5000); // 首次请求前额外等待
		}
		
		console.log(`尝试获取 ${code} 数据，第 ${retryCount + 1} 次尝试`);
		const resp = await fetch(url, {
			timeout: 10000, // 设置超时时间
			headers: {
				'User-Agent': 'Mozilla/5.0 (Cloudflare Worker)',
				'Accept': 'application/json'
			}
		});
		
		if (!resp.ok) {
			throw new Error(`请求失败，状态码: ${resp.status}`);
		}
		
		const data = await resp.json();
		if (!data?.data?.trends) {
			throw new Error('数据格式不正确');
		}
		
		const trends = data.data.trends;
		const columns = ['timestamp', 'open', 'close', 'high', 'low', 'volume', 'amount', 'average'];
		return trends.map(row => {
			const arr = row.split(',');
			let obj = {};
			for (let i = 0; i < columns.length; ++i) obj[columns[i]] = arr[i];
			return obj;
		});
	} catch (error) {
		console.error(`获取 ${code} 数据失败: ${error.message}`);
		
		// 实现重试逻辑
		if (retryCount < MAX_RETRIES) {
			// 使用指数退避策略计算重试延迟
			const retryDelay = BASE_RETRY_DELAY * Math.pow(2, retryCount);
			console.log(`将在 ${retryDelay}ms 后重试获取 ${code} 数据...`);
			await delay(retryDelay);
			return fetchKline(code, retryCount + 1);
		} else {
			console.error(`获取 ${code} 数据失败，已达到最大重试次数`);
			return null;
		}
	}
}
  
  // 定义需要获取的合约代码
// let codes = ["imm", "tam", "agm", "rbm", "pm"]
let codes = ["imm", "aum", "ifm", "icm", "cum", "im", "ihm", "rbm", "agm", "ym", "tm", "hcm", "tlm", "cfm", "jmm", "pm", "oim", "tam"]
// 完整代码列表，暂时注释掉以减少API请求数量
// let codes = Array.from(
// 	new Set(Object.keys(code_to_mkt).map(code => code.toLowerCase() + 'm'))
// );

// 最大重试次数
const MAX_RETRIES = 3;
// 重试延迟基础时间（毫秒）
const BASE_RETRY_DELAY = 15000;

// 新增常量
const CHUNK_SIZE_REGULAR = 10; // 每批处理的常规代码数量
const REQUEST_INTERVAL_REGULAR_CODE_IN_CHUNK = 10000; // 常规代码块中每个代码的请求间隔
const INTER_CHUNK_PROCESSING_DELAY = 20000; // 常规代码块之间的处理延迟
const SENSITIVE_CODE_REQUEST_INTERVAL = 20000; // 敏感代码请求间隔
const SENSITIVE_CODE_COOLDOWN_TIME = 40000; // 敏感代码处理后的冷却时间
const REGULAR_CODE_SINGLE_COOLDOWN_TIME = 5000; // 单个常规代码处理后的冷却时间 (用于 processSingleCodeWithRetries)
const DB_OPERATION_BATCH_SIZE = 50; // 数据库批量操作的行数
  
  function getBeijingTime() {
	let now = new Date(Date.now() + 8 * 3600 * 1000);
	return now.toISOString().slice(0, 19).replace('T', ' ');
  }
  
  // 添加延时函数，返回一个Promise，在指定的毫秒数后resolve
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// 批量插入数据函数
async function batchInsertData(env, rows, code, update_time) {
  if (rows.length === 0) return 0;
  
  // 使用事务进行批量插入，减少API调用次数
  const stmt = env.DB.prepare(
    "INSERT INTO minute_klines (timestamp, code, open, close, high, low, volume, amount, average, update_time) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  );
  
  let inserted = 0;
  const BATCH_SIZE = 20; // 每批处理的记录数
  
  // 按批次处理数据
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    try {
      // 开始事务
      await env.DB.batch(batch.map(row => {
        return stmt.bind(
          row.timestamp, code,
          row.open, row.close, row.high, row.low, row.volume, row.amount, row.average,
          update_time
        );
      }));
      inserted += batch.length;
      // 每批次之间添加短暂延时，避免过度消耗资源
      if (i + BATCH_SIZE < rows.length) {
        await delay(100);
      }
    } catch (e) {
      console.error(`批量插入 ${code} 数据失败:`, e);
      // 如果批量插入失败，尝试单条插入
      for (const row of batch) {
        try {
          await stmt.bind(
            row.timestamp, code,
            row.open, row.close, row.high, row.low, row.volume, row.amount, row.average,
            update_time
          ).run();
          inserted += 1;
          await delay(50); // 单条插入之间添加短暂延时
        } catch (e) {
          console.error(`单条插入 ${code} 数据失败:`, e);
        }
      }
    }
  }
  
  return inserted;
}

// 检查是否是API请求限制错误
function isRateLimitError(error) {
  return error && (error.message || '').includes('Too many API requests');
}

// 新函数：批量插入来自多个代码的数据
async function batchInsertMultipleCodesData(env, rowsToInsert) {
  if (rowsToInsert.length === 0) return 0;

  const stmt = env.DB.prepare(
    "INSERT INTO minute_klines (timestamp, code, open, close, high, low, volume, amount, average, update_time) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
  );

  let inserted = 0;
  // DB_OPERATION_BATCH_SIZE 定义在文件顶部，例如 50

  for (let i = 0; i < rowsToInsert.length; i += DB_OPERATION_BATCH_SIZE) {
    const batch = rowsToInsert.slice(i, i + DB_OPERATION_BATCH_SIZE);
    try {
      await env.DB.batch(batch.map(row => {
        return stmt.bind(
          row.timestamp, row.code,
          row.open, row.close, row.high, row.low, row.volume, row.amount, row.average,
          row.update_time // update_time 应该在收集时加入到row对象中
        );
      }));
      inserted += batch.length;
      if (i + DB_OPERATION_BATCH_SIZE < rowsToInsert.length) {
        await delay(100); // 短暂延时避免过度消耗DB资源
      }
    } catch (e) {
      console.error(`数据库批量插入失败 (chunk):`, e);
      // 尝试单条插入
      for (const row of batch) {
        try {
          await stmt.bind(
            row.timestamp, row.code,
            row.open, row.close, row.high, row.low, row.volume, row.amount, row.average,
            row.update_time
          ).run();
          inserted += 1;
          await delay(50);
        } catch (singleE) {
          console.error(`数据库单条插入失败 (chunk fallback) for code ${row.code}:`, singleE);
        }
      }
    }
  }
  return inserted;
}

// 新函数：处理一个常规代码块 (获取数据并批量插入)
async function processRegularChunk(env, codeChunk, update_time) {
  let allRowsForChunkDatabase = [];
  let failedFetchingInChunk = [];

  console.log(`开始处理常规代码块: ${codeChunk.join(', ')}`);

  for (const code of codeChunk) {
    const mkt = getMarket(code);
    if (!mkt) {
      console.error(`未找到代码 ${code} 对应的交易所 (in chunk)`);
      failedFetchingInChunk.push(code);
      continue;
    }
    // REQUEST_INTERVAL_REGULAR_CODE_IN_CHUNK 定义在文件顶部
    await delay(REQUEST_INTERVAL_REGULAR_CODE_IN_CHUNK);
    console.log(`从块中获取 ${code} 数据...`);
    const klineRows = await fetchKline(code); // fetchKline 包含其自身的重试逻辑

    if (klineRows && klineRows.length > 0) {
      console.log(`成功获取 ${code} 的K线数据 ${klineRows.length} 条 (in chunk)`);
      klineRows.forEach(row => allRowsForChunkDatabase.push({ ...row, code: code, update_time: update_time }));
    } else {
      console.error(`获取 ${code} 的K线数据失败或为空 (in chunk)`);
      failedFetchingInChunk.push(code);
    }
  }

  let insertedInChunk = 0;
  if (allRowsForChunkDatabase.length > 0) {
    console.log(`准备为代码块 ${codeChunk.join(', ')} 插入 ${allRowsForChunkDatabase.length} 条数据`);
    insertedInChunk = await batchInsertMultipleCodesData(env, allRowsForChunkDatabase);
    console.log(`代码块 ${codeChunk.join(', ')} 成功插入 ${insertedInChunk} 条数据`);
  }

  return { inserted: insertedInChunk, failed: failedFetchingInChunk };
}

// 新函数：处理单个代码（通常是敏感代码），包含特定逻辑和重试
async function processSingleCodeWithRetries(env, code, update_time, isSensitive) {
  let inserted = 0;
  let failed = [];

  const mkt = getMarket(code);
  if (!mkt) {
    console.error(`未找到代码 ${code} 对应的交易所`);
    return { inserted: 0, failed: [code] };
  }

  const requestInterval = isSensitive ? SENSITIVE_CODE_REQUEST_INTERVAL : REQUEST_INTERVAL_REGULAR_CODE_IN_CHUNK; // 使用合适的间隔
  const cooldownTime = isSensitive ? SENSITIVE_CODE_COOLDOWN_TIME : REGULAR_CODE_SINGLE_COOLDOWN_TIME;

  try {
    await delay(requestInterval);
    console.log(`开始请求 ${code} 的K线数据... (single processing)`);
    const klineRows = await fetchKline(code);

    if (!klineRows) {
      console.error(`获取 ${code} 的K线数据失败 (single processing)`);
      failed.push(code);
      return { inserted: 0, failed: failed };
    }
    console.log(`成功获取 ${code} 的K线数据，共 ${klineRows.length} 条 (single processing)`);

    if (isSensitive && klineRows.length > 50) {
      console.log(`${code} 数据量较大 (${klineRows.length}条)，将分批处理以避免API限制`);
      const subBatchSize = 50;
      let totalInsertedForSensitive = 0;
      for (let i = 0; i < klineRows.length; i += subBatchSize) {
        const dataSubBatch = klineRows.slice(i, i + subBatchSize);
        console.log(`处理 ${code} 数据子批次 ${i / subBatchSize + 1}/${Math.ceil(klineRows.length / subBatchSize)}`);
        try {
          const batchInsertedCount = await batchInsertData(env, dataSubBatch, code, update_time); // 使用旧的 batchInsertData
          totalInsertedForSensitive += batchInsertedCount;
          console.log(`子批次插入成功: ${batchInsertedCount} 条`);
          if (i + subBatchSize < klineRows.length) {
            await delay(5000); // 子批次之间添加延时
          }
        } catch (e) {
          console.error(`处理 ${code} 的子批次失败:`, e);
          if (isRateLimitError(e)) {
            console.log('检测到API限制，增加等待时间...');
            await delay(30000);
            // 可以选择重试子批次，或标记失败并继续
          }
        }
      }
      inserted += totalInsertedForSensitive;
      console.log(`成功插入 ${code} (敏感，大批量) 的数据共 ${totalInsertedForSensitive} 条`);
    } else {
      const insertedCount = await batchInsertData(env, klineRows, code, update_time); // 使用旧的 batchInsertData
      inserted += insertedCount;
      console.log(`成功插入 ${code} 的数据 ${insertedCount} 条 (single processing)`);
    }

    await delay(cooldownTime);

  } catch (e) {
    console.error(`处理 ${code} 时发生错误 (single processing):`, e);
    if (isRateLimitError(e)) {
      console.log(`检测到API限制错误，将 ${code} 标记为失败`);
      failed.push(code);
      await delay(45000); // 遇到限制时等待较长时间
    } else {
      failed.push(code);
      await delay(5000); // 其他错误等待较短时间
    }
  }
  return { inserted, failed };
}

// 重写 processCodeList 函数
async function processCodeList(env, codeList, update_time) {
  const sensitiveCode = 'tam';
  const regularCodes = codeList.filter(code => code !== sensitiveCode);
  const hasSensitiveCode = codeList.includes(sensitiveCode);

  let results = { inserted: 0, failed: [] };

  // CHUNK_SIZE_REGULAR 和 INTER_CHUNK_PROCESSING_DELAY 定义在文件顶部
  console.log(`常规代码处理开始，每批 ${CHUNK_SIZE_REGULAR} 个，批间延迟 ${INTER_CHUNK_PROCESSING_DELAY / 1000}s`);
  for (let i = 0; i < regularCodes.length; i += CHUNK_SIZE_REGULAR) {
    const chunk = regularCodes.slice(i, i + CHUNK_SIZE_REGULAR);
    const chunkResult = await processRegularChunk(env, chunk, update_time);
    results.inserted += chunkResult.inserted;
    results.failed.push(...chunkResult.failed);

    if (i + CHUNK_SIZE_REGULAR < regularCodes.length) {
      console.log(`常规代码块处理完毕，等待 ${INTER_CHUNK_PROCESSING_DELAY / 1000} 秒后处理下一块...`);
      await delay(INTER_CHUNK_PROCESSING_DELAY);
    }
  }
  console.log('所有常规代码块处理完毕。');

  if (hasSensitiveCode) {
    console.log('等待30秒后处理敏感代码...');
    await delay(30000);
    console.log(`开始处理敏感代码: ${sensitiveCode}`);
    const sensitiveResult = await processSingleCodeWithRetries(env, sensitiveCode, update_time, true);
    results.inserted += sensitiveResult.inserted;
    results.failed.push(...sensitiveResult.failed);
    console.log(`敏感代码 ${sensitiveCode} 处理完毕。`);
  }

  return results;
}


async function fetchAndSaveAllCodes(env) {
	const codeList = codes;
	const update_time = getBeijingTime();
	console.log(`开始执行定时任务，当前北京时间: ${update_time}，处理代码列表: ${codeList.join(', ')}`);
  
	try {
	  await env.DB.exec(
		"CREATE TABLE IF NOT EXISTS minute_klines (" +
		"timestamp TEXT," +
		"code TEXT," +
		"open REAL," +
		"close REAL," +
		"high REAL," +
		"low REAL," +
		"volume REAL," +
		"amount REAL," +
		"average REAL," +
		"update_time TEXT" +
		")"
	  );
	} catch (e) {
	  console.error(`创建表失败:`, e);
	  return { inserted: 0, failed: codeList, error: e.message };
	}
  
	// 使用智能处理函数处理代码列表
	const results = await processCodeList(env, codeList, update_time);
	console.log(`定时任务完成，成功插入 ${results.inserted} 条数据，失败代码: ${results.failed.join(', ')}`);
	return results;
  }
  
  export default {
	async fetch(request, env, ctx) {
	  if (request.method === 'POST') {
		let req;
		try { req = await request.json(); } catch (_) { req = {}; }
		if (req.codes) codes = req.codes;
		const result = await fetchAndSaveAllCodes(env);
		return Response.json(result, { status: 200 });
	  }
	  if (request.method === 'GET') {
		const { results } = await env.DB.prepare(
		  "SELECT code, COUNT(*) AS record_count FROM minute_klines GROUP BY code ORDER BY code DESC LIMIT 100"
		).all();
		return Response.json(results);
	  }
	  return new Response('Use POST for data fetch and save, or GET for query.', { status: 400 });
	},
  
	async scheduled(event, env, ctx) {
	  try {
		console.log(`定时任务触发，时间: ${getBeijingTime()}`);
		const result = await fetchAndSaveAllCodes(env);
		console.log(`定时任务执行结果:`, result);
		return result;
	  } catch (error) {
		console.error(`定时任务执行失败:`, error);
	  }
	}
  };