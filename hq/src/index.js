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
// let codes = ["im", "tam", "agm", "rbm", "pm"]

// 完整代码列表，暂时注释掉以减少API请求数量
let codes = Array.from(
	new Set(Object.keys(code_to_mkt).map(code => code.toLowerCase() + 'm'))
);

// 最大重试次数
const MAX_RETRIES = 3;
// 重试延迟基础时间（毫秒）
const BASE_RETRY_DELAY = 15000;
  
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

// 智能处理代码列表，将敏感代码（如tam）分散处理
async function processCodeList(env, codeList, update_time) {
  // 将敏感代码（如tam）单独处理
  const sensitiveCode = 'tam';
  const regularCodes = codeList.filter(code => code !== sensitiveCode);
  const hasSensitiveCode = codeList.includes(sensitiveCode);
  
  let results = {
    inserted: 0,
    failed: []
  };
  
  // 先处理常规代码
  if (regularCodes.length > 0) {
    console.log(`开始处理常规代码: ${regularCodes.join(', ')}`);
    const regularResults = await processCodeBatch(env, regularCodes, update_time);
    results.inserted += regularResults.inserted;
    results.failed = [...results.failed, ...regularResults.failed];
    
    // 常规代码处理完后等待较长时间，确保API冷却
    if (hasSensitiveCode) {
      console.log('常规代码处理完毕，等待30秒后处理敏感代码...');
      await delay(30000);
    }
  }
  
  // 单独处理敏感代码
  if (hasSensitiveCode) {
    console.log(`开始处理敏感代码: ${sensitiveCode}`);
    const sensitiveResults = await processCodeBatch(env, [sensitiveCode], update_time, true);
    results.inserted += sensitiveResults.inserted;
    results.failed = [...results.failed, ...sensitiveResults.failed];
  }
  
  return results;
}

// 处理一批代码
async function processCodeBatch(env, codeBatch, update_time, isSensitive = false) {
  let inserted = 0, failed = [];
  const REQUEST_INTERVAL = isSensitive ? 20000 : 10000; // 敏感代码使用更长的间隔
  
  for (const code of codeBatch) {
    console.log(`处理代码: ${code}`);
    const mkt = getMarket(code);
    if (!mkt) {
      console.error(`未找到代码 ${code} 对应的交易所`);
      failed.push(code);
      continue;
    }
    
    try {
      // 添加请求前的延时，避免API限流
      await delay(REQUEST_INTERVAL);
      console.log(`开始请求 ${code} 的K线数据...`);
      
      const klineRows = await fetchKline(code);
      if (!klineRows) { 
        console.error(`获取 ${code} 的K线数据失败`);
        failed.push(code); 
        continue; 
      }
      console.log(`成功获取 ${code} 的K线数据，共 ${klineRows.length} 条`);
      
      // 对于敏感代码，进一步分批处理数据，每批之间增加额外延时
      if (isSensitive && klineRows.length > 50) {
        console.log(`${code} 数据量较大，将分批处理以避免API限制`);
        const batchSize = 50;
        let totalInserted = 0;
        
        for (let i = 0; i < klineRows.length; i += batchSize) {
          const dataBatch = klineRows.slice(i, i + batchSize);
          console.log(`处理 ${code} 数据批次 ${i/batchSize + 1}/${Math.ceil(klineRows.length/batchSize)}`);
          
          try {
            const batchInserted = await batchInsertData(env, dataBatch, code, update_time);
            totalInserted += batchInserted;
            console.log(`批次插入成功: ${batchInserted} 条`);
            
            // 批次之间添加延时
            if (i + batchSize < klineRows.length) {
              await delay(5000);
            }
          } catch (e) {
            console.error(`批次处理失败:`, e);
            if (isRateLimitError(e)) {
              console.log('检测到API限制，增加等待时间...');
              await delay(30000); // 遇到限制时等待更长时间
              
              // 重试当前批次，但减小批次大小
              try {
                const smallerBatch = dataBatch.slice(0, dataBatch.length / 2);
                const retryInserted = await batchInsertData(env, smallerBatch, code, update_time);
                totalInserted += retryInserted;
                console.log(`使用较小批次重试成功: ${retryInserted} 条`);
              } catch (retryError) {
                console.error(`重试失败:`, retryError);
              }
            }
          }
        }
        
        inserted += totalInserted;
        console.log(`成功插入 ${code} 的数据共 ${totalInserted} 条`);
      } else {
        // 常规处理
        const insertedCount = await batchInsertData(env, klineRows, code, update_time);
        inserted += insertedCount;
        console.log(`成功插入 ${code} 的数据 ${insertedCount} 条`);
      }
      
      // 每个代码处理完成后添加额外的冷却时间
      const cooldownTime = isSensitive ? REQUEST_INTERVAL * 2 : REQUEST_INTERVAL / 2;
      await delay(cooldownTime);
      
    } catch (e) {
      console.error(`处理 ${code} 时发生错误:`, e);
      
      if (isRateLimitError(e)) {
        console.log(`检测到API限制错误，将 ${code} 标记为失败并继续处理其他代码`);
        failed.push(code);
        // 遇到限制时等待较长时间再继续
        await delay(45000);
      } else {
        failed.push(code);
        await delay(5000); // 其他错误等待较短时间
      }
    }
  }
  
  return { inserted, failed };
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
		  "SELECT distinct code FROM minute_klines ORDER BY timestamp DESC LIMIT 100"
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