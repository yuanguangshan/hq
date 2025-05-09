//可连接github进行构建
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
  
  async function fetchKline(code) {
	const mkt = getMarket(code);
	if (!mkt) return null;
	const url = `http://futsse.eastmoney.com/static/rtdata/${mkt}_${code}`;
	const resp = await fetch(url);
	if (!resp.ok) return null;
	const data = await resp.json();
	if (!data?.data?.trends) return null;
	const trends = data.data.trends;
	const columns = ['timestamp', 'open', 'close', 'high', 'low', 'volume', 'amount', 'average'];
	return trends.map(row => {
	  const arr = row.split(',');
	  let obj = {};
	  for (let i = 0; i < columns.length; ++i) obj[columns[i]] = arr[i];
	  return obj;
	});
  }
  
//   let codes = ["rbm", "cum"]; // 可补充
	let codes = Array.from(
	new Set(Object.keys(code_to_mkt).map(code => code.toLowerCase() + 'm'))
  );
  
  function getBeijingTime() {
	let now = new Date(Date.now() + 8 * 3600 * 1000);
	return now.toISOString().slice(0, 19).replace('T', ' ');
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
  
	let inserted = 0, failed = [];
	for (const code of codeList) {
	  console.log(`处理代码: ${code}`);
	  const mkt = getMarket(code);
	  if (!mkt) {
		console.error(`未找到代码 ${code} 对应的交易所`);
		failed.push(code);
		continue;
	  }
	  
	  try {
		const klineRows = await fetchKline(code);
		if (!klineRows) { 
		  console.error(`获取 ${code} 的K线数据失败`);
		  failed.push(code); 
		  continue; 
		}
		console.log(`成功获取 ${code} 的K线数据，共 ${klineRows.length} 条`);
		
		for (const row of klineRows) {
		  try {
			await env.DB.prepare(
			  "INSERT INTO minute_klines (timestamp, code, open, close, high, low, volume, amount, average, update_time) " +
			  "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
			).bind(
			  row.timestamp, code,
			  row.open, row.close, row.high, row.low, row.volume, row.amount, row.average,
			  update_time
			).run();
			inserted += 1;
		  } catch (e) {
			console.error(`插入 ${code} 的数据失败:`, e);
		  }
		}
	  } catch (e) {
		console.error(`处理 ${code} 时发生错误:`, e);
		failed.push(code);
	  }
	}
	console.log(`定时任务完成，成功插入 ${inserted} 条数据，失败代码: ${failed.join(', ')}`);
	return { inserted, failed };
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
		  "SELECT * FROM minute_klines ORDER BY timestamp DESC LIMIT 100"
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