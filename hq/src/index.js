/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

const code_to_mkt = {
	c: "DCE", cu: "SHFE", rb: "SHFE", /* ...补充全部 */ 
  };
  
  function getMarket(code) {
	let code2 = code.replace(/\d/g, '');
	let code3 = code2.replace(/[mM]$/, '');
	return code_to_mkt[code3];
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
  
  // 可配置抓取的 codes
  const codes = ["rbm", "cum"]; // ...可补充
  
  function getBeijingTime() {
	let now = new Date(Date.now() + 8 * 3600 * 1000);
	return now.toISOString().slice(0, 19).replace('T', ' ');
  }
  
  export default {
	async fetch(request, env, ctx) {
	  if (request.method === 'POST') {
		let req;
		try { req = await request.json(); } catch (_) { req = {}; }
		let codeList = Array.isArray(req.codes) ? req.codes : codes;
		let update_time = getBeijingTime();
  
		await env.DB.exec(`
		  CREATE TABLE IF NOT EXISTS minute_klines (
			timestamp TEXT,
			code TEXT,
			open REAL,
			close REAL,
			high REAL,
			low REAL,
			volume REAL,
			amount REAL,
			average REAL,
			update_time TEXT
		  );
		`);
  
		let inserted = 0, failed = [];
		for (const code of codeList) {
		  const klineRows = await fetchKline(code);
		  if (!klineRows) { failed.push(code); continue; }
		  for (const row of klineRows) {
			try {
			  await env.DB.prepare(
				`INSERT INTO minute_klines (timestamp, code, open, close, high, low, volume, amount, average, update_time)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
			  ).bind(
				row.timestamp, code,
				row.open, row.close, row.high, row.low, row.volume, row.amount, row.average,
				update_time
			  ).run();
			  inserted += 1;
			} catch (e) { /* 可加错误处理 */ }
		  }
		}
		return Response.json({ inserted, failed }, { status: 200 });
	  }
  
	  if (request.method === 'GET') {
		const { results } = await env.DB.prepare("SELECT * FROM minute_klines ORDER BY timestamp DESC LIMIT 100;").all();
		return Response.json(results);
	  }
  
	  return new Response('Use POST for data fetch and save, or GET for query.', { status: 400 });
	}
  }