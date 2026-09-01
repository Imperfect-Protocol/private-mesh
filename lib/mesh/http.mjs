export function corsHeaders(extra = {}) {
  return {
    "access-control-allow-headers": "content-type",
    "access-control-allow-methods": "GET, HEAD, POST, OPTIONS",
    "access-control-allow-origin": "*",
    "cache-control": "no-store",
    ...extra,
  };
}

export function sendJson(res, statusCode, value) {
  const body = JSON.stringify(value);
  res.writeHead(statusCode, corsHeaders({
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
  }));
  res.end(body);
}

export function readJsonBody(req, maxBodyBytes) {
  return new Promise((resolve, reject) => {
    let body = "";
    req.setEncoding("utf8");
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > maxBodyBytes) {
        reject(Object.assign(new Error("request body too large"), { statusCode: 413 }));
        req.destroy();
      }
    });
    req.on("end", () => {
      try {
        resolve(body ? JSON.parse(body) : {});
      } catch {
        reject(Object.assign(new Error("invalid JSON request body"), { statusCode: 400 }));
      }
    });
    req.on("error", reject);
  });
}
