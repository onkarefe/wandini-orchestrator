import express from 'express';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import https from 'https';

const app = express();
app.use(express.json({ limit: '10mb' }));

const ARTIFACT_DIR = '/tmp/wandini';
fs.mkdirSync(ARTIFACT_DIR, { recursive: true });

function verifyShopifyHmac(req, secret) {
  const hmac = req.get('X-Shopify-Hmac-Sha256');
  if (!hmac) return false;

  const digest = crypto
    .createHmac('sha256', secret)
    .update(JSON.stringify(req.body), 'utf8')
    .digest('base64');

  return crypto.timingSafeEqual(Buffer.from(hmac), Buffer.from(digest));
}

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (res) => {
      res.pipe(file);
      file.on('finish', () => file.close(resolve));
    }).on('error', reject);
  });
}

function orderToXML(order, masterAssetId) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<Order>
  <OrderId>${order.id}</OrderId>
  <MasterAssetId>${masterAssetId}</MasterAssetId>
  <Email>${order.email}</Email>
  <TotalPrice>${order.total_price}</TotalPrice>
  <Currency>${order.currency}</Currency>
  <RawPayload><![CDATA[${JSON.stringify(order)}]]></RawPayload>
</Order>`;
}

app.post('/webhooks/orders-paid', async (req, res) => {
  if (!verifyShopifyHmac(req, process.env.SHOPIFY_WEBHOOK_SECRET)) {
    return res.status(401).send('Invalid HMAC');
  }

  const order = req.body;
  const orderId = order.id;

  let masterAssetId = null;
  for (const item of order.line_items || []) {
    for (const p of item.properties || []) {
      if (p.name === 'configurator_payload') {
        const cfg = JSON.parse(p.value);
        masterAssetId = cfg.master_asset_id;
      }
    }
  }

  if (!masterAssetId) {
    return res.status(400).send('master_asset_id missing');
  }

  const masterUrl = `https://storage.googleapis.com/wandini-masters/${masterAssetId}/master.png`;

  const orderDir = path.join(ARTIFACT_DIR, String(orderId));
  fs.mkdirSync(orderDir, { recursive: true });

  const masterPath = path.join(orderDir, 'master.png');
  const xmlPath = path.join(orderDir, 'order.xml');

  await downloadFile(masterUrl, masterPath);
  fs.writeFileSync(xmlPath, orderToXML(order, masterAssetId));

  console.log('ARTIFACTS READY:', {
    orderId,
    masterPath,
    xmlPath
  });

  res.status(200).json({ ok: true });
});

app.get('/', (_req, res) => {
  res.send('wandini orchestrator alive');
});

const PORT = process.env.PORT || 10000;
app.listen(PORT, () => {
  console.log(`Orchestrator listening on port ${PORT}`);
});
