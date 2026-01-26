import express from 'express';
import fs from 'fs';
import path from 'path';
import https from 'https';
import archiver from 'archiver';

const app = express();
app.use(express.json({ limit: '20mb' }));

const PORT = process.env.PORT || 10000;
const BASE_DIR = '/tmp/wandini';

// klasör garanti
fs.mkdirSync(BASE_DIR, { recursive: true });

/**
 * Google Cloud Storage'dan dosya indirir
 */
function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);

    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`Download failed: ${res.statusCode}`));
          return;
        }

        res.pipe(file);
        file.on('finish', () => file.close(resolve));
      })
      .on('error', reject);
  });
}

/**
 * Shopify order -> XML
 */
function orderToXML(order, masterAssetId) {
  return `<?xml version="1.0" encoding="UTF-8"?>
<Order>
  <OrderId>${order.id}</OrderId>
  <Email>${order.email || ''}</Email>
  <Currency>${order.currency}</Currency>
  <TotalPrice>${order.total_price}</TotalPrice>
  <MasterAssetId>${masterAssetId}</MasterAssetId>
  <RawPayload><![CDATA[${JSON.stringify(order)}]]></RawPayload>
</Order>`;
}

/**
 * WEBHOOK — orders/paid
 * HMAC KAPALI (Hookdeck arkasındayız)
 */
app.post('/webhooks/orders-paid', async (req, res) => {
  try {
    const order = req.body;
    const orderId = order.id;

    if (!orderId) {
      return res.status(400).send('order.id missing');
    }

    // master_asset_id extract
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

    const orderDir = path.join(BASE_DIR, String(orderId));
    fs.mkdirSync(orderDir, { recursive: true });

    const masterUrl = `https://storage.googleapis.com/wandini-masters/${masterAssetId}/master.png`;
    const masterPath = path.join(orderDir, 'master.png');
    const xmlPath = path.join(orderDir, 'order.xml');

    await downloadFile(masterUrl, masterPath);
    fs.writeFileSync(xmlPath, orderToXML(order, masterAssetId));

    console.log('ARTIFACTS READY:', {
      orderId,
      masterPath,
      xmlPath,
    });

    res.status(200).json({ ok: true });
  } catch (err) {
    console.error('WEBHOOK ERROR:', err);
    res.status(500).send('internal error');
  }
});

/**
 * DOWNLOAD — ZIP
 */
app.get('/download/:orderId', (req, res) => {
  const { orderId } = req.params;
  const dir = path.join(BASE_DIR, orderId);

  if (!fs.existsSync(dir)) {
    return res.status(404).send('Order artifacts not found');
  }

  res.setHeader('Content-Type', 'application/zip');
  res.setHeader(
    'Content-Disposition',
    `attachment; filename=wandini-${orderId}.zip`
  );

  const archive = archiver('zip', { zlib: { level: 9 } });
  archive.pipe(res);

  archive.file(path.join(dir, 'master.png'), { name: 'master.png' });
  archive.file(path.join(dir, 'order.xml'), { name: 'order.xml' });

  archive.finalize();
});

/**
 * HEALTH
 */
app.get('/', (_req, res) => {
  res.send('wandini orchestrator alive');
});

app.listen(PORT, () => {
  console.log(`Orchestrator listening on port ${PORT}`);
});
