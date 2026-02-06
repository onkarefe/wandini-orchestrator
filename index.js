import express from 'express';
import fs from 'fs';
import path from 'path';
import https from 'https';
import archiver from 'archiver';
import sharp from 'sharp';

/**
 * SHARP GLOBAL AYARLAR
 */
sharp.cache(false);
sharp.concurrency(1);

const app = express();
app.use(express.json({ limit: '20mb' }));

const PORT = process.env.PORT || 10000;
const BASE_DIR = '/tmp/wandini';

// ORDER-LEVEL LOCK (TEST İÇİN)
const processingOrders = new Set();

fs.mkdirSync(BASE_DIR, { recursive: true });

function log(orderId, msg) {
  console.log(`[${orderId}] ${msg}`);
}

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

app.post('/webhooks/orders-paid', async (req, res) => {
  const order = req.body;
  const orderId = order.id;

  if (!orderId) return res.status(400).send('order.id missing');

  // 🔒 DUPLICATE WEBHOOK KORUMASI
  if (processingOrders.has(orderId)) {
    log(orderId, 'Duplicate webhook ignored (already processing)');
    return res.status(200).send('ok');
  }

  processingOrders.add(orderId);

  try {
    let masterAssetId = null;
    let cropRatio = null;

    for (const item of order.line_items || []) {
      for (const p of item.properties || []) {
        if (p.name === 'configurator_payload') {
          const cfg = JSON.parse(p.value);
          masterAssetId = cfg.master_asset_id;
          cropRatio = cfg.crop_ratio;
        }
      }
    }

    if (!masterAssetId) return res.status(400).send('master_asset_id missing');
    if (!cropRatio) return res.status(400).send('crop_ratio missing');

    const orderDir = path.join(BASE_DIR, String(orderId));
    fs.mkdirSync(orderDir, { recursive: true });

    const masterUrl = `https://storage.googleapis.com/wandini-masters/${masterAssetId}/master.png`;
    const masterPath = path.join(orderDir, 'master.png');
    const croppedPath = path.join(orderDir, 'cropped.png');
    const xmlPath = path.join(orderDir, 'order.xml');

    log(orderId, 'Webhook received');

    await downloadFile(masterUrl, masterPath);
    log(orderId, 'Master file downloaded');

    fs.writeFileSync(xmlPath, orderToXML(order, masterAssetId));
    log(orderId, 'XML created');

    log(orderId, 'Sharp started');
    const t0 = Date.now();

    const image = sharp(masterPath, {
      sequentialRead: true,
      limitInputPixels: false,
    });

    const meta = await image.metadata();

    const crop = {
      left: Math.round(meta.width * cropRatio.x),
      top: Math.round(meta.height * cropRatio.y),
      width: Math.round(meta.width * cropRatio.w),
      height: Math.round(meta.height * cropRatio.h),
    };

    await image
      .extract(crop)
      .png({ compressionLevel: 0 })
      .toFile(croppedPath);

    log(orderId, `Crop completed (${Date.now() - t0} ms)`);

    const downloadUrl = `/download/${orderId}`;
    log(orderId, `ZIP ready: ${downloadUrl}`);

    res.status(200).json({ ok: true, download: downloadUrl });
  } catch (err) {
    console.error('WEBHOOK ERROR:', err);
    res.status(500).send('internal error');
  } finally {
    processingOrders.delete(orderId);
  }
});

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

  archive.file(path.join(dir, 'order.xml'), { name: 'order.xml' });
  archive.file(path.join(dir, 'cropped.png'), { name: 'cropped.png' });

  archive.finalize();
});

app.get('/', (_req, res) => {
  res.send('wandini orchestrator alive');
});

app.listen(PORT, () => {
  console.log(`Orchestrator listening on port ${PORT}`);
});
