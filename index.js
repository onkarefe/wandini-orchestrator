import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import crypto from 'crypto';

const app = express();
app.use(express.json({ limit: '10mb' }));

/**
 * Shopify HMAC doğrulama
 */
function verifyShopifyHmac(req, secret) {
  const hmac = req.get('X-Shopify-Hmac-Sha256');
  const body = JSON.stringify(req.body);

  const digest = crypto
    .createHmac('sha256', secret)
    .update(body, 'utf8')
    .digest('base64');

  if (!hmac) return false;

  return crypto.timingSafeEqual(
    Buffer.from(hmac),
    Buffer.from(digest)
  );
}

/**
 * Basit idempotency (ilk faz)
 */
const processedOrders = new Set();

/**
 * Webhook endpoint
 */
app.post('/webhooks/orders-paid', (req, res) => {
  // 🔐 HMAC doğrulama
  if (!verifyShopifyHmac(req, process.env.SHOPIFY_WEBHOOK_SECRET)) {
    console.error('Invalid HMAC');
    return res.status(401).send('Invalid HMAC');
  }

  // 🔁 Idempotency
  const orderId = req.body?.id;
  if (processedOrders.has(orderId)) {
    console.log('Duplicate order received:', orderId);
    return res.status(200).json({ ok: true, duplicate: true });
  }
  processedOrders.add(orderId);

  try {
    const order = req.body;

    // 🔍 Configurator payload'u bul
    const lineItems = order?.line_items || [];
    let config = null;

    for (const item of lineItems) {
      const props = item.properties || [];
      for (const p of props) {
        if (p.name === 'configurator_payload') {
          config = JSON.parse(p.value);
        }
      }
    }

    if (!config) {
      console.error('Configurator payload not found');
      return res.status(400).json({ error: 'configurator_payload missing' });
    }

    // 🧠 Normalized job
    const job = {
      job_id: uuidv4(),
      master_asset_id: config.master_asset_id,
      master_url: `https://storage.googleapis.com/wandini-masters/${config.master_asset_id}/master.png`,
      output_mm: {
        w: config.output.width,
        h: config.output.height
      },
      crop: config.crop_ratio,
      status: 'RECEIVED'
    };

    console.log('JOB RECEIVED:\n', JSON.stringify(job, null, 2));

    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error('Webhook processing error:', err);
    return res.status(500).json({ error: 'internal error' });
  }
});

/**
 * Health check
 */
app.get('/', (_req, res) => {
  res.send('wandini orchestrator alive');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Orchestrator listening on port ${PORT}`);
});
