import express from 'express';
import { v4 as uuidv4 } from 'uuid';

const app = express();
app.use(express.json({ limit: '10mb' }));

app.post('/webhooks/orders-paid', (req, res) => {
  try {
    const order = req.body;

    // 🔍 Line item properties içinden configurator payload’u bul
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

    console.log('JOB RECEIVED:', JSON.stringify(job, null, 2));
    res.status(200).json({ ok: true });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'internal error' });
  }
});

app.get('/', (_req, res) => {
  res.send('wandini orchestrator alive');
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Orchestrator listening on ${PORT}`);
});
