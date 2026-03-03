import express from "express";
import fs from "fs";
import path from "path";
import https from "https";
import archiver from "archiver";
import sharp from "sharp";

/**
 * SHARP GLOBAL AYARLAR
 */
sharp.cache(false);
sharp.concurrency(1);

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 10000;
const BASE_DIR = "/tmp/wandini";

fs.mkdirSync(BASE_DIR, { recursive: true });

/**
 * SIMPLE IN-MEMORY QUEUE
 */
let isProcessing = false;
const queue = [];
const doneOrders = new Set();

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
        file.on("finish", () => file.close(resolve));
      })
      .on("error", reject);
  });
}

function orderToXML(order, masterAssetId) {
  const orderNumber = order.id;

  const shippingType = "Standard"; // MANUAL (değiştirilebilir)

  const shippingFrom = order.billing_address || {};
  const shippingTo = order.shipping_address || {};

  // Configurator output width/height
  let outputWidth = null;
  let outputHeight = null;
  let quantity = 1;

  for (const item of order.line_items || []) {
    quantity = item.quantity || 1;

    for (const p of item.properties || []) {
      if (p.name === "configurator_payload") {
        const cfg = JSON.parse(p.value);
        outputWidth = cfg.output?.width;
        outputHeight = cfg.output?.height;
      }
    }
  }

  const sku = "1.14-1.3.10"; // MANUAL – factory'den gelecek

  return `<?xml version="1.0" encoding="UTF-8"?>
<root>
  <order>
    <order_number>${orderNumber}</order_number>
    <reference></reference>
    <shipping_type>${shippingType}</shipping_type>

    <shipping_from>
      <company>${shippingFrom.company || shippingFrom.name || ""}</company>
      <contact_person>${shippingFrom.name || ""}</contact_person>
      <street>${shippingFrom.address1 || ""}</street>
      <postcode>${shippingFrom.zip || ""}</postcode>
      <city>${shippingFrom.city || ""}</city>
      <country>${shippingFrom.country_code || ""}</country>
    </shipping_from>

    <shipping_to>
      <company>${shippingTo.company || shippingTo.name || ""}</company>
      <contact_person>${shippingTo.name || ""}</contact_person>
      <street>${shippingTo.address1 || ""}</street>
      <postcode>${shippingTo.zip || ""}</postcode>
      <city>${shippingTo.city || ""}</city>
      <country>${shippingTo.country_code || ""}</country>
      <phone>${shippingTo.phone || ""}</phone>
    </shipping_to>

    <delivery_note type="ftp"></delivery_note>
  </order>

  <positions>
    <position>
      <sku>${sku}</sku>
      <width unit="mm">${outputWidth}</width>
      <height unit="mm">${outputHeight}</height>
      <variants>1</variants>
      <copies_per_variant>${quantity}</copies_per_variant>
      <files>
        <file type="ftp">${orderNumber}_1.pdf</file>
      </files>
    </position>
  </positions>
</root>`;
}

/**
 * CORE WORKER
 */
async function processOrder(order) {
  const orderId = order.id;
  isProcessing = true;

  try {
    log(orderId, "Processing started");

    let masterAssetId = null;
    let cropRatio = null;

    for (const item of order.line_items || []) {
      for (const p of item.properties || []) {
        if (p.name === "configurator_payload") {
          const cfg = JSON.parse(p.value);
          masterAssetId = cfg.master_asset_id;
          cropRatio = cfg.crop_ratio;
        }
      }
    }

    if (!masterAssetId || !cropRatio) {
      log(orderId, "Missing configurator data, skipping");
      return;
    }

    const orderDir = path.join(BASE_DIR, String(orderId));
    fs.mkdirSync(orderDir, { recursive: true });

    const masterUrl = `https://storage.googleapis.com/wandini-masters/${masterAssetId}/master.png`;
    const masterPath = path.join(orderDir, "master.png");
    const croppedPath = path.join(orderDir, "cropped.png");
    const xmlPath = path.join(orderDir, "order.xml");

    await downloadFile(masterUrl, masterPath);
    log(orderId, "Master file downloaded");

    fs.writeFileSync(xmlPath, orderToXML(order, masterAssetId));
    log(orderId, "XML created");

    log(orderId, "Sharp started");
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

    await image.extract(crop).png({ compressionLevel: 0 }).toFile(croppedPath);

    log(orderId, `Crop completed (${Date.now() - t0} ms)`);

    log(orderId, `ZIP ready: /download/${orderId}`);
    doneOrders.add(orderId);
  } catch (err) {
    console.error(`[${orderId}] PROCESS ERROR:`, err);
  } finally {
    isProcessing = false;

    if (queue.length > 0) {
      const next = queue.shift();
      processOrder(next);
    }
  }
}

/**
 * WEBHOOK
 */
app.post("/webhooks/orders-paid", (req, res) => {
  const order = req.body;
  const orderId = order.id;

  if (!orderId) return res.status(400).send("order.id missing");

  if (doneOrders.has(orderId)) {
    log(orderId, "Already done, skipping");
    return res.status(200).send("ok");
  }

  if (isProcessing) {
    log(orderId, "Queued (worker busy)");
    queue.push(order);
    return res.status(200).send("queued");
  }

  log(orderId, "Accepted for processing");
  processOrder(order);
  res.status(200).send("processing");
});

/**
 * DOWNLOAD ZIP
 */
app.get("/download/:orderId", (req, res) => {
  const { orderId } = req.params;
  const dir = path.join(BASE_DIR, orderId);

  if (!fs.existsSync(dir)) {
    return res.status(404).send("Order artifacts not found");
  }

  res.setHeader("Content-Type", "application/zip");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename=wandini-${orderId}.zip`,
  );

  const archive = archiver("zip", { zlib: { level: 9 } });
  archive.pipe(res);

  archive.file(path.join(dir, "order.xml"), { name: "order.xml" });
  archive.file(path.join(dir, "cropped.png"), { name: "cropped.png" });

  archive.finalize();
});

/**
 * HEALTH
 */
app.get("/", (_req, res) => {
  res.send("wandini orchestrator alive");
});

app.listen(PORT, () => {
  console.log(`Orchestrator listening on port ${PORT}`);
});
