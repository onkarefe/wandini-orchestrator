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
const MAX_PANEL_CM = 70;
const TMP_LIMIT_BYTES = 2 * 1024 * 1024 * 1024; // Render /tmp hard limit (2GB)
const TMP_SAFETY_BYTES = 200 * 1024 * 1024; // Leave headroom to avoid eviction
const ARTIFACT_TTL_MS = 2 * 60 * 60 * 1000; // 2 hours

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

function logStep(orderId, step, details = null) {
  if (details === null || details === undefined) {
    console.log(`[${orderId}] [${step}]`);
    return;
  }
  console.log(`[${orderId}] [${step}]`, details);
}

function formatBytes(bytes) {
  if (!Number.isFinite(bytes) || bytes < 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return `${value.toFixed(2)} ${units[idx]}`;
}

function getDirSizeBytes(targetPath) {
  if (!fs.existsSync(targetPath)) return 0;
  const stat = fs.statSync(targetPath);
  if (stat.isFile()) return stat.size;
  if (!stat.isDirectory()) return 0;
  let total = 0;
  for (const entry of fs.readdirSync(targetPath)) {
    total += getDirSizeBytes(path.join(targetPath, entry));
  }
  return total;
}

function getTmpUsageBytes() {
  if (!fs.existsSync(BASE_DIR)) return 0;
  let total = 0;
  for (const entry of fs.readdirSync(BASE_DIR)) {
    total += getDirSizeBytes(path.join(BASE_DIR, entry));
  }
  return total;
}

function cleanupStaleArtifacts(orderId) {
  if (!fs.existsSync(BASE_DIR)) return { removed: 0, freedBytes: 0 };
  const now = Date.now();
  let removed = 0;
  let freedBytes = 0;
  for (const entry of fs.readdirSync(BASE_DIR)) {
    const dirPath = path.join(BASE_DIR, entry);
    let stat;
    try {
      stat = fs.statSync(dirPath);
    } catch {
      continue;
    }
    if (!stat.isDirectory()) continue;
    const ageMs = now - stat.mtimeMs;
    if (ageMs < ARTIFACT_TTL_MS) continue;
    const dirSize = getDirSizeBytes(dirPath);
    try {
      fs.rmSync(dirPath, { recursive: true, force: true });
      removed += 1;
      freedBytes += dirSize;
    } catch (err) {
      logStep(orderId, "tmp.cleanup.stale_failed", {
        dirPath,
        message: err?.message || String(err),
      });
    }
  }
  return { removed, freedBytes };
}

function ensureTmpBudget(orderId, estimatedNeedBytes) {
  const usage = getTmpUsageBytes();
  const available = Math.max(0, TMP_LIMIT_BYTES - usage);
  const availableSafe = Math.max(0, available - TMP_SAFETY_BYTES);
  logStep(orderId, "tmp.budget.check", {
    usageBytes: usage,
    usageHuman: formatBytes(usage),
    estimatedNeedBytes,
    estimatedNeedHuman: formatBytes(estimatedNeedBytes),
    availableBytes: available,
    availableHuman: formatBytes(available),
    availableSafeBytes: availableSafe,
    availableSafeHuman: formatBytes(availableSafe),
  });
  if (estimatedNeedBytes > availableSafe) {
    throw new Error(
      `Insufficient /tmp space: need ${formatBytes(estimatedNeedBytes)}, safe-available ${formatBytes(availableSafe)}`,
    );
  }
}

function parseConfiguratorPayload(order) {
  for (const item of order.line_items || []) {
    for (const p of item.properties || []) {
      if (p.name === "configurator_payload" && p.value) {
        return JSON.parse(p.value);
      }
    }
  }
  return null;
}

function computePanelsFromOutputMm(outputWidthMm) {
  const widthCm = Number(outputWidthMm) / 10;
  if (!Number.isFinite(widthCm) || widthCm <= 0) {
    return { widthCm: 0, panelCount: 0, panelWidthCm: 0 };
  }
  const panelCount = Math.max(1, Math.ceil(widthCm / MAX_PANEL_CM));
  const panelWidthCm = widthCm / panelCount;
  return { widthCm, panelCount, panelWidthCm };
}

function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    const startedAt = Date.now();

    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error(`Download failed: ${res.statusCode}`));
          return;
        }
        console.log(`[download] started`, { url, statusCode: res.statusCode, dest });
        res.pipe(file);
        file.on("finish", () => {
          file.close(() => {
            console.log(`[download] completed`, {
              url,
              dest,
              durationMs: Date.now() - startedAt,
            });
            resolve();
          });
        });
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
  let panelCount = 0;
  let panelWidthCm = 0;
  let quantity = 1;

  for (const item of order.line_items || []) {
    quantity = item.quantity || 1;

    for (const p of item.properties || []) {
      if (p.name === "configurator_payload") {
        const cfg = JSON.parse(p.value);
        outputWidth = cfg.output?.width;
        outputHeight = cfg.output?.height;
        const panelInfo = computePanelsFromOutputMm(outputWidth);
        panelCount = panelInfo.panelCount;
        panelWidthCm = panelInfo.panelWidthCm;
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
      <panel_count>${panelCount}</panel_count>
      <panel_width_cm>${panelWidthCm.toFixed(4)}</panel_width_cm>
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
    logStep(orderId, "processing.start", {
      queueLength: queue.length,
      lineItemCount: (order.line_items || []).length,
    });
    const stale = cleanupStaleArtifacts(orderId);
    logStep(orderId, "tmp.cleanup.stale_done", {
      removedDirs: stale.removed,
      freedBytes: stale.freedBytes,
      freedHuman: formatBytes(stale.freedBytes),
      usageAfterCleanup: formatBytes(getTmpUsageBytes()),
    });

    const cfg = parseConfiguratorPayload(order);
    const masterAssetId = cfg?.master_asset_id || null;
    const cropRatio = cfg?.crop_ratio || null;
    const outputWidthMm = cfg?.output?.width;
    const outputHeightMm = cfg?.output?.height;

    const panelInfo = computePanelsFromOutputMm(outputWidthMm);
    logStep(orderId, "configurator.payload", {
      version: cfg?.version ?? null,
      masterAssetId,
      output: { widthMm: outputWidthMm, heightMm: outputHeightMm, unit: cfg?.output?.unit },
      cropRatio,
      panelCount: panelInfo.panelCount,
      panelWidthCm: Number(panelInfo.panelWidthCm.toFixed(4)),
    });

    if (!masterAssetId || !cropRatio) {
      logStep(orderId, "processing.skip", "Missing configurator data");
      return;
    }

    const orderDir = path.join(BASE_DIR, String(orderId));
    fs.mkdirSync(orderDir, { recursive: true });
    logStep(orderId, "fs.orderDir.ready", { orderDir });

    const masterUrl = `https://storage.googleapis.com/wandini-masters/${masterAssetId}/master.png`;
    const masterPath = path.join(orderDir, "master.png");
    const xmlPath = path.join(orderDir, "order.xml");
    logStep(orderId, "paths.prepared", { masterUrl, masterPath, xmlPath });

    await downloadFile(masterUrl, masterPath);
    logStep(orderId, "download.master.done");

    fs.writeFileSync(xmlPath, orderToXML(order, masterAssetId));
    logStep(orderId, "xml.created", { xmlPath });

    logStep(orderId, "sharp.start");
    const t0 = Date.now();

    const image = sharp(masterPath, {
      sequentialRead: true,
      limitInputPixels: false,
    });

    const meta = await image.metadata();
    logStep(orderId, "sharp.metadata", {
      width: meta.width,
      height: meta.height,
      format: meta.format,
    });

    const crop = {
      left: Math.round(meta.width * cropRatio.x),
      top: Math.round(meta.height * cropRatio.y),
      width: Math.round(meta.width * cropRatio.w),
      height: Math.round(meta.height * cropRatio.h),
    };
    const safeCrop = {
      left: Math.max(0, Math.min(crop.left, Math.max(0, meta.width - 1))),
      top: Math.max(0, Math.min(crop.top, Math.max(0, meta.height - 1))),
      width: Math.max(1, Math.min(crop.width, Math.max(1, meta.width - crop.left))),
      height: Math.max(1, Math.min(crop.height, Math.max(1, meta.height - crop.top))),
    };
    logStep(orderId, "sharp.crop.calculated", { requested: crop, safeCrop });
    const estimatedPanelBytes = Math.ceil(safeCrop.width * safeCrop.height * 4 * 1.1);
    const estimatedTotalNeedBytes = estimatedPanelBytes + 100 * 1024 * 1024;
    ensureTmpBudget(orderId, estimatedTotalNeedBytes);

    const panelCount = panelInfo.panelCount || 1;
    const panelPxWidths = [];
    let remainingPx = safeCrop.width;
    for (let i = 0; i < panelCount; i++) {
      const last = i === panelCount - 1;
      if (last) {
        panelPxWidths.push(Math.max(1, remainingPx));
        break;
      }
      const remainingPieces = panelCount - i;
      const minReservedForRest = remainingPieces - 1;
      const maxThis = Math.max(1, remainingPx - minReservedForRest);
      const estimated = Math.round(safeCrop.width / panelCount);
      const currentPx = Math.min(Math.max(estimated, 1), maxThis);
      panelPxWidths.push(currentPx);
      remainingPx -= currentPx;
    }

    logStep(orderId, "sharp.panel.plan", {
      panelCount,
      panelWidthCm: Number(panelInfo.panelWidthCm.toFixed(4)),
      croppedWidthPx: safeCrop.width,
      croppedHeightPx: safeCrop.height,
      panelPxWidths,
    });

    let offsetLeft = 0;
    for (let i = 0; i < panelPxWidths.length; i++) {
      const panelWidthPx = panelPxWidths[i];
      const panelPath = path.join(orderDir, `panel-${i + 1}.png`);
      await sharp(masterPath, {
        sequentialRead: true,
        limitInputPixels: false,
      })
        .extract({
          left: safeCrop.left + offsetLeft,
          top: safeCrop.top,
          width: panelWidthPx,
          height: safeCrop.height,
        })
        .png({ compressionLevel: 0 })
        .toFile(panelPath);
      logStep(orderId, "sharp.panel.saved", {
        panelIndex: i + 1,
        panelWidthPx,
        panelPath,
      });
      offsetLeft += panelWidthPx;
    }

    logStep(orderId, "sharp.crop.completed", {
      durationMs: Date.now() - t0,
      panelCount,
    });

    logStep(orderId, "processing.done", { downloadPath: `/download/${orderId}` });
    doneOrders.add(orderId);
  } catch (err) {
    console.error(`[${orderId}] [processing.error]`, {
      message: err?.message,
      stack: err?.stack,
    });
  } finally {
    isProcessing = false;
    logStep(orderId, "processing.finally", {
      queueLengthAfter: queue.length,
      doneOrdersCount: doneOrders.size,
    });

    if (queue.length > 0) {
      const next = queue.shift();
      logStep(orderId, "queue.dequeue.next", { nextOrderId: next?.id, queueLengthNow: queue.length });
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
  logStep(orderId || "unknown", "webhook.received", {
    hasId: !!orderId,
    lineItemCount: (order?.line_items || []).length,
    financialStatus: order?.financial_status || null,
  });

  if (!orderId) return res.status(400).send("order.id missing");

  if (doneOrders.has(orderId)) {
    logStep(orderId, "webhook.skip.already_done");
    return res.status(200).send("ok");
  }

  if (isProcessing) {
    logStep(orderId, "webhook.queued.worker_busy", { queueLengthBefore: queue.length });
    queue.push(order);
    logStep(orderId, "webhook.queued", { queueLengthAfter: queue.length });
    return res.status(200).send("queued");
  }

  logStep(orderId, "webhook.accepted");
  processOrder(order);
  res.status(200).send("processing");
});

/**
 * DOWNLOAD ZIP
 */
app.get("/download/:orderId", (req, res) => {
  const { orderId } = req.params;
  const dir = path.join(BASE_DIR, orderId);
  logStep(orderId, "download.requested", { dir });

  if (!fs.existsSync(dir)) {
    logStep(orderId, "download.not_found");
    return res.status(404).send("Order artifacts not found");
  }

  res.setHeader("Content-Type", "application/zip");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename=wandini-${orderId}.zip`,
  );

  const archive = archiver("zip", { zlib: { level: 9 } });
  archive.pipe(res);
  logStep(orderId, "download.archive.start");

  archive.file(path.join(dir, "order.xml"), { name: "order.xml" });
  const panelFiles = fs
    .readdirSync(dir)
    .filter((name) => /^panel-\d+\.png$/.test(name))
    .sort((a, b) => Number(a.match(/\d+/)?.[0] || 0) - Number(b.match(/\d+/)?.[0] || 0));

  for (const fileName of panelFiles) {
    archive.file(path.join(dir, fileName), { name: fileName });
  }
  logStep(orderId, "download.archive.files", { count: panelFiles.length, files: panelFiles });

  archive.finalize();
  logStep(orderId, "download.archive.finalized");

  res.on("finish", () => {
    try {
      const dirSize = getDirSizeBytes(dir);
      fs.rmSync(dir, { recursive: true, force: true });
      doneOrders.delete(orderId);
      doneOrders.delete(Number(orderId));
      logStep(orderId, "tmp.cleanup.after_download_done", {
        deletedDir: dir,
        freedBytes: dirSize,
        freedHuman: formatBytes(dirSize),
        usageAfterCleanup: formatBytes(getTmpUsageBytes()),
      });
    } catch (err) {
      logStep(orderId, "tmp.cleanup.after_download_failed", {
        dir,
        message: err?.message || String(err),
      });
    }
  });
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
