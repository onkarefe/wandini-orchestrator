import express from "express";
import fs from "fs";
import path from "path";
import https from "https";
import archiver from "archiver";
import sharp from "sharp";
import { PDFDocument } from "pdf-lib";

/**
 * SHARP GLOBAL AYARLAR
 */
sharp.cache(false);
sharp.concurrency(1);

const app = express();
app.use(express.json({ limit: "100mb" }));

const PORT = process.env.PORT || 10000;
const BASE_DIR = "/tmp/wandini";
const MAX_PANEL_CM = 70;
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || null;
const ARTIFACT_TTL_MS = 2 * 60 * 60 * 1000; // 2 hours
const SUPPRESSED_STEPS = new Set([
  "configurator.payload",
  "sharp.metadata",
  "sharp.crop.calculated",
  "sharp.panel.plan",
  "zip.panel.appended",
  "tmp.budget.check",
  "tmp.budget.cleanup_needed",
  "tmp.budget.after_cleanup",
  "tmp.usage.check",
]);

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

function xmlEscape(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function logStep(orderId, step, details = null) {
  if (SUPPRESSED_STEPS.has(step)) return;

  if (details === null || details === undefined) {
    console.log(`[${orderId}] [${step}]`);
    return;
  }

  const keepDetails =
    step.includes("error") ||
    step.includes("failed") ||
    step.endsWith(".done") ||
    step === "processing.done" ||
    step === "download.stream.start" ||
    step === "download.not_found";

  if (keepDetails) {
    console.log(`[${orderId}] [${step}]`, details);
  } else {
    console.log(`[${orderId}] [${step}]`);
  }
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

const PT_PER_MM = 72 / 25.4;
function mmToPt(mm) {
  return Number(mm) * PT_PER_MM;
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

function parseConfiguratorPayload(order) {
  for (const item of order.line_items || []) {
    for (const p of item.properties || []) {
      if (p.name === "configurator_payload" && p.value) {
        try {
          return JSON.parse(p.value);
        } catch {
          return null;
        }
      }
    }
  }
  return null;
}

function isValidCropRatio(cropRatio) {
  if (!cropRatio || typeof cropRatio !== "object") return false;
  const { x, y, w, h } = cropRatio;
  if (![x, y, w, h].every((v) => Number.isFinite(v))) return false;
  if (x < 0 || y < 0 || w <= 0 || h <= 0) return false;
  if (x > 1 || y > 1 || w > 1 || h > 1) return false;
  if (x + w > 1.000001 || y + h > 1.000001) return false;
  return true;
}

function buildPanelPixelWidths(totalWidthPx, panelCount) {
  const widths = [];
  let remainingPx = totalWidthPx;
  for (let i = 0; i < panelCount; i++) {
    const last = i === panelCount - 1;
    if (last) {
      widths.push(Math.max(1, remainingPx));
      break;
    }
    const remainingPieces = panelCount - i;
    const minReservedForRest = remainingPieces - 1;
    const maxThis = Math.max(1, remainingPx - minReservedForRest);
    const estimated = Math.round(totalWidthPx / panelCount);
    const currentPx = Math.min(Math.max(estimated, 1), maxThis);
    widths.push(currentPx);
    remainingPx -= currentPx;
  }
  return widths;
}

function createSingleZipFromPlan(
  orderId,
  zipPath,
  xmlPath,
  masterPath,
  safeCrop,
  panelPxWidths,
  outputWidthMm,
  outputHeightMm,
) {
  return new Promise((resolve, reject) => {
    const output = fs.createWriteStream(zipPath);
    const archive = archiver("zip", { zlib: { level: 9 } });

    output.on("close", () => {
      logStep(orderId, "zip.created", {
        zipPath,
        bytes: archive.pointer(),
        bytesHuman: formatBytes(archive.pointer()),
      });
      resolve();
    });
    output.on("error", reject);
    archive.on("error", reject);

    archive.pipe(output);
    if (fs.existsSync(xmlPath)) {
      archive.file(xmlPath, { name: "order.xml" });
    }

    (async () => {
      try {
        let offsetLeft = 0;
        const panelCount = panelPxWidths.length || 1;
        const panelWidthMm = Number(outputWidthMm) / panelCount;
        const panelHeightMm = Number(outputHeightMm);
        const pageWidthPt = mmToPt(panelWidthMm);
        const pageHeightPt = mmToPt(panelHeightMm);

        for (let i = 0; i < panelPxWidths.length; i++) {
          const panelWidthPx = panelPxWidths[i];
          const panelName = `panel-${i + 1}.pdf`;

          const panelPngBuffer = await sharp(masterPath, {
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
            .toBuffer();

          const pdfDoc = await PDFDocument.create();
          const embeddedImage = await pdfDoc.embedPng(panelPngBuffer);
          const page = pdfDoc.addPage([pageWidthPt, pageHeightPt]);
          page.drawImage(embeddedImage, {
            x: 0,
            y: 0,
            width: pageWidthPt,
            height: pageHeightPt,
          });

          const panelPdfBytes = await pdfDoc.save({ useObjectStreams: false });
          archive.append(Buffer.from(panelPdfBytes), { name: panelName });
          logStep(orderId, "zip.panel.appended", {
            panelIndex: i + 1,
            panelWidthPx,
            panelName,
          });

          offsetLeft += panelWidthPx;
        }

        archive.finalize();
      } catch (err) {
        reject(err);
      }
    })();
  });
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
        console.log(`[download] started`, {
          url,
          statusCode: res.statusCode,
          dest,
        });
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
  const orderNumber = order?.id ?? "";

  const shippingType = "Standard"; // MANUAL (değiştirilebilir)

  const shippingFrom = order?.billing_address || {};
  const shippingTo = order?.shipping_address || {};

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
        let cfg = null;
        try {
          cfg = JSON.parse(p.value);
        } catch {
          cfg = null;
        }
        if (!cfg) continue;
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
    <order_number>${xmlEscape(orderNumber)}</order_number>
    <reference></reference>
    <shipping_type>${shippingType}</shipping_type>

    <shipping_from>
      <company>${xmlEscape(shippingFrom.company || shippingFrom.name || "")}</company>
      <contact_person>${xmlEscape(shippingFrom.name || "")}</contact_person>
      <street>${xmlEscape(shippingFrom.address1 || "")}</street>
      <postcode>${xmlEscape(shippingFrom.zip || "")}</postcode>
      <city>${xmlEscape(shippingFrom.city || "")}</city>
      <country>${xmlEscape(shippingFrom.country_code || "")}</country>
    </shipping_from>

    <shipping_to>
      <company>${xmlEscape(shippingTo.company || shippingTo.name || "")}</company>
      <contact_person>${xmlEscape(shippingTo.name || "")}</contact_person>
      <street>${xmlEscape(shippingTo.address1 || "")}</street>
      <postcode>${xmlEscape(shippingTo.zip || "")}</postcode>
      <city>${xmlEscape(shippingTo.city || "")}</city>
      <country>${xmlEscape(shippingTo.country_code || "")}</country>
      <phone>${xmlEscape(shippingTo.phone || "")}</phone>
    </shipping_to>

    <delivery_note type="ftp"></delivery_note>
  </order>

  <positions>
    <position>
      <sku>${xmlEscape(sku)}</sku>
      <width unit="mm">${Number.isFinite(Number(outputWidth)) ? Number(outputWidth) : 0}</width>
      <height unit="mm">${Number.isFinite(Number(outputHeight)) ? Number(outputHeight) : 0}</height>
      <variants>1</variants>
      <copies_per_variant>${Number.isFinite(Number(quantity)) ? Number(quantity) : 1}</copies_per_variant>
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
    const outputWidthMmNum = Number(outputWidthMm);
    const outputHeightMmNum = Number(outputHeightMm);

    const panelInfo = computePanelsFromOutputMm(outputWidthMm);
    logStep(orderId, "configurator.payload", {
      version: cfg?.version ?? null,
      masterAssetId,
      output: {
        widthMm: outputWidthMm,
        heightMm: outputHeightMm,
        unit: cfg?.output?.unit,
      },
      cropRatio,
      panelCount: panelInfo.panelCount,
      panelWidthCm: Number(panelInfo.panelWidthCm.toFixed(4)),
    });

    if (!masterAssetId || !cropRatio) {
      logStep(orderId, "processing.skip", "Missing configurator data");
      return;
    }
    if (!isValidCropRatio(cropRatio)) {
      logStep(orderId, "processing.skip", "Invalid crop_ratio");
      return;
    }
    if (!Number.isFinite(outputWidthMmNum) || outputWidthMmNum <= 0) {
      logStep(orderId, "processing.skip", "Invalid output.width");
      return;
    }
    if (!Number.isFinite(outputHeightMmNum) || outputHeightMmNum <= 0) {
      logStep(orderId, "processing.skip", "Invalid output.height");
      return;
    }

    const orderDir = path.join(BASE_DIR, String(orderId));
    fs.mkdirSync(orderDir, { recursive: true });
    logStep(orderId, "fs.orderDir.ready", { orderDir });

    const masterUrl = `https://storage.googleapis.com/wandini-masters/${masterAssetId}/master.png`;
    const masterPath = path.join(orderDir, "master.png");
    const xmlPath = path.join(orderDir, "order.xml");
    const zipPath = path.join(orderDir, `wandini-${orderId}.zip`);
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
      width: Math.max(
        1,
        Math.min(
          crop.width,
          Math.max(
            1,
            meta.width -
              Math.max(0, Math.min(crop.left, Math.max(0, meta.width - 1))),
          ),
        ),
      ),
      height: Math.max(
        1,
        Math.min(
          crop.height,
          Math.max(
            1,
            meta.height -
              Math.max(0, Math.min(crop.top, Math.max(0, meta.height - 1))),
          ),
        ),
      ),
    };
    logStep(orderId, "sharp.crop.calculated", { requested: crop, safeCrop });
    const panelCount = panelInfo.panelCount || 1;
    const panelPxWidths = buildPanelPixelWidths(safeCrop.width, panelCount);

    logStep(orderId, "sharp.panel.plan", {
      panelCount,
      panelWidthCm: Number(panelInfo.panelWidthCm.toFixed(4)),
      croppedWidthPx: safeCrop.width,
      croppedHeightPx: safeCrop.height,
      panelPxWidths,
    });

    logStep(orderId, "sharp.crop.completed", {
      durationMs: Date.now() - t0,
      panelCount,
    });

    await createSingleZipFromPlan(
      orderId,
      zipPath,
      xmlPath,
      masterPath,
      safeCrop,
      panelPxWidths,
      outputWidthMmNum,
      outputHeightMmNum,
    );
    try {
      if (fs.existsSync(masterPath)) fs.unlinkSync(masterPath);
      if (fs.existsSync(xmlPath)) fs.unlinkSync(xmlPath);
      logStep(orderId, "tmp.cleanup.source_deleted", {
        masterPath,
        xmlPath,
        zipPath,
      });
    } catch (cleanupErr) {
      logStep(orderId, "tmp.cleanup.source_delete_failed", {
        message: cleanupErr?.message || String(cleanupErr),
      });
    }

    const downloadPath = `/download/${orderId}`;
    const downloadUrl = PUBLIC_BASE_URL
      ? `${String(PUBLIC_BASE_URL).replace(/\/+$/, "")}${downloadPath}`
      : null;
    logStep(orderId, "processing.done", { downloadPath, downloadUrl });
    doneOrders.add(orderId);
  } catch (err) {
    console.error(`[${orderId}] [processing.error]`, {
      message: err?.message,
      stack: err?.stack,
    });
    try {
      const orderDir = path.join(BASE_DIR, String(orderId));
      if (fs.existsSync(orderDir)) {
        const dirSize = getDirSizeBytes(orderDir);
        fs.rmSync(orderDir, { recursive: true, force: true });
        logStep(orderId, "tmp.cleanup.after_error_done", {
          removedDir: orderDir,
          freedBytes: dirSize,
          freedHuman: formatBytes(dirSize),
        });
      }
    } catch (cleanupErr) {
      logStep(orderId, "tmp.cleanup.after_error_failed", {
        message: cleanupErr?.message || String(cleanupErr),
      });
    }
  } finally {
    isProcessing = false;
    logStep(orderId, "processing.finally", {
      queueLengthAfter: queue.length,
      doneOrdersCount: doneOrders.size,
    });

    if (queue.length > 0) {
      const next = queue.shift();
      logStep(orderId, "queue.dequeue.next", {
        nextOrderId: next?.id,
        queueLengthNow: queue.length,
      });
      processOrder(next);
    }
  }
}

/**
 * WEBHOOK
 */
app.post("/webhooks/orders-paid", (req, res) => {
  const order = req.body ?? {};
  const orderId = order?.id;
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
    logStep(orderId, "webhook.queued.worker_busy", {
      queueLengthBefore: queue.length,
    });
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
  const zipPath = path.join(dir, `wandini-${orderId}.zip`);
  logStep(orderId, "download.requested", { dir });

  if (!fs.existsSync(dir) || !fs.existsSync(zipPath)) {
    logStep(orderId, "download.not_found");
    return res.status(404).send("Order artifacts not found");
  }
  logStep(orderId, "download.stream.start", { zipPath });
  res.download(zipPath, `wandini-${orderId}.zip`, (err) => {
    if (err) {
      logStep(orderId, "download.stream.error", {
        message: err?.message || String(err),
      });
      return;
    }
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
    } catch (cleanupErr) {
      logStep(orderId, "tmp.cleanup.after_download_failed", {
        dir,
        message: cleanupErr?.message || String(cleanupErr),
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
