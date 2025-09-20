const express = require("express");
const bodyParser = require("body-parser");
const AWS = require("aws-sdk");
const client = require("prom-client");
const Jimp = require("jimp");
const crypto = require("crypto");

const SPACES_ENDPOINT = process.env.SPACES_ENDPOINT || "https://lon1.digitaloceanspaces.com";
const BUCKET = process.env.SPACES_BUCKET || "quantumstream";
const REGION = process.env.SPACES_REGION || "lon1";

const s3 = new AWS.S3({
  endpoint: SPACES_ENDPOINT,
  accessKeyId: process.env.DO_SPACES_KEY,
  secretAccessKey: process.env.DO_SPACES_SECRET,
  region: REGION,
});

// === Prometheus setup ===
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const okCounter = new client.Counter({
  name: "thumbnails_ok_total",
  help: "Total number of thumbnails uploaded",
  labelNames: ["streamId", "feedId"],
});
register.registerMetric(okCounter);

const freezeCounter = new client.Counter({
  name: "thumbnails_freeze_total",
  help: "Total number of detected freeze frames",
  labelNames: ["streamId", "feedId"],
});
register.registerMetric(freezeCounter);

const latestGauge = new client.Gauge({
  name: "thumbnail_latest_timestamp_seconds",
  help: "Unix timestamp of the latest thumbnail uploaded",
  labelNames: ["streamId", "feedId"],
});
register.registerMetric(latestGauge);

// === Memory store for freeze detection ===
const lastFrames = new Map();

const app = express();
app.use(bodyParser.json({ limit: "10mb" }));
app.use("/thumbnail", express.raw({ type: "image/jpeg", limit: "10mb" }));

// === Helper: hash an image for quick compare ===
async function hashImage(buffer) {
  const img = await Jimp.read(buffer);
  return img.hash(); // perceptual hash
}

// === Routes ===
app.post("/thumbnail", async (req, res) => {
  try {
    let buffer;
    let feedId, streamId, timestamp;

    if (req.is("application/json")) {
      const { feedId: f, streamId: s, timestamp: t, thumbnail } = req.body;
      if (!thumbnail) {
        return res.status(400).json({ error: "Missing thumbnail in JSON body" });
      }
      buffer = Buffer.from(thumbnail, "base64");
      feedId = f || "unknown";
      streamId = s || "unknown";
      timestamp = t || Date.now();
    } else if (req.is("image/jpeg")) {
      buffer = req.body;
      feedId = req.header("X-Millicast-Feed-Id") || "testfeed";
      streamId = req.header("X-Millicast-Stream-Id") || "teststream";
      timestamp = req.header("X-Millicast-Timestamp") || Date.now();
    } else {
      return res.status(400).json({ error: "Unsupported Content-Type" });
    }

    const key = `${streamId}/${feedId}/${new Date(parseInt(timestamp, 10)).toISOString()}.jpg`;

    // Freeze detection
    let isFreeze = false;
    try {
      const newHash = await hashImage(buffer);
      const lastKey = `${streamId}:${feedId}`;
      if (lastFrames.has(lastKey)) {
        const prevHash = lastFrames.get(lastKey);
        if (newHash === prevHash) {
          isFreeze = true;
          freezeCounter.inc({ streamId, feedId });
        }
      }
      lastFrames.set(lastKey, newHash);
    } catch (err) {
      console.error("Freeze detection failed:", err.message);
    }

    // Upload thumbnail to Spaces
    await s3
      .putObject({
        Bucket: BUCKET,
        Key: key,
        Body: buffer,
        ACL: "public-read",
        ContentType: "image/jpeg",
      })
      .promise();

    okCounter.inc({ streamId, feedId });
    latestGauge.set({ streamId, feedId }, Math.floor(Date.now() / 1000));

    const url = `https://${BUCKET}.lon1.digitaloceanspaces.com/${key}`;
    res.json({ message: "Uploaded", key, url, freeze: isFreeze });
  } catch (err) {
    console.error("Upload failed:", err);
    res.status(500).json({ error: "Upload failed", details: err.message });
  }
});

// === New route: Get latest thumbnail for a stream/feed ===
app.get("/streams/:streamId/:feedId/latest", async (req, res) => {
  const { streamId, feedId } = req.params;

  try {
    const prefix = `${streamId}/${feedId}/`;

    const objects = await s3
      .listObjectsV2({
        Bucket: BUCKET,
        Prefix: prefix,
        MaxKeys: 100,
      })
      .promise();

    if (!objects.Contents || objects.Contents.length === 0) {
      return res.status(404).json({ error: "No thumbnails found" });
    }

    const latest = objects.Contents.reduce((a, b) =>
      new Date(a.LastModified) > new Date(b.LastModified) ? a : b
    );

    const url = `https://${BUCKET}.lon1.digitaloceanspaces.com/${latest.Key}`;
    res.json({ streamId, feedId, latest: url });
  } catch (err) {
    console.error("Failed to fetch latest thumbnail:", err);
    res.status(500).json({ error: "Failed to fetch latest thumbnail", details: err.message });
  }
});

// === Prometheus metrics endpoint ===
app.get("/metrics", async (req, res) => {
  try {
    res.set("Content-Type", register.contentType);
    res.end(await register.metrics());
  } catch (err) {
    res.status(500).end(err.message);
  }
});

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Thumbnail service running on port ${port}`);
});
