const express = require("express");
const bodyParser = require("body-parser");
const AWS = require("aws-sdk");
const client = require("prom-client");

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
  labelNames: ["feedId", "streamId", "quality"],
});
register.registerMetric(okCounter);

const app = express();

// Dolby webhook: JSON with base64 thumbnail
app.use(bodyParser.json({ limit: "10mb" }));

// Postman test OR Dolby sending raw JPEG
app.use("/thumbnail", express.raw({ type: "image/jpeg", limit: "10mb" }));

// === Helper: quality mapping ===
function mapResolutionToQuality(width, height) {
  if (width === 854 && height === 480) return "high";
  if (width === 640 && height === 360) return "med";
  if (width === 426 && height === 240) return "low";
  return "unknown";
}

// === Routes ===
app.post("/thumbnail", async (req, res) => {
  try {
    let buffer;
    let feedId, streamId, timestamp, width, height;

    if (req.is("application/json")) {
      // Dolby webhook JSON
      const { feedId: f, streamId: s, timestamp: t, thumbnail, width: w, height: h } = req.body;
      if (!thumbnail) {
        return res.status(400).json({ error: "Missing thumbnail in JSON body" });
      }
      buffer = Buffer.from(thumbnail, "base64");
      feedId = f || req.header("X-Millicast-Feed-Id") || "unknown";
      streamId = s || req.header("X-Millicast-Stream-Id") || "unknown";
      timestamp = t || req.header("X-Millicast-Timestamp") || Date.now();
      width = w;
      height = h;
    } else if (req.is("image/jpeg")) {
      // Raw JPEG (Postman test OR Dolby binary mode)
      buffer = req.body;
      feedId = req.header("X-Millicast-Feed-Id") || "testfeed";
      streamId = req.header("X-Millicast-Stream-Id") || "teststream";
      timestamp = req.header("X-Millicast-Timestamp") || Date.now();
      width = parseInt(req.header("X-Thumbnail-Width")) || null;
      height = parseInt(req.header("X-Thumbnail-Height")) || null;
    } else {
      return res.status(400).json({ error: "Unsupported Content-Type" });
    }

    const quality = mapResolutionToQuality(width, height);
    const key = `${feedId}/${streamId}/${quality}/${new Date(parseInt(timestamp, 10)).toISOString()}.jpg`;

    await s3
      .putObject({
        Bucket: BUCKET,
        Key: key,
        Body: buffer,
        ACL: "public-read", // public for testing
        ContentType: "image/jpeg",
      })
      .promise();

    okCounter.inc({ feedId, streamId, quality });

    const url = `https://${BUCKET}.lon1.digitaloceanspaces.com/${key}`;
    res.json({ message: "Uploaded", key, quality, url });
  } catch (err) {
    console.error("Upload failed:", err);
    res.status(500).json({ error: "Upload failed", details: err.message });
  }
});

// === New route: Get latest thumbnails per quality ===
app.get("/streams/:feedId/:streamId/latest", async (req, res) => {
  const { feedId, streamId } = req.params;

  try {
    const qualities = ["high", "med", "low"];
    const results = {};

    for (const q of qualities) {
      const prefix = `${feedId}/${streamId}/${q}/`;

      const objects = await s3
        .listObjectsV2({
          Bucket: BUCKET,
          Prefix: prefix,
          MaxKeys: 1,
          StartAfter: prefix,
        })
        .promise();

      if (objects.Contents && objects.Contents.length > 0) {
        // Objects are sorted by LastModified ascending
        const latest = objects.Contents.reduce((a, b) =>
          new Date(a.LastModified) > new Date(b.LastModified) ? a : b
        );
        results[q] = `https://${BUCKET}.lon1.digitaloceanspaces.com/${latest.Key}`;
      } else {
        results[q] = null;
      }
    }

    res.json({ feedId, streamId, latest: results });
  } catch (err) {
    console.error("Failed to fetch latest thumbnails:", err);
    res.status(500).json({ error: "Failed to fetch latest thumbnails", details: err.message });
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
