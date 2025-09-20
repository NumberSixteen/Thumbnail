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
  labelNames: ["feedId", "streamId"],
});
register.registerMetric(okCounter);

const latestGauge = new client.Gauge({
  name: "thumbnail_latest_timestamp_seconds",
  help: "Unix timestamp of the latest thumbnail uploaded",
  labelNames: ["feedId", "streamId"],
});
register.registerMetric(latestGauge);

const latestUrlGauge = new client.Gauge({
  name: "thumbnail_latest_url_info",
  help: "Dummy gauge carrying latest URL as a label",
  labelNames: ["feedId", "streamId", "url"],
});
register.registerMetric(latestUrlGauge);

const app = express();

// Dolby webhook: JSON with base64 thumbnail
app.use(bodyParser.json({ limit: "10mb" }));

// Raw JPEG (Postman test)
app.use("/thumbnail", express.raw({ type: "image/jpeg", limit: "10mb" }));

// === Routes ===
app.post("/thumbnail", async (req, res) => {
  try {
    let buffer;
    let feedId, streamId, timestamp;

    if (req.is("application/json")) {
      // Dolby webhook JSON
      const { feedId: f, streamId: s, timestamp: t, thumbnail } = req.body;
      if (!thumbnail) {
        return res.status(400).json({ error: "Missing thumbnail in JSON body" });
      }
      buffer = Buffer.from(thumbnail, "base64");
      feedId = f || "unknown";
      streamId = s || "unknown";
      timestamp = t || Date.now();
    } else if (req.is("image/jpeg")) {
      // Raw JPEG (Postman)
      buffer = req.body;
      feedId = req.header("X-Millicast-Feed-Id") || "testfeed";
      streamId = req.header("X-Millicast-Stream-Id") || "teststream";
      timestamp = req.header("X-Millicast-Timestamp") || Date.now();
    } else {
      return res.status(400).json({ error: "Unsupported Content-Type" });
    }

    const key = `${feedId}/${streamId}/${new Date(parseInt(timestamp, 10)).toISOString()}.jpg`;

    await s3
      .putObject({
        Bucket: BUCKET,
        Key: key,
        Body: buffer,
        ACL: "public-read", // public for testing
        ContentType: "image/jpeg",
      })
      .promise();

    // Prometheus counters
    okCounter.inc({ feedId, streamId });
    latestGauge.set({ feedId, streamId }, Math.floor(Date.now() / 1000));
    latestUrlGauge.set({ feedId, streamId, url: `https://${BUCKET}.lon1.digitaloceanspaces.com/${key}` }, 1);

    const url = `https://${BUCKET}.lon1.digitaloceanspaces.com/${key}`;
    res.json({ message: "Uploaded", key, url });
  } catch (err) {
    console.error("Upload failed:", err);
    res.status(500).json({ error: "Upload failed", details: err.message });
  }
});

// === New route: Get latest thumbnail for a feed/stream ===
app.get("/streams/:feedId/:streamId/latest", async (req, res) => {
  const { feedId, streamId } = req.params;

  try {
    const prefix = `${feedId}/${streamId}/`;

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

    // Find latest by LastModified
    const latest = objects.Contents.reduce((a, b) =>
      new Date(a.LastModified) > new Date(b.LastModified) ? a : b
    );

    const url = `https://${BUCKET}.lon1.digitaloceanspaces.com/${latest.Key}`;
    res.json({ feedId, streamId, latest: url });
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
