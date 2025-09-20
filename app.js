// app.js
const express = require("express");
const bodyParser = require("body-parser");
const AWS = require("aws-sdk");
const client = require("prom-client");

// === Environment variables ===
// DO_SPACES_KEY, DO_SPACES_SECRET, WEBHOOK_SECRET must be set in App Platform
const SPACES_ENDPOINT = process.env.SPACES_ENDPOINT || "https://lon1.digitaloceanspaces.com";
const BUCKET = process.env.SPACES_BUCKET || "quantumstream";
const REGION = process.env.SPACES_REGION || "lon1";

// === Configure DigitalOcean Spaces ===
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
  help: "Total number of thumbnails received and uploaded",
  labelNames: ["feedId", "streamId"],
});
register.registerMetric(okCounter);

const app = express();
app.use(bodyParser.json({ limit: "10mb" })); // Dolby sends JSON with base64 thumbnail

// === Routes ===
app.post("/thumbnail", async (req, res) => {
  try {
    const { feedId, streamId, timestamp, thumbnail } = req.body;

    if (!thumbnail) {
      return res.status(400).json({ error: "Missing thumbnail in request body" });
    }

    const buffer = Buffer.from(thumbnail, "base64");
    const key = `${feedId || "unknown"}/${streamId || "unknown"}/${new Date(timestamp || Date.now()).toISOString()}.jpg`;

    await s3
      .putObject({
        Bucket: BUCKET,
        Key: key,
        Body: buffer,
        ACL: "public-read", // make public for testing
        ContentType: "image/jpeg",
      })
      .promise();

    // Increment Prometheus counter
    okCounter.inc({ feedId: feedId || "unknown", streamId: streamId || "unknown" });

    const url = `https://${BUCKET}.lon1.digitaloceanspaces.com/${key}`;

    res.json({ message: "Uploaded", key, url });
  } catch (err) {
    console.error("Upload failed:", err);
    res.status(500).json({ error: "Upload failed", details: err.message });
  }
});

app.get("/metrics", async (req, res) => {
  try {
    res.set("Content-Type", register.contentType);
    res.end(await register.metrics());
  } catch (err) {
    res.status(500).end(err.message);
  }
});

// === Start server ===
const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Thumbnail service running on port ${port}`);
});
