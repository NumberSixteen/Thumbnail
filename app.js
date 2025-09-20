// app.js
const express = require("express");
const bodyParser = require("body-parser");
const AWS = require("aws-sdk");
const Jimp = require("jimp");
const fetch = require("node-fetch");
const client = require("prom-client");

const app = express();
app.use(bodyParser.raw({ type: "image/jpeg", limit: "5mb" }));

// === DigitalOcean Spaces config ===
const s3 = new AWS.S3({
  endpoint: new AWS.Endpoint("https://lon1.digitaloceanspaces.com"),
  accessKeyId: process.env.SPACES_KEY,
  secretAccessKey: process.env.SPACES_SECRET,
});
const BUCKET = "quantumstream";

// === Prometheus setup ===
const register = new client.Registry();
client.collectDefaultMetrics({ register });

const okCounter = new client.Counter({
  name: "thumbnails_ok_total",
  help: "Total number of OK thumbnails",
  labelNames: ["feedId", "streamId"],
});
const blackCounter = new client.Counter({
  name: "thumbnails_black_total",
  help: "Total number of black thumbnails",
  labelNames: ["feedId", "streamId"],
});
const corruptCounter = new client.Counter({
  name: "thumbnails_corrupt_total",
  help: "Total number of corrupt thumbnails",
  labelNames: ["feedId", "streamId"],
});
register.registerMetric(okCounter);
register.registerMetric(blackCounter);
register.registerMetric(corruptCounter);

// === Helpers ===
async function isBlackFrame(buffer) {
  const image = await Jimp.read(buffer);
  let total = 0;
  image.scan(0, 0, image.bitmap.width, image.bitmap.height, (x, y, idx) => {
    const r = image.bitmap.data[idx + 0];
    const g = image.bitmap.data[idx + 1];
    const b = image.bitmap.data[idx + 2];
    total += (r + g + b) / 3;
  });
  const avg = total / (image.bitmap.width * image.bitmap.height);
  return avg < 10; // tweak threshold
}

async function logResult(feedId, streamId, status, extra = {}) {
  const ts = new Date().toISOString();
  const date = ts.split("T")[0]; // YYYY-MM-DD
  const logKey = `logs/${date}.json`;

  let logs = [];
  try {
    const logFile = await s3.getObject({ Bucket: BUCKET, Key: logKey }).promise();
    logs = JSON.parse(logFile.Body.toString());
  } catch (err) {
    if (err.code !== "NoSuchKey") throw err;
  }

  logs.push({ ts, feedId, streamId, status, ...extra });

  await s3
    .putObject({
      Bucket: BUCKET,
      Key: logKey,
      Body: JSON.stringify(logs, null, 2),
      ContentType: "application/json",
      ACL: "private",
    })
    .promise();
}

async function notifyPowerAutomate(feedId, streamId, status, key) {
  if (!process.env.POWER_AUTOMATE_WEBHOOK) return;

  const payload = {
    feedId,
    streamId,
    status,
    thumbnail: key,
    timestamp: new Date().toISOString(),
  };

  try {
    await fetch(process.env.POWER_AUTOMATE_WEBHOOK, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    console.log("Notification sent to Power Automate");
  } catch (err) {
    console.error("Failed to send notification:", err.message);
  }
}

// === Routes ===
app.post("/thumbnail", async (req, res) => {
  const feedId = req.header("X-Millicast-Feed-Id") || "unknown";
  const streamId = req.header("X-Millicast-Stream-Id") || "unknown";
  const ts = req.header("X-Millicast-Timestamp") || Date.now().toString();

  let status = "ok";
  const path = `${feedId}/${streamId}/${new Date(parseInt(ts, 10)).toISOString()}.jpg`;

  try {
    // Validate image
    try {
      await Jimp.read(req.body);
    } catch (err) {
      status = "corrupt";
    }

    if (status !== "corrupt") {
      const isBlack = await isBlackFrame(req.body);
      if (isBlack) status = "black";
    }

    // Upload thumbnail (always)
    await s3
      .putObject({
        Bucket: BUCKET,
        Key: path,
        Body: req.body,
        ACL: "private",
        ContentType: "image/jpeg",
      })
      .promise();

    // Update Prometheus metrics
    if (status === "ok") {
      okCounter.inc({ feedId, streamId });
    } else if (status === "black") {
      blackCounter.inc({ feedId, streamId });
    } else if (status === "corrupt") {
      corruptCounter.inc({ feedId, streamId });
    }

    // Log status
    await logResult(feedId, streamId, status, { key: path });

    // Notify Power Automate if bad
    if (status === "black" || status === "corrupt") {
      await notifyPowerAutomate(feedId, streamId, status, path);
    }

    res.json({ message: "Uploaded", key: path, status });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Upload failed", details: err.message });
  }
});

app.get("/logs/:date", async (req, res) => {
  const { date } = req.params; // YYYY-MM-DD
  const logKey = `logs/${date}.json`;

  try {
    const logFile = await s3.getObject({ Bucket: BUCKET, Key: logKey }).promise();
    const logs = JSON.parse(logFile.Body.toString());
    res.json(logs);
  } catch (err) {
    if (err.code === "NoSuchKey") {
      return res.status(404).json({ error: "No logs for that date" });
    }
    console.error(err);
    res.status(500).json({ error: "Failed to fetch logs", details: err.message });
  }
});

// Prometheus metrics endpoint
app.get("/metrics", async (req, res) => {
  try {
    res.set("Content-Type", register.contentType);
    res.end(await register.metrics());
  } catch (err) {
    res.status(500).end(err.message);
  }
});

// === Start server ===
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Thumbnail service running on port ${PORT}`);
});
