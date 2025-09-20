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

const app = express();

// Dolby webhook: JSON with base64 thumbnail
app.use(bodyParser.json({ limit: "10mb" }));

// Postman test OR Dolby sending raw JPEG
app.use("/thumbnail", express.raw({ type: "image/jpeg", limit: "10mb" }));

app.post("/thumbnail", async (req, res) => {
  try {
    let buffer;
    let feedId, streamId, timestamp;

    if (req.is("application/json")) {
      // Dolby webhook format
      const { feedId: f, streamId: s, timestamp: t, thumbnail } = req.body;
      if (!thumbnail) {
        return res.status(400).json({ error: "Missing thumbnail in JSON body" });
      }
      buffer = Buffer.from(thumbnail, "base64");
      feedId = f || req.header("X-Millicast-Feed-Id") || "unknown";
      streamId = s || req.header("X-Millicast-Stream-Id") || "unknown";
      timestamp = t || req.header("X-Millicast-Timestamp") || Date.now();
    } else if (req.is("image/jpeg")) {
      // Raw JPEG (Postman OR Dolby binary mode)
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
        ACL: "public-read", // public for easy testing
        ContentType: "image/jpeg",
      })
      .promise();

    okCounter.inc({ feedId, streamId });

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

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Thumbnail service running on port ${port}`);
});
