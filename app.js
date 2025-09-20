const express = require("express");
const bodyParser = require("body-parser");
const crypto = require("crypto");
const AWS = require("aws-sdk");
const Jimp = require("jimp");
const client = require("prom-client");
const fetch = require("node-fetch");

// Load environment variables
const SPACES_ENDPOINT = process.env.SPACES_ENDPOINT || "https://lon1.digitaloceanspaces.com";
const BUCKET = process.env.SPACES_BUCKET || "quantumstream";
const REGION = process.env.SPACES_REGION || "lon1";
const DO_KEY = process.env.DO_SPACES_KEY;
const DO_SECRET = process.env.DO_SPACES_SECRET;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET;
const POWER_AUTOMATE_URL = process.env.POWER_AUTOMATE_URL;

if (!DO_KEY || !DO_SECRET || !WEBHOOK_SECRET) {
  console.error("Missing required environment variables!");
  process.exit(1);
}

// Configure DigitalOcean Spaces (S3-compatible)
const s3 = new AWS.S3({
  endpoint: SPACES_ENDPOINT,
  accessKeyId: DO_KEY,
  secretAccessKey: DO_SECRET,
  region: REGION,
});

// Prometheus setup
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

const app = express();
app.use(bodyParser.json({ limit: "10mb" }));

// Verify Dolby webhook signature
function verifySignature(req) {
  const signature = req.headers["x-dolby-signature"];
  const body = JSON.stringify(req.body);
  const hmac = crypto.createHmac("sha256", WEBHOOK_SECRET).update(body).digest("hex");
  return signature === hmac;
}

// Basic image check for black/corrupt
async function analyzeImage(buffer) {
  try {
    const image = await Jimp.read(buffer);
    let blackPixels = 0;
    const totalPixels = image.bitmap.width * image.bitmap.height;

    image.scan(0, 0, image.bitmap.width, image.bitmap.height, function (x, y, idx) {
      const red = this.bitmap.data[idx];
      const green = this.bitmap.data[idx + 1];
      const blue = this.bitmap.data[idx + 2];
      if (red < 10 && green < 10 && blue < 10) {
        blackPixels++;
      }
    });

    const blackRatio = blackPixels / totalPixels;
    if (blackRatio > 0.9) return "black";
    return "ok";
  } catch (err) {
    return "corrupt";
  }
}

// Receive Dolby thumbnail webhook
app.post("/thumbnail", async (req, res) => {
  if (!verifySignature(req)) {
    return res.status(401).send("Invalid signature");
  }

  try {
    const { feedId, streamId, timestamp, thumbnail } = req.body;
    const buffer = Buffer.from(thumbnail, "base64");

    // Analyze image
    const status = await analyzeImage(buffer);

    // Update Prometheus counters
    if (status === "ok") {
      okCounter.inc({ feedId, streamId });
    } else if (status === "black") {
      blackCounter.inc({ feedId, streamId });
    } else {
      corruptCounter.inc({ feedId, streamId });
    }

    // Upload thumbnail to Spaces
    const key = `${feedId}/${streamId}/${new Date(timestamp).toISOString()}.jpg`;
    await s3
      .putObject({
        Bucket: BUCKET,
        Key: key,
        Body: buffer,
        ACL: "public-read",
        ContentType: "image/jpeg",
      })
      .promise();

    console.log(`Uploaded ${key}, status=${status}`);

    // If bad frame, notify Power Automate
    if (status !== "ok" && POWER_AUTOMATE_URL) {
      await fetch(POWER_AUTOMATE_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          feedId,
          streamId,
          status,
          timestamp,
          file: key,
        }),
      });
      console.log(`Alert sent to Power Automate for ${status} frame`);
    }

    res.json({ success: true, status });
  } catch (err) {
    console.error("Error handling thumbnail:", err);
    res.status(500).send("Internal error");
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

const port = process.env.PORT || 8080;
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
