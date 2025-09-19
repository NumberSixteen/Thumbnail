const express = require("express");
const AWS = require("aws-sdk");
const crypto = require("crypto");

// Configure Spaces (London region)
const s3 = new AWS.S3({
  endpoint: "https://lon1.digitaloceanspaces.com",
  accessKeyId: process.env.DO_SPACES_KEY,
  secretAccessKey: process.env.DO_SPACES_SECRET,
});

const app = express();

// Parse raw binary for JPEG thumbnails
app.use("/thumbnail", express.raw({ type: "image/jpeg", limit: "10mb" }));

app.post("/thumbnail", async (req, res) => {
  try {
  //   // Verify HMAC signature
  //   const signature = req.header("X-Millicast-Signature");
  //   const hmac = crypto
  //     .createHmac("sha1", process.env.WEBHOOK_SECRET)
  //     .update(req.body)
  //     .digest("hex");

  //   if (hmac !== signature) {
  //     return res.status(401).json({ error: "Invalid signature" });
  //   }

    // Metadata headers
    const ts = req.header("X-Millicast-Timestamp");
    const feedId = req.header("X-Millicast-Feed-Id");
    const streamId = req.header("X-Millicast-Stream-Id");

    const date = new Date(parseInt(ts, 10));
    const path = `${feedId}/${streamId}/${date.getUTCFullYear()}/${
      String(date.getUTCMonth() + 1).padStart(2, "0")
    }/${String(date.getUTCDate()).padStart(2, "0")}/${date
      .toISOString()
      .slice(11, 19)
      .replace(/:/g, "")}.jpg`;

    // Upload thumbnail to Spaces
    await s3
      .putObject({
        Bucket: "quantumstream",
        Key: path,
        Body: req.body,
        ACL: "private", // use "public-read" if you want public URLs
        ContentType: "image/jpeg",
      })
      .promise();

    // Optional: build public URL (if ACL=public-read)
    const publicUrl = `https://quantumstream.lon1.digitaloceanspaces.com/${path}`;

    return res.json({
      message: "Uploaded",
      key: path,
      url: publicUrl, // only valid if objects are public
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Upload failed" });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Thumbnail API running on port ${port}`));
