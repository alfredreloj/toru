/**
 * Toru — anonymous file sharing by link.
 *
 * Upload documents / images / audio / video, get one short link, share it.
 * No login, no email. Every link (and its files) auto-deletes 24h after upload.
 *
 * Storage is DigitalOcean Spaces (S3-compatible), so files survive app
 * restarts and redeploys. Object layout, one prefix per link:
 *   <linkId>/meta.json        link metadata + file manifest
 *   <linkId>/<fileId>         each stored file (original name kept in meta)
 *
 * Actual deletion is enforced by a bucket lifecycle rule (expire after 1 day,
 * set by setup-bucket.mjs); meta.expiresAt is the exact 24h mark shown in the UI
 * and used to stop serving a link the moment it lapses.
 */

import "dotenv/config";
import express from "express";
import multer from "multer";
import archiver from "archiver";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import fsp from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectsCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";

// ── Config ────────────────────────────────────────────────────────────────
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 4800;
const PUBLIC_DIR = path.join(__dirname, "public");

const MAX_TOTAL_BYTES = 500 * 1024 * 1024; // 500 MB total per link (all files combined)
const MAX_FILES = 50; // guard against a single request with thousands of files
const EXPIRY_MS = 24 * 60 * 60 * 1000; // links live for 24h

const BUCKET = process.env.SPACES_BUCKET;
const s3 = new S3Client({
  region: process.env.SPACES_REGION,
  endpoint: process.env.SPACES_ENDPOINT,
  credentials: {
    accessKeyId: process.env.SPACES_KEY,
    secretAccessKey: process.env.SPACES_SECRET,
  },
  forcePathStyle: false,
});

// ── ID generation ───────────────────────────────────────────────────────────
// Short, URL-safe, unguessable ids. base62 keeps links short and phone-typeable.
const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
/** Generate a random base62 id of the given length. */
function makeId(len = 8) {
  const bytes = randomBytes(len);
  let out = "";
  for (let i = 0; i < len; i++) out += ALPHABET[bytes[i] % ALPHABET.length];
  return out;
}

// ── Helpers ───────────────────────────────────────────────────────────────
/** Validate a link id from the URL (base62 only — blocks key/path injection). */
function validId(id) {
  return typeof id === "string" && /^[A-Za-z0-9]{1,32}$/.test(id);
}

/** Read a link's meta.json from Spaces, or null if missing/expired. */
async function readMeta(id) {
  if (!validId(id)) return null;
  try {
    const obj = await s3.send(
      new GetObjectCommand({ Bucket: BUCKET, Key: `${id}/meta.json` })
    );
    const meta = JSON.parse(await obj.Body.transformToString());
    if (Date.now() > meta.expiresAt) {
      deleteLink(id).catch(() => {}); // best-effort; lifecycle also sweeps it
      return null;
    }
    return meta;
  } catch {
    return null;
  }
}

/** Delete every object under a link prefix. */
async function deleteLink(id) {
  if (!validId(id)) return;
  try {
    const meta = await s3
      .send(new GetObjectCommand({ Bucket: BUCKET, Key: `${id}/meta.json` }))
      .then((o) => o.Body.transformToString())
      .then(JSON.parse)
      .catch(() => null);
    const keys = [{ Key: `${id}/meta.json` }];
    if (meta) for (const f of meta.files) keys.push({ Key: `${id}/${f.fileId}` });
    await s3.send(
      new DeleteObjectsCommand({ Bucket: BUCKET, Delete: { Objects: keys } })
    );
  } catch {
    /* ignore — lifecycle rule is the backstop */
  }
}

/** Human-readable byte size for UI. */
function humanSize(bytes) {
  const units = ["B", "KB", "MB", "GB"];
  let n = bytes;
  let u = 0;
  while (n >= 1024 && u < units.length - 1) {
    n /= 1024;
    u++;
  }
  return `${n.toFixed(n < 10 && u > 0 ? 1 : 0)} ${units[u]}`;
}

// ── App ───────────────────────────────────────────────────────────────────
const app = express();
app.set("trust proxy", true); // App Platform terminates TLS upstream; trust X-Forwarded-*
app.use(express.static(PUBLIC_DIR));

// Multer buffers each upload to a per-request temp dir on local disk (fine on
// ephemeral storage — it only lives for the duration of the request), then we
// stream those temp files up to Spaces and delete them.
const storage = multer.diskStorage({
  destination(req, file, cb) {
    if (!req.linkId) {
      req.linkId = makeId(8);
      req.fileManifest = [];
      req.tmpDir = path.join(os.tmpdir(), `toru-${req.linkId}`);
      fs.mkdirSync(req.tmpDir, { recursive: true });
    }
    cb(null, req.tmpDir);
  },
  filename(req, file, cb) {
    const fileId = makeId(10);
    // Multer decodes the filename as latin1; restore UTF-8 so accents survive.
    const originalName = Buffer.from(file.originalname, "latin1").toString("utf8");
    req.fileManifest.push({ fileId, name: originalName, type: file.mimetype });
    cb(null, fileId);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: MAX_TOTAL_BYTES, files: MAX_FILES },
});

// ── Routes ──────────────────────────────────────────────────────────────────

/** GET /health — liveness probe for the platform (no auth). */
app.get("/health", (req, res) => res.json({ ok: true }));

/**
 * POST /api/upload — accept one or more files, push them to Spaces, return a link.
 * Enforces the 500 MB combined cap (multer caps per-file; we sum and reject the
 * whole batch if the total is over).
 */
app.post("/api/upload", (req, res) => {
  upload.array("files", MAX_FILES)(req, res, async (err) => {
    const cleanupTmp = async () => {
      if (req.tmpDir) await fsp.rm(req.tmpDir, { recursive: true, force: true }).catch(() => {});
    };

    if (err) {
      await cleanupTmp();
      const tooBig = err.code === "LIMIT_FILE_SIZE";
      return res
        .status(tooBig ? 413 : 400)
        .json({ error: tooBig ? "A file exceeds the 500 MB limit." : "Upload failed." });
    }

    if (!req.files || req.files.length === 0) {
      await cleanupTmp();
      return res.status(400).json({ error: "No files were provided." });
    }

    const total = req.files.reduce((sum, f) => sum + f.size, 0);
    if (total > MAX_TOTAL_BYTES) {
      await cleanupTmp();
      return res.status(413).json({ error: "Total upload exceeds the 500 MB limit." });
    }

    try {
      const files = req.fileManifest.map((m, i) => ({
        ...m,
        size: req.files[i].size,
        sizeHuman: humanSize(req.files[i].size),
      }));

      // Stream each temp file up to Spaces.
      for (let i = 0; i < files.length; i++) {
        const f = files[i];
        await new Upload({
          client: s3,
          params: {
            Bucket: BUCKET,
            Key: `${req.linkId}/${f.fileId}`,
            Body: fs.createReadStream(req.files[i].path),
            ContentType: f.type || "application/octet-stream",
          },
        }).done();
      }

      const now = Date.now();
      const meta = {
        id: req.linkId,
        createdAt: now,
        expiresAt: now + EXPIRY_MS,
        totalSize: total,
        files,
      };
      await s3.send(
        new PutObjectCommand({
          Bucket: BUCKET,
          Key: `${req.linkId}/meta.json`,
          Body: JSON.stringify(meta),
          ContentType: "application/json",
        })
      );

      res.json({
        id: req.linkId,
        url: `${req.protocol}://${req.get("host")}/d/${req.linkId}`,
        expiresAt: meta.expiresAt,
        files,
      });
    } catch (e) {
      console.error("Upload to Spaces failed:", e.name, e.message);
      deleteLink(req.linkId).catch(() => {});
      res.status(500).json({ error: "Could not store the files. Please try again." });
    } finally {
      await cleanupTmp();
    }
  });
});

/** GET /api/link/:id — link metadata + file manifest (drives the download page). */
app.get("/api/link/:id", async (req, res) => {
  const meta = await readMeta(req.params.id);
  if (!meta) return res.status(404).json({ error: "This link has expired or does not exist." });
  res.json({
    id: meta.id,
    createdAt: meta.createdAt,
    expiresAt: meta.expiresAt,
    totalSize: meta.totalSize,
    files: meta.files,
  });
});

/**
 * GET /api/file/:id/:fileId — stream one file from Spaces.
 * Default is inline (images/video/PDF preview in the browser); ?dl=1 forces a
 * download. Range requests are passed through to Spaces so video/audio scrub.
 */
app.get("/api/file/:id/:fileId", async (req, res) => {
  const meta = await readMeta(req.params.id);
  if (!meta) return res.status(404).send("This link has expired or does not exist.");

  const entry = meta.files.find((f) => f.fileId === req.params.fileId);
  if (!entry) return res.status(404).send("File not found.");

  try {
    const range = req.headers.range;
    const obj = await s3.send(
      new GetObjectCommand({
        Bucket: BUCKET,
        Key: `${req.params.id}/${req.params.fileId}`,
        Range: range || undefined,
      })
    );

    const disposition = req.query.dl ? "attachment" : "inline";
    res.setHeader("Content-Type", entry.type || "application/octet-stream");
    res.setHeader(
      "Content-Disposition",
      `${disposition}; filename*=UTF-8''${encodeURIComponent(entry.name)}`
    );
    res.setHeader("Accept-Ranges", "bytes");
    if (obj.ContentLength != null) res.setHeader("Content-Length", obj.ContentLength);
    if (range && obj.ContentRange) {
      res.status(206);
      res.setHeader("Content-Range", obj.ContentRange);
    }
    obj.Body.pipe(res);
  } catch (e) {
    res.status(404).send("File not found.");
  }
});

/** GET /api/zip/:id — stream every file in the link as one .zip ("Download all"). */
app.get("/api/zip/:id", async (req, res) => {
  const meta = await readMeta(req.params.id);
  if (!meta) return res.status(404).send("This link has expired or does not exist.");

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename="toru-${meta.id}.zip"`);

  const archive = archiver("zip", { zlib: { level: 6 } });
  archive.on("error", () => res.destroy());
  archive.pipe(res);
  try {
    for (const f of meta.files) {
      const obj = await s3.send(
        new GetObjectCommand({ Bucket: BUCKET, Key: `${meta.id}/${f.fileId}` })
      );
      archive.append(obj.Body, { name: f.name });
    }
    await archive.finalize();
  } catch {
    res.destroy();
  }
});

/** GET /d/:id — the recipient's download page (static; fetches metadata via API). */
app.get("/d/:id", (req, res) => {
  res.sendFile(path.join(PUBLIC_DIR, "download.html"));
});

app.listen(PORT, () => {
  console.log(`Toru running on http://localhost:${PORT}`);
});
