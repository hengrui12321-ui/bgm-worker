const express = require("express");
const { runPipeline } = require("./pipeline");
const { exec } = require("child_process");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;
const WORKER_SECRET = process.env.WORKER_SECRET || "";

const jobQueue = [];
let isProcessing = false;

async function processQueue() {
  if (isProcessing || jobQueue.length === 0) return;
  isProcessing = true;
  const job = jobQueue.shift();
  console.log("[Queue] Starting job " + job.jobId);
  try {
    await runPipeline(job);
    console.log("[Queue] Job " + job.jobId + " completed");
  } catch (err) {
    console.error("[Queue] Job " + job.jobId + " failed:", err.message);
  }
  isProcessing = false;
  processQueue();
}

app.get("/health", (req, res) => {
  res.json({ status: "ok", queue: jobQueue.length, processing: isProcessing });
});

app.get("/check-ffmpeg", (req, res) => {
  exec("ffmpeg -version", (err, stdout) => {
    if (err) return res.json({ ffmpeg: false, error: err.message });
    res.json({ ffmpeg: true, version: stdout.split("\n")[0] });
  });
});

app.post("/process", (req, res) => {
  if (WORKER_SECRET) {
    var auth = req.headers["authorization"];
    if (auth !== "Bearer " + WORKER_SECRET) {
      return res.status(401).json({ error: "Unauthorized" });
    }
  }
  var jobId = req.body.jobId;
  var videoPath = req.body.videoPath;
  var mood = req.body.mood;
  var userId = req.body.userId;
  if (!jobId || !videoPath || !mood || !userId) {
    return res.status(400).json({ error: "Missing required fields" });
  }
  var validMoods = ["calm", "suspense", "vlog", "high-energy"];
  if (!validMoods.includes(mood)) {
    return res.status(400).json({ error: "Invalid mood" });
  }
  jobQueue.push({ jobId: jobId, videoPath: videoPath, mood: mood, userId: userId });
  console.log("[Queue] Job " + jobId + " queued");
  processQueue();
  res.status(202).json({ message: "Job queued", jobId: jobId });
});

app.listen(PORT, function() {
  console.log("BGM Worker running on port " + PORT);
});