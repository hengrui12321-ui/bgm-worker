const express = require("express");
const { runPipeline } = require("./pipeline");

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3001;

// Simple in-memory queue — good enough for MVP/demo
// One job runs at a time; others wait in line
const jobQueue = [];
let isProcessing = false;

async function processQueue() {
  if (isProcessing || jobQueue.length === 0) return;
  isProcessing = true;

  const job = jobQueue.shift();
  console.log(`[Queue] Starting job ${job.jobId}`);

  try {
    await runPipeline(job);
    console.log(`[Queue] Job ${job.jobId} completed`);
  } catch (err) {
    console.error(`[Queue] Job ${job.jobId} failed:`, err.message);
  }

  isProcessing = false;
  processQueue(); // process next job if any
}

// Health check — Railway uses this to verify the service is up
app.get("/health", (req, res) => {
  res.json({ status: "ok", queue: jobQueue.length });
});

// Vercel calls this endpoint to kick off a job
app.post("/process", (req, res) => {
  const { jobId, videoPath, mood, userId } = req.body;

  // Validate required fields
  if (!jobId || !videoPath || !mood || !userId) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  // Validate mood
  const validMoods = ["calm", "suspense", "vlog", "high-energy"];
  if (!validMoods.includes(mood)) {
    return res.status(400).json({ error: `Invalid mood. Must be one of: ${validMoods.join(", ")}` });
  }

  // Add to queue
  jobQueue.push({ jobId, videoPath, mood, userId });
  console.log(`[Queue] Job ${jobId} queued. Queue length: ${jobQueue.length}`);

  // Start processing (non-blocking)
  processQueue();

  // Respond immediately — frontend will get updates via Supabase Realtime
  res.status(202).json({ message: "Job queued", jobId });
});

app.listen(PORT, () => {
  console.log(`BGM Worker running on port ${PORT}`);
});
