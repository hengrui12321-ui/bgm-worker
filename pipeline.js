const { exec } = require("child_process");
const { promisify } = require("util");
const execAsync = promisify(exec);
const fs = require("fs");
const path = require("path");
const os = require("os");
const { createClient } = require("@supabase/supabase-js");
const Replicate = require("replicate");

// ─── Clients ──────────────────────────────────────────────────────────────────

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const replicate = new Replicate({
  auth: process.env.REPLICATE_API_TOKEN,
});

// ─── Mood config ──────────────────────────────────────────────────────────────

const MOOD_CONFIG = {
  calm: {
    bpm: 75,
    prompt: "calm ambient lo-fi background music, soft piano and gentle pads, 75 BPM, peaceful and relaxing, no vocals, no lyrics",
  },
  suspense: {
    bpm: 95,
    prompt: "suspenseful cinematic background music, tense strings and low brass, 95 BPM, dark and dramatic, no vocals, no lyrics",
  },
  vlog: {
    bpm: 105,
    prompt: "upbeat positive vlog background music, acoustic guitar and light percussion, 105 BPM, warm and friendly, no vocals, no lyrics",
  },
  "high-energy": {
    bpm: 128,
    prompt: "high energy electronic dance music, punchy kick drum, synth bass, 128 BPM, hype and energetic, no vocals, no lyrics",
  },
};

// ─── Supabase helpers ─────────────────────────────────────────────────────────

async function updateJob(jobId, fields) {
  const { error } = await supabase
    .from("bgm_jobs")
    .update({ ...fields, updated_at: new Date().toISOString() })
    .eq("id", jobId);

  if (error) console.error("[Supabase] updateJob error:", error.message);
}

async function downloadVideo(storagePath, localPath) {
  // storagePath is the path inside the bucket, e.g. "videos/user-123/clip.mp4"
  const { data, error } = await supabase.storage
    .from("videos") // ← your bucket name, change if different
    .download(storagePath);

  if (error || !data) throw new Error(`Supabase download failed: ${error?.message}`);

  const buffer = Buffer.from(await data.arrayBuffer());
  fs.writeFileSync(localPath, buffer);
  console.log(`[Pipeline] Video downloaded to ${localPath}`);
}

async function uploadResult(localPath, userId, jobId) {
  const storagePath = `outputs/${userId}/${jobId}/final.mp4`;
  const buffer = fs.readFileSync(localPath);

  const { error } = await supabase.storage
    .from("videos")
    .upload(storagePath, buffer, { upsert: true, contentType: "video/mp4" });

  if (error) throw new Error(`Supabase upload failed: ${error.message}`);

  const { data } = supabase.storage.from("videos").getPublicUrl(storagePath);
  return data.publicUrl;
}

// ─── Video analysis ───────────────────────────────────────────────────────────

async function getVideoDuration(videoPath) {
  const { stdout } = await execAsync(
    `ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "${videoPath}"`
  );
  const duration = parseFloat(stdout.trim());
  if (isNaN(duration)) throw new Error("Could not read video duration");
  return duration;
}

async function detectSceneCuts(videoPath) {
  // FFmpeg scene filter outputs matched frames to stderr via showinfo
  // We capture both stdout and stderr with 2>&1, then parse pts_time values
  // "|| true" prevents exec from throwing on non-zero exit (expected for -f null)
  const { stdout: combined } = await execAsync(
    `ffmpeg -i "${videoPath}" -vf "select='gt(scene,0.3)',showinfo" -vsync vfr -f null /dev/null 2>&1 || true`
  ).catch((e) => ({ stdout: e.stdout || "", stderr: e.stderr || "" }));

  const cuts = [0]; // always include 0 as first cut

  // Parse "pts_time:3.200000" patterns from showinfo output
  const regex = /pts_time:([\d.]+)/g;
  let match;
  while ((match = regex.exec(combined)) !== null) {
    const t = parseFloat(match[1]);
    // Skip cuts in first 0.5s and last 0.5s (likely noise)
    if (!isNaN(t) && t > 0.5) {
      cuts.push(t);
    }
  }

  const sorted = [...new Set(cuts)].sort((a, b) => a - b);
  console.log(`[Pipeline] Detected ${sorted.length} scene cuts:`, sorted);
  return sorted;
}

function getPacing(cuts, duration) {
  if (cuts.length < 2) return "slow";
  const avgSceneDuration = duration / cuts.length;
  if (avgSceneDuration < 3) return "fast";
  if (avgSceneDuration > 5) return "slow";
  return "medium";
}

// ─── Music generation ─────────────────────────────────────────────────────────

async function generateMusic(mood, duration, pacing) {
  const { bpm, prompt: basePrompt } = MOOD_CONFIG[mood];

  const pacingHint =
    pacing === "fast" ? ", fast transitions, high energy build" :
    pacing === "slow" ? ", slow build, sustained and continuous" :
    "";

  // MusicGen caps at 30s; clamp to video duration
  const musicDuration = Math.min(Math.ceil(duration), 30);

  const prompt = `${basePrompt}${pacingHint}, ${musicDuration} seconds`;
  console.log(`[Pipeline] Generating music: "${prompt}"`);

  // replicate.run() polls automatically until done
  const output = await replicate.run(
    "meta/musicgen:671ac645ce5e552cc63a54a2bbff63fcf798043055d2dac5fc9e36a837eedcfb",
    {
      input: {
        prompt,
        duration: musicDuration,
        output_format: "mp3",
        normalization_strategy: "peak",
      },
    }
  );

  // output is a URL string or array of URL strings
  const audioUrl = Array.isArray(output) ? output[0] : output;
  if (!audioUrl) throw new Error("Replicate returned no audio URL");

  console.log(`[Pipeline] Music generated: ${audioUrl}`);
  return audioUrl;
}

async function downloadAudio(url, localPath) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to download audio: ${res.status} ${res.statusText}`);
  const buffer = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(localPath, buffer);
  console.log(`[Pipeline] Audio downloaded to ${localPath}`);
}

// ─── FFmpeg mux with ducking ──────────────────────────────────────────────────

async function muxVideoWithBGM(videoPath, audioPath, outputPath, cuts, duration) {
  // Build volume duck filter: briefly dip to 60% volume at each scene cut
  // This makes cuts feel intentional even when music doesn't perfectly align
  const duckFilters = cuts
    .filter((t) => t > 0.3 && t < duration - 0.5)
    .map((t) => {
      const start = Math.max(0, t - 0.1).toFixed(3);
      const end = Math.min(duration, t + 0.1).toFixed(3);
      return `volume=enable='between(t,${start},${end})':volume=0.6`;
    });

  // Fade in 0.5s at start, fade out 1s at end
  const fadeIn  = `afade=t=in:st=0:d=0.5`;
  const fadeOut = `afade=t=out:st=${Math.max(0, duration - 1).toFixed(3)}:d=1`;

  const audioFilter = [...duckFilters, fadeIn, fadeOut].join(",");

  // FFmpeg command:
  // -map 0:v:0  → take video from input file (no original audio)
  // -map 1:a:0  → take audio from generated BGM
  // -af         → apply duck + fade filters, trim to video duration
  // -c:v copy   → don't re-encode video (fast)
  // -c:a aac    → encode audio as AAC for MP4 compatibility
  // -shortest   → stop at shortest stream (video length)
  const cmd = [
    `ffmpeg -y`,
    `-i "${videoPath}"`,
    `-i "${audioPath}"`,
    `-map 0:v:0`,
    `-map 1:a:0`,
    `-af "${audioFilter},atrim=0:${duration.toFixed(3)},asetpts=PTS-STARTPTS"`,
    `-c:v copy`,
    `-c:a aac`,
    `-shortest`,
    `"${outputPath}"`,
  ].join(" ");

  console.log(`[Pipeline] Running FFmpeg mux...`);
  const { stderr } = await execAsync(cmd);

  // Verify output was created
  if (!fs.existsSync(outputPath)) {
    throw new Error(`FFmpeg mux failed. stderr: ${stderr}`);
  }
  console.log(`[Pipeline] Mux complete: ${outputPath}`);
}

// ─── Main pipeline ────────────────────────────────────────────────────────────

async function runPipeline({ jobId, videoPath, mood, userId }) {
  const tmpDir = path.join(os.tmpdir(), `bgm-${jobId}`);
  fs.mkdirSync(tmpDir, { recursive: true });

  const localVideo  = path.join(tmpDir, "input.mp4");
  const localAudio  = path.join(tmpDir, "music.mp3");
  const localOutput = path.join(tmpDir, "output.mp4");

  try {
    // Step 1 — Download video
    await updateJob(jobId, { status: "processing", step: "Downloading video..." });
    await downloadVideo(videoPath, localVideo);

    // Step 2 — Analyze video
    await updateJob(jobId, { status: "processing", step: "Analyzing scene cuts..." });
    const [duration, cuts] = await Promise.all([
      getVideoDuration(localVideo),
      detectSceneCuts(localVideo),
    ]);
    const pacing = getPacing(cuts, duration);
    console.log(`[Pipeline] Duration: ${duration}s, Pacing: ${pacing}, Cuts: ${cuts.length}`);

    // Step 3 — Generate music
    await updateJob(jobId, { status: "processing", step: "Generating BGM (this takes ~30-60s)..." });
    const musicUrl = await generateMusic(mood, duration, pacing);

    // Step 4 — Download audio
    await updateJob(jobId, { status: "processing", step: "Downloading generated audio..." });
    await downloadAudio(musicUrl, localAudio);

    // Step 5 — Mux video + audio
    await updateJob(jobId, { status: "processing", step: "Syncing audio to video..." });
    await muxVideoWithBGM(localVideo, localAudio, localOutput, cuts, duration);

    // Step 6 — Upload result
    await updateJob(jobId, { status: "processing", step: "Uploading final video..." });
    const resultUrl = await uploadResult(localOutput, userId, jobId);

    // Step 7 — Done → Supabase Realtime fires on frontend
    await updateJob(jobId, {
      status: "done",
      step: "Done!",
      result_url: resultUrl,
    });

    console.log(`[Pipeline] Job ${jobId} complete. Result: ${resultUrl}`);

  } catch (err) {
    console.error(`[Pipeline] Job ${jobId} error:`, err);
    await updateJob(jobId, {
      status: "error",
      step: "Failed",
      error_message: err.message,
    }).catch(() => {}); // don't throw if this also fails
    throw err;

  } finally {
    // Always clean up temp files
    fs.rmSync(tmpDir, { recursive: true, force: true });
    console.log(`[Pipeline] Temp files cleaned: ${tmpDir}`);
  }
}

module.exports = { runPipeline };
