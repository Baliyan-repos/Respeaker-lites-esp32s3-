#!/usr/bin/env node
/**
 * index.mjs — ReSpeaker Bridge (Production Firmware Compatible)
 *
 * - Converts incoming URLs -> 16k mono WAV via ffmpeg
 * - S3 caching using sha256(url) keyed wavs (S3_PREFIX/<sha>.wav)
 * - Publishes presigned GET URL to <device>/audio/play/control
 * - Assembles recordings from devices and uploads to S3
 * - Compatible with esp32_respeaker_prod_final.ino firmware
 *
 * Requirements:
 *   npm i mqtt dotenv express multer @aws-sdk/client-s3 @aws-sdk/credential-providers @aws-sdk/signature-v4 @aws-sdk/protocol-http @aws-crypto/sha256-js @aws-sdk/s3-request-presigner
 *
 * Edit .env (do NOT commit secrets)
 */

import fs from "fs";
import os from "os";
import path from "path";
import crypto from "crypto";
import { spawn, spawnSync } from "child_process";
import express from "express";
import multer from "multer";
import { pipeline } from "stream";
import mqtt from "mqtt";
import dotenv from "dotenv";
import dns from "dns/promises";
dotenv.config();

import { S3Client, PutObjectCommand, HeadObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { fromEnv } from "@aws-sdk/credential-providers";
import { SignatureV4 } from "@aws-sdk/signature-v4";
import { HttpRequest } from "@aws-sdk/protocol-http";
import { Sha256 } from "@aws-crypto/sha256-js";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

////////////////////////////////////////////////////////////////////////////////
// config
////////////////////////////////////////////////////////////////////////////////
const {
  MQTT_URL,
  MQTT_CLIENT_ID,
  AWS_REGION,
  S3_BUCKET,
  S3_PREFIX = "final/",
  TMP_DIR = process.env.TMP_DIR || path.join(os.tmpdir(), "rs_bridge"),
  LOG_LEVEL = "info",
  FFMPEG_PATH = process.env.FFMPEG_PATH || "ffmpeg",
  HTTP_PORT = process.env.HTTP_PORT || 3001,
  AUDIO_DIR = process.env.AUDIO_DIR || path.join(process.cwd(), "audio"),
  SIGNED_URL_EXPIRES = process.env.SIGNED_URL_EXPIRES || "900",
  USE_AWS_SIGV4 = (process.env.USE_AWS_SIGV4 || (process.env.AWS_REGION ? "true" : "false")).toLowerCase() === "true",
  DNS_RETRY_MAX = Number(process.env.DNS_RETRY_MAX || 12),
  DNS_RETRY_BASE_MS = Number(process.env.DNS_RETRY_BASE_MS || 250)
} = process.env;

if(!MQTT_URL){
  console.error("MQTT_URL required in .env");
  process.exit(1);
}

const LOG_LEVELS = { error:0, warn:1, info:2, debug:3 };
const currentLogLevel = LOG_LEVELS[LOG_LEVEL] ?? 2;
const log = (lvl, ...args) => { if((LOG_LEVELS[lvl] ?? 99) <= currentLogLevel) console.log(new Date().toISOString(), lvl.toUpperCase(), ...args); };

const EFFECTIVE_CLIENT_ID = MQTT_CLIENT_ID || `esp32-bridge-${Math.floor(Math.random()*100000)}`;

fs.mkdirSync(TMP_DIR, { recursive: true });
fs.mkdirSync(AUDIO_DIR, { recursive: true });

log("info","CONFIG", {
  MQTT_URL: MQTT_URL.startsWith("mqtt://") ? "mqtt://…" : "wss://…",
  MQTT_CLIENT_ID: EFFECTIVE_CLIENT_ID,
  FFMPEG_PATH,
  USE_AWS_SIGV4, DNS_RETRY_MAX, DNS_RETRY_BASE_MS
});

////////////////////////////////////////////////////////////////////////////////
// optional S3
////////////////////////////////////////////////////////////////////////////////
let S3 = null;
let awsCredentialsProvider = null;
if(process.env.AWS_REGION && USE_AWS_SIGV4){
  try { awsCredentialsProvider = fromEnv(); } catch(e){ log("warn","awsCredentialsProvider init failed:", e?.message||e); awsCredentialsProvider = null; }
}
if(S3_BUCKET && process.env.AWS_REGION){
  try { S3 = new S3Client({ region: process.env.AWS_REGION, credentials: fromEnv() }); log("info","S3 archive configured ->", S3_BUCKET); } catch(e){ log("warn","S3 init failed:", e?.message||e); S3 = null; }
} else log("info","S3 archive disabled (S3_BUCKET or AWS_REGION missing)");

////////////////////////////////////////////////////////////////////////////////
// util helpers
////////////////////////////////////////////////////////////////////////////////
const tmpFile = (deviceId, sessionId) => path.join(TMP_DIR, `${deviceId}--${sessionId}.pcm`);
const tmpWavFile = (deviceId, sessionId) => path.join(TMP_DIR, `${deviceId}--${sessionId}.wav`);
const tmpConvertedFile = (deviceId, sessionId) => path.join(TMP_DIR, `${deviceId}--${sessionId}--converted.wav`);

function sha256hex(s){ return crypto.createHash('sha256').update(s).digest('hex'); }

function buildWavBuffer(pcmBuffer, sampleRate=16000, channels=1){
  const bits = 16;
  const byteRate = sampleRate * channels * (bits/8);
  const blockAlign = channels * (bits/8);
  const header = Buffer.alloc(44);
  header.write("RIFF",0); header.writeUInt32LE(36 + pcmBuffer.length,4); header.write("WAVE",8);
  header.write("fmt ",12); header.writeUInt32LE(16,16); header.writeUInt16LE(1,20);
  header.writeUInt16LE(channels,22); header.writeUInt32LE(sampleRate,24);
  header.writeUInt32LE(byteRate,28); header.writeUInt16LE(blockAlign,32); header.writeUInt16LE(bits,34);
  header.write("data",36); header.writeUInt32LE(pcmBuffer.length,40);
  return Buffer.concat([header, pcmBuffer]);
}

////////////////////////////////////////////////////////////////////////////////
// mqtt publish helpers
////////////////////////////////////////////////////////////////////////////////
let mqttClient = null;
function mqttIsConnected(){ try { return mqttClient && (typeof mqttClient.connected === "function" ? mqttClient.connected() : mqttClient.connected); } catch(e){ return false; } }

function publishAsync(topic, buf, opts={qos:1, retain:false}, timeoutMs=20000){
  return new Promise((res, rej) => {
    if(!mqttClient) return rej(new Error("mqtt not ready"));
    if(!mqttIsConnected()) return rej(new Error("mqtt not connected"));
    let done = false;
    const t = setTimeout(()=>{ if(done) return; done=true; rej(new Error("publish timeout")); }, timeoutMs).unref();
    const payload = Buffer.isBuffer(buf) ? buf : Buffer.from(String(buf));
    mqttClient.publish(topic, payload, opts, (err)=>{
      if(done) return;
      done=true; clearTimeout(t);
      if(err) return rej(err);
      return res();
    });
  });
}

async function publishWithRetry(topic, buf, opts={}, timeoutMs=20000, attempts=5){
  let lastErr = null;
  for(let i=1;i<=attempts;i++){
    try { await publishAsync(topic, buf, opts, timeoutMs); return; }
    catch(e){ lastErr = e; log("warn","publish attempt",i,"failed:", e?.message||e); await new Promise(r=>setTimeout(r,150*i)); }
  }
  throw lastErr;
}

////////////////////////////////////////////////////////////////////////////////
// ffmpeg helpers + S3 caching
////////////////////////////////////////////////////////////////////////////////
function ensureFfmpegAvailable(){
  try { const chk = spawnSync(FFMPEG_PATH, ['-version'], { stdio: 'ignore' }); return chk.status === 0 || chk.status === null; } catch(e){ return false; }
}

function ffmpegConvertToWavFile(urlOrPath, outPath){
  return new Promise((res, rej) => {
    const args = [
      "-hide_banner", "-loglevel", "error",
      "-y",
      "-fflags", "nobuffer",
      "-flags", "low_delay",
      "-probesize", "32",
      "-analyzeduration", "0",
      "-threads", "0",
      "-i", urlOrPath,
      "-ac", "1",
      "-ar", "16000",
      "-f", "wav",
      outPath
    ];
    log("debug","ffmpeg convert args", args.join(" "));
    const ff = spawn(FFMPEG_PATH, args, { stdio: ['ignore','pipe','pipe'] });
    let stderr = "";
    ff.stderr.on('data', d => { stderr += d.toString(); if(currentLogLevel >= LOG_LEVELS.debug) process.stderr.write(d); });
    ff.on('error', e => rej(new Error("ffmpeg spawn error: "+(e?.message||e))));
    ff.on('close', (code, sig) => {
      if(code === 0) return res(outPath);
      return rej(new Error(`ffmpeg exit ${code} sig=${sig} stderr=${stderr.slice(0,2000)}`));
    });
  });
}

async function s3ObjectExists(key){
  if(!S3) return false;
  try { await S3.send(new HeadObjectCommand({ Bucket: S3_BUCKET, Key: key })); return true; } catch(e){ if(e.name === "NotFound" || e.$metadata?.httpStatusCode === 404) return false; log("warn","HeadObject error", e?.message || e); return false; }
}
async function uploadBufferToS3(key, buf){
  if(!S3) throw new Error("S3 not configured");
  await S3.send(new PutObjectCommand({ Bucket: S3_BUCKET, Key: key, Body: buf, ContentType: "audio/wav" }));
}
async function getPresignedUrlForKey(key, expires=Number(SIGNED_URL_EXPIRES)){
  if(!S3) throw new Error("S3 not configured");
  const cmd = new GetObjectCommand({ Bucket: S3_BUCKET, Key: key });
  return await getSignedUrl(S3, cmd, { expiresIn: expires });
}

////////////////////////////////////////////////////////////////////////////////
// conversion locks
////////////////////////////////////////////////////////////////////////////////
const conversionLocks = new Map();
function lockKeyForUrl(url){ return sha256hex(url); }
function acquireConversionLock(key, factory){
  if(conversionLocks.has(key)) return conversionLocks.get(key);
  const p = (async ()=>{
    try { return await factory(); } finally { conversionLocks.delete(key); }
  })();
  conversionLocks.set(key, p);
  return p;
}

////////////////////////////////////////////////////////////////////////////////
// main playback path: convert or use cache - publish presigned URL
////////////////////////////////////////////////////////////////////////////////
async function handlePlaybackRequest(deviceId, urlOrPath, opts={}){
  if(!deviceId || !urlOrPath) { log("warn","missing device or url"); return; }
  log("info","[PLAY] request ->", deviceId, urlOrPath);
  if(!ensureFfmpegAvailable()){ log("error","ffmpeg not found at", FFMPEG_PATH); return; }

  const urlKey = lockKeyForUrl(urlOrPath);
  const s3Key = path.posix.join(S3_PREFIX, "play", encodeURIComponent(urlKey) + ".wav");
  const requestStartTime = Date.now();

  return acquireConversionLock(urlKey, async ()=>{
    try {
      let s3Url = null;
      let cacheHit = false;
      
      // Check cache first
      if(S3 && await s3ObjectExists(s3Key)){
        const cacheCheckTime = Date.now();
        log("info", `[CACHE][HIT] device=${deviceId} s3Key=${s3Key.substring(0, 50)}... checking file`);
        
        try {
          // Get file size for logging
          const headCmd = new HeadObjectCommand({ Bucket: S3_BUCKET, Key: s3Key });
          const headResult = await S3.send(headCmd);
          const fileSizeBytes = headResult.ContentLength || 0;
          const fileSizeKB = (fileSizeBytes / 1024).toFixed(1);
          const estimatedDurationSec = fileSizeBytes > 44 ? ((fileSizeBytes - 44) / 2) / 16000 : 0;
          
          s3Url = await getPresignedUrlForKey(s3Key, Number(SIGNED_URL_EXPIRES));
          const cacheAgeMs = Date.now() - cacheCheckTime;
          log("info", `[CACHE][URL] device=${deviceId} presigned URL generated in ${cacheAgeMs}ms fileSize=${fileSizeKB}kB estimatedDuration=${estimatedDurationSec.toFixed(2)}s`);
          cacheHit = true;
        } catch(e){
          log("warn", `[CACHE][ERROR] device=${deviceId} failed to generate presigned URL: ${e?.message || e}, will convert fresh`);
        }
      }

      // Convert if not in cache
      if(!s3Url){
        const convertStartTime = Date.now();
        const session = `play_${Date.now()}_${Math.floor(Math.random()*10000)}`;
        const convertedPath = tmpConvertedFile("conv_" + session);
        log("info", `[CONVERT][START] device=${deviceId} input=${urlOrPath.substring(0, 60)}... output=${convertedPath}`);
        
        await ffmpegConvertToWavFile(urlOrPath, convertedPath);
        
        const convertTime = Date.now() - convertStartTime;
        const wavBuf = fs.readFileSync(convertedPath);
        const wavBytes = wavBuf.length;
        const convertSpeedKBps = wavBytes > 0 ? (wavBytes / 1024) / (convertTime / 1000) : 0;
        
        log("info", `[CONVERT][DONE] device=${deviceId} wavBytes=${wavBytes} convertTime=${convertTime}ms convertSpeed=${convertSpeedKBps.toFixed(1)}kB/s`);
        
        // Upload to S3 for caching
        if(S3){
          try {
            const uploadStartTime = Date.now();
            await uploadBufferToS3(s3Key, wavBuf);
            const uploadTime = Date.now() - uploadStartTime;
            const uploadSpeedKBps = wavBytes > 0 ? (wavBytes / 1024) / (uploadTime / 1000) : 0;
            log("info", `[S3][UPLOAD] device=${deviceId} uploaded to S3 s3Key=${s3Key.substring(0, 50)}... uploadTime=${uploadTime}ms uploadSpeed=${uploadSpeedKBps.toFixed(1)}kB/s`);
            
            // Generate presigned URL
            s3Url = await getPresignedUrlForKey(s3Key, Number(SIGNED_URL_EXPIRES));
            log("info", `[S3][URL] device=${deviceId} presigned URL generated`);
          } catch(e){ log("warn","S3 upload failed", e?.message||e); }
        }
        
        try { fs.unlinkSync(convertedPath); } catch(_) {}
      }

      // Publish presigned URL to device
      if(s3Url){
        const topic = `${deviceId}/audio/play/control`;
        const payload = JSON.stringify({ 
          url: s3Url,
          sampleRate: 16000 
        });
        
        try {
          const publishStart = Date.now();
          await publishWithRetry(topic, payload, { qos: 1, retain: false });
          const publishTime = Date.now() - publishStart;
          const totalRequestTime = Date.now() - requestStartTime;
          
          // Extract S3 key info for logging
          let s3KeyInfo = "";
          try {
            const urlObj = new URL(s3Url);
            s3KeyInfo = urlObj.pathname.substring(0, 50);
          } catch(_) {}
          
          log("info", `[PLAY][DONE] device=${deviceId} totalRequestTime=${totalRequestTime}ms publishTime=${publishTime}ms mode=HTTP_URL cacheHit=${cacheHit} s3Key=${s3KeyInfo}...`);
          log("info", `[PLAY][URL] device=${deviceId} published to ${topic} urlLength=${s3Url.length} expires=${SIGNED_URL_EXPIRES}s`);
        } catch(e){
          log("error", `[PLAY][ERROR] device=${deviceId} failed to publish URL: ${e?.message || e}`);
        }
      } else {
        log("error", `[PLAY][ERROR] device=${deviceId} no S3 URL available`);
      }
    } catch(e){ log("error","handlePlaybackRequest failed:", e?.message || e); throw e; }
  });
}

////////////////////////////////////////////////////////////////////////////////
// recording assembly (device -> audio/<session>/{chunk,control})
////////////////////////////////////////////////////////////////////////////////
const recordings = new Map();
function ensureRecording(deviceId, sessionId, meta={}){
  const key = `${deviceId}/${sessionId}`;
  if(recordings.has(key)) return recordings.get(key);
  const tmp = tmpFile(deviceId, sessionId);
  fs.mkdirSync(path.dirname(tmp), { recursive: true });
  const fd = fs.openSync(tmp, "a");
  const st = { deviceId, sessionId, tmpPath: tmp, fd, bytes:0, sampleRate: meta.sampleRate||16000, channels: meta.channels||1, lastSeen: Date.now() };
  recordings.set(key, st);
  return st;
}
function appendRecording(st, pcm){ try { fs.writeSync(st.fd, pcm); st.bytes += pcm.length; st.lastSeen = Date.now(); } catch(e){ log("error","record write failed", e?.message||e); } }

async function finalizeRecording(deviceId, sessionId, meta={}){
  const key = `${deviceId}/${sessionId}`; const st = recordings.get(key);
  if(!st){ log("warn","finalize called but no recording state", key); return; }
  try { fs.fsyncSync(st.fd); fs.closeSync(st.fd); st.fd = null; } catch(e){}
  const pcm = fs.readFileSync(st.tmpPath);
  const minBytes = Math.ceil(st.sampleRate * st.channels * 2 * 0.6);
  let padded = pcm;
  if(pcm.length < minBytes){ padded = Buffer.concat([pcm, Buffer.alloc(minBytes - pcm.length)]); log("info","padded pcm to", padded.length); }
  const wav = buildWavBuffer(padded, st.sampleRate, st.channels);
  const out = path.join(AUDIO_DIR, `${deviceId}--${sessionId}.wav`);
  fs.writeFileSync(out, wav);
  log("info","wrote recording to", out);
  if(S3){
    try {
      const keyName = path.posix.join(S3_PREFIX, deviceId, `${sessionId}.wav`);
      await S3.send(new PutObjectCommand({ Bucket: S3_BUCKET, Key: keyName, Body: wav, ContentType: "audio/wav" }));
      log("info","uploaded recording to s3://", S3_BUCKET, keyName);
    } catch(e){ log("error","S3 upload failed", e?.message||e); }
  }
  try { fs.unlinkSync(st.tmpPath); } catch(_) {}
  recordings.delete(key);
}

////////////////////////////////////////////////////////////////////////////////
// MQTT connection (with SigV4 for AWS IoT)
////////////////////////////////////////////////////////////////////////////////
async function createSignedUrlIfAws(mqttUrl){
  try {
    if(!USE_AWS_SIGV4) { log("info","USE_AWS_SIGV4 disabled - not signing URL"); return mqttUrl; }
    const u = new URL(mqttUrl);
    if(!u.hostname || (!u.hostname.includes("iot") && !u.hostname.includes("amazonaws"))) return mqttUrl;
    if(!awsCredentialsProvider) { log("warn","No aws credentials provider - cannot sign"); return mqttUrl; }
    const creds = await awsCredentialsProvider();
    if(!creds || !creds.accessKeyId) { log("warn","aws creds missing - cannot sign"); return mqttUrl; }
    const host = u.hostname;
    const pathName = (u.pathname && u.pathname !== "") ? u.pathname : "/mqtt";
    const request = new HttpRequest({ method: "GET", protocol: "wss:", hostname: host, path: pathName, headers: { host } });
    const signer = new SignatureV4({ credentials: creds, service: "iotdevicegateway", region: process.env.AWS_REGION, sha256: Sha256 });
    const signed = await signer.presign(request, { expiresIn: 3600 });
    const query = new URLSearchParams(signed.query).toString();
    const signedUrl = `wss://${host}${pathName}?${query}`;
    log("info","generated SigV4 signed URL for AWS IoT");
    return signedUrl;
  } catch(e){ log("warn","createSignedUrlIfAws failed:", e?.message||e); return mqttUrl; }
}

////////////////////////////////////////////////////////////////////////////////
// DNS resolution helper with retries
////////////////////////////////////////////////////////////////////////////////
async function resolveHostWithRetry(hostname, maxAttempts=DNS_RETRY_MAX, baseMs=DNS_RETRY_BASE_MS){
  for(let attempt=1; attempt<=maxAttempts; attempt++){
    try {
      if(currentLogLevel >= LOG_LEVELS.debug) log("debug","DNS lookup attempt", attempt, "for", hostname);
      const res = await dns.lookup(hostname);
      if(res && (res.address || res.family)) {
        if(currentLogLevel >= LOG_LEVELS.debug) log("debug","DNS resolved", hostname, "->", res.address, "family", res.family);
        return res;
      }
    } catch(err){
      const code = err && err.code ? err.code : '';
      log("warn", `DNS lookup attempt ${attempt} failed for ${hostname}: ${err?.message || err} (${code})`);
      if(attempt === maxAttempts){
        throw err;
      }
      const wait = baseMs * Math.pow(2, Math.min(attempt-1, 8));
      if(currentLogLevel >= LOG_LEVELS.debug) log("debug", `waiting ${wait}ms before next DNS attempt`);
      await new Promise(r=>setTimeout(r, wait));
    }
  }
  throw new Error("DNS resolution exhausted");
}

////////////////////////////////////////////////////////////////////////////////
// connectMqtt (improved resilience)
////////////////////////////////////////////////////////////////////////////////
async function connectMqtt(){
  let connectUrl = MQTT_URL;
  try {
    connectUrl = await createSignedUrlIfAws(MQTT_URL);
  } catch(e){
    log("warn","signing failed, falling back to raw MQTT_URL:", e?.message || e);
    connectUrl = MQTT_URL;
  }

  let parsed;
  try { parsed = new URL(connectUrl); } catch(e){ log("error","invalid MQTT_URL:", e?.message||e); throw e; }

  // Pre-check DNS for hostname to avoid immediate getaddrinfo EAI_AGAIN
  try {
    await resolveHostWithRetry(parsed.hostname, DNS_RETRY_MAX, DNS_RETRY_BASE_MS);
  } catch(dnsErr){
    log("error","DNS resolution failed repeatedly for", parsed.hostname, "->", dnsErr?.message||dnsErr);
    log("error","Possible causes: network/DNS outage, local firewall, or misconfigured DNS.");
    log("error","I'll still attempt to connect; mqtt client will continue to retry, but check your network/DNS.");
    // continue — mqtt.connect will also retry
  }

  const opts = {
    clientId: EFFECTIVE_CLIENT_ID,
    reconnectPeriod: 2000,
    keepalive: 60,
    connectTimeout: 30_000,
    clean: true,
    protocolId: 'MQTT',
    protocolVersion: 4,
  };

  try {
    if(parsed.protocol === "wss:" || parsed.protocol === "wss") opts.protocol = "wss";
    else if(parsed.protocol === "ws:" || parsed.protocol === "ws") opts.protocol = "ws";
    else if(parsed.protocol === "mqtt:" || parsed.protocol === "mqtt") opts.protocol = "mqtt";
    else if(parsed.protocol === "mqtts:" || parsed.protocol === "mqtts") opts.protocol = "mqtts";
  } catch(e){ /* ignore */ }

  log("info","[MQTT] connecting to", parsed.hostname, "clientId", opts.clientId);
  mqttClient = mqtt.connect(connectUrl, opts);

  mqttClient.on('packetsend', pkt => { if(currentLogLevel >= LOG_LEVELS.debug) log('debug','packetsend', pkt && pkt.cmd ? pkt.cmd : pkt); });
  mqttClient.on('packetreceive', pkt => { if(currentLogLevel >= LOG_LEVELS.debug) log('debug','packetreceive', pkt && pkt.cmd ? pkt.cmd : pkt); });

  mqttClient.on("connect", () => {
    log("info","[MQTT] connected ✅");
    setTimeout(()=> {
      const subs = ["+/audio/+/chunk", "+/audio/+/control", "+/sub", "bridge/sub", "+/audio/play/control"];
      for(const t of subs){
        (function subscribeWithRetry(topic, attempt=1){
          mqttClient.subscribe(topic, { qos: 1 }, (err) => {
            if(err){
              log("error","subscribe failed", topic, err?.message || err);
              if(attempt <= 4) setTimeout(()=> subscribeWithRetry(topic, attempt+1), 300 * attempt);
              else log("warn","subscribe permanently failed after retries", topic);
            } else log("info","subscribed", topic);
          });
        })(t);
      }
    }, 250);
  });

  mqttClient.on("reconnect", () => log("info","[MQTT] reconnecting"));
  mqttClient.on("close", () => log("warn","[MQTT] closed (socket closed)"));
  mqttClient.on("disconnect", (packet) => log("warn","[MQTT] disconnect", packet));
  mqttClient.on("error", (e) => {
    const code = e && e.code ? e.code : null;
    log("error","[MQTT] error", e?.message || e, code ? `(${code})` : "");
    if(code === "ENOTFOUND" || code === "EAI_AGAIN"){
      log("warn","DNS/network error while connecting to MQTT host. Test with: nslookup", parsed.hostname);
    }
  });
  mqttClient.on("offline", () => log("warn","[MQTT] offline"));

  mqttClient.on("message", async (topic, payloadBuf) => {
    try {
      const payload = Buffer.isBuffer(payloadBuf) ? payloadBuf : Buffer.from(payloadBuf || "");
      const parts = String(topic).split("/").filter(Boolean);

      // Recording device -> audio/<session>/{chunk,control}
      if(parts.length >= 4 && parts[1] === "audio" && parts[2] !== "play"){
        const deviceId = parts[0];
        const session = parts[2];
        const what = parts[3];
        if(what === "chunk"){
          if(payload.length < 4){ log("warn","[REC] chunk too small"); return; }
          const seq = payload.readUInt32LE(0); const pcm = payload.slice(4);
          const st = ensureRecording(deviceId, session, {}); appendRecording(st, pcm);
          if(currentLogLevel >= LOG_LEVELS.debug) log("debug","[REC] chunk recv", deviceId, session, seq, pcm.length);
          return;
        }
        if(what === "control"){
          let j = null; try { j = JSON.parse(payload.toString("utf8")); } catch(e){ log("warn","[REC] control JSON invalid"); return; }
          const st = ensureRecording(deviceId, session, j || {});
          if(j && j.start){ if(j.sampleRate) st.sampleRate = Number(j.sampleRate); if(j.channels) st.channels = Number(j.channels); if(Number.isFinite(j.totalChunks)) st.finalExpected = Number(j.totalChunks); if(Number.isFinite(j.chunkBytes)) st.chunkBytes = Number(j.chunkBytes);
            log("info","[REC] start for", deviceId, session, "meta", { sampleRate: st.sampleRate, channels: st.channels, totalChunks: st.finalExpected }); }
          if(j && j.final){ log("info","[REC] final -> finalize", deviceId, session); try{ await finalizeRecording(deviceId, session, j); } catch(e){ log("error","finalize failed", e?.message||e); } }
          return;
        }
      }

      // Play control (incoming control to start play) - firmware expects {url, sampleRate}
      if(parts.length >= 4 && parts[1] === "audio" && parts[2] === "play" && parts[3] === "control"){
        const deviceId = parts[0];
        let j = null; try { j = JSON.parse(payload.toString("utf8")); } catch(e){ log("warn","[PLAY_CTRL] invalid JSON"); return; }
        
        // Check if this is a playback request (has url field)
        if(j && j.url && j.device){ 
          handlePlaybackRequest(j.device, j.url, j).catch(e => log("error","play req failed", e?.message || e)); 
          return; 
        }
        // Otherwise, firmware sends control messages - we ignore them (they're for device internal use)
        return;
      }

      // Bridge/sub commands
      if((parts.length >= 2 && parts[1] === "sub") || topic === "bridge/sub"){
        let msg = null; try { msg = JSON.parse(payload.toString("utf8")); } catch(_) {}
        if(msg && msg.url && msg.device){ handlePlaybackRequest(msg.device, msg.url, msg).catch(e => log("error","play req failed", e?.message || e)); return; }
        const s = payload.toString("utf8").trim();
        if(s.startsWith("http://") || s.startsWith("https://") || s.startsWith("s3://")){
          if(parts.length >= 2 && parts[1] === "sub"){ const deviceId = parts[0]; handlePlaybackRequest(deviceId, s).catch(e => log("error", e?.message || e)); return; } else { handlePlaybackRequest("broadcast", s).catch(e => log("error", e?.message || e)); return; }
        }
      }

    } catch(e){ log("error","[MSG] handler uncaught", e?.stack || e); }
  });

  return mqttClient;
}

////////////////////////////////////////////////////////////////////////////////
// express API (stream/upload/play/files)
////////////////////////////////////////////////////////////////////////////////
const app = express();
app.use(express.json({ limit: "10mb" }));
app.use((req, res, next) => { res.header('Access-Control-Allow-Origin', '*'); res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS'); res.header('Access-Control-Allow-Headers', 'Content-Type, Range'); if(req.method==='OPTIONS') return res.sendStatus(200); next(); });
app.use(express.static('public'));

function contentTypeFor(filename){ const ext = path.extname(filename).toLowerCase(); if(ext === '.mp3') return 'audio/mpeg'; if(ext === '.wav') return 'audio/wav'; if(ext === '.m4a') return 'audio/mp4'; if(ext === '.ogg') return 'audio/ogg'; return 'application/octet-stream'; }

app.get('/api/files', (req, res) => {
  try { if(!fs.existsSync(AUDIO_DIR)) return res.json({ files: [] }); const files = fs.readdirSync(AUDIO_DIR).filter(f => /\.(mp3|wav|m4a|ogg)$/i.test(f)); res.json({ files }); } catch(e){ res.status(500).json({ error: e.message }); }
});

app.get('/stream/:filename', (req, res) => {
  try {
    const filename = req.params.filename;
    const baseName = filename.replace(/\.(mp3|wav)$/i, '');

    const wavPath = path.join(AUDIO_DIR, `${baseName}.wav`);
    const mp3Path = path.join(AUDIO_DIR, `${baseName}.mp3`);

    let filePath;
    let contentType;

    if (fs.existsSync(wavPath)) {
      filePath = wavPath;
      contentType = 'audio/wav';
    } else if (fs.existsSync(mp3Path)) {
      filePath = mp3Path;
      contentType = 'audio/mpeg';
    } else {
      return res.status(404).json({ error: 'File not found' });
    }

    const stat = fs.statSync(filePath);
    const fileSize = stat.size;

    const head = {
      'Content-Length': fileSize,
      'Content-Type': contentType,
    };

    res.writeHead(200, head);
    const fileStream = fs.createReadStream(filePath, {
      highWaterMark: 32 * 1024
    });

    pipeline(fileStream, res, (err) => {
      if (err) {
        if (err.code !== 'ERR_STREAM_PREMATURE_CLOSE' && err.code !== 'ECONNRESET') {
          console.error('Stream pipeline error:', err);
        }
        if (!res.headersSent) {
          res.status(500).json({ error: 'Error streaming file' });
        } else {
          res.end();
        }
      }
    });
  } catch(e){ log("error","/stream error", e?.message||e); res.status(500).json({ error: e.message }); }
});

const upload = multer({ dest: TMP_DIR, limits: { fileSize: 50 * 1024 * 1024 } });

app.post('/upload', upload.single('file'), async (req, res) => {
  try {
    if(!req.file) return res.status(400).json({ error: "no file" });
    const { originalname, path: tmpPath } = req.file;
    const device = req.body.device || "broadcast";
    const autoplay = req.body.autoplay === "true" || req.body.autoplay === "1" || req.body.autoplay === true;
    const destName = `${Date.now()}--${originalname}`;
    const destPath = path.join(AUDIO_DIR, destName);
    fs.renameSync(tmpPath, destPath);
    log("info","uploaded file saved to", destPath);
    if(S3){
      try {
        const keyName = path.posix.join(S3_PREFIX, "uploads", destName);
        const body = fs.readFileSync(destPath);
        await S3.send(new PutObjectCommand({ Bucket: S3_BUCKET, Key: keyName, Body: body, ContentType: req.file.mimetype }));
        log("info","uploaded file to s3://", S3_BUCKET, keyName);
        if(autoplay) {
          const s3Url = await getPresignedUrlForKey(keyName, Number(SIGNED_URL_EXPIRES));
          const topic = `${device}/audio/play/control`;
          mqttClient.publish(topic, JSON.stringify({ url: s3Url, sampleRate: 16000 }), { qos: 1 }, (err) => {
            if(err) log('warn','publish play url failed', err.message || err);
            else log('info','published play url to', topic);
          });
        }
      } catch(e){ log("warn","S3 upload failed", e?.message||e); }
    }
    if(autoplay) setImmediate(()=> handlePlaybackRequest(device, destPath));
    res.json({ ok: true, file: destName });
  } catch(e){ log("error","/upload failed", e?.message||e); res.status(500).json({ error: e.message }); }
});

app.post('/play', async (req, res) => {
  try {
    const { url, device } = req.body;
    if(!url || !device) return res.status(400).json({ error: "require 'url' and 'device' in body" });
    setImmediate(()=> handlePlaybackRequest(device, url));
    res.json({ ok: true });
  } catch(e){ log("error","/play failed", e?.message || e); res.status(500).json({ error: e.message }); }
});

////////////////////////////////////////////////////////////////////////////////
// startup
////////////////////////////////////////////////////////////////////////////////
(async () => {
  try {
    if(!ensureFfmpegAvailable()) log("warn","ffmpeg not found at", FFMPEG_PATH, "— conversion won't work");
    await connectMqtt();
  } catch(e){ log("error","startup failed", e?.message || e); }
  app.listen(HTTP_PORT, () => log("info", `HTTP server listening on port ${HTTP_PORT} (audio dir: ${AUDIO_DIR})`) );

  setInterval(()=>{
    const now = Date.now();
    for(const [k, st] of recordings){ if(now - (st.lastSeen || 0) > (1000 * 60 * 10)){ log("warn","[CLEANUP] stale recording ->", k); try { fs.closeSync(st.fd); } catch(_){} try { fs.unlinkSync(st.tmpPath); } catch(_){} recordings.delete(k); } }
  }, 60_000).unref();
})();

process.on("SIGINT", async () => { log("info","SIGINT -> shutting down"); try { mqttClient?.end(true); } catch(e){} process.exit(0); });
process.on("SIGTERM", async () => { log("info","SIGTERM -> shutting down"); try { mqttClient?.end(true); } catch(e){} process.exit(0); });
