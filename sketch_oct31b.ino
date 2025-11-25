// esp32_respeaker_prod_final.ino
// Production firmware (updated playback prebuffer + robustness)

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <WiFiManager.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <LittleFS.h>
#include <Adafruit_NeoPixel.h>
#include <HTTPClient.h>
#include "driver/i2s.h"
#include "certs.h"
#include <math.h>

#ifndef THINGNAME
#error "certs.h must define THINGNAME"
#endif
#ifndef AWS_IOT_ENDPOINT
#error "certs.h must define AWS_IOT_ENDPOINT"
#endif

// -------- PINS / DEVICE --------
static const int BUTTON_PIN      = 3;    // active low
static const int I2S_BCK         = 8;
static const int I2S_WS          = 7;
static const int I2S_DATA_IN     = 44;   // RX from codec
static const int I2S_DOUT        = 43;   // TX to codec
static const int LED_PIN         = 1;
static const int LED_COUNT       = 8;    // set to 1 if you have single LED

// -------- NETWORK / MQTT --------
static const char* DEVICE_ID     = THINGNAME;
static const char* MQTT_HOST     = AWS_IOT_ENDPOINT;
static const uint16_t MQTT_PORT  = 8883;
static const unsigned long MQTT_RECONNECT_MS = 2000UL;
static const size_t MQTT_BUF_SIZE = 16384;

// -------- WIFI MANAGER --------
WiFiManager wm; // use one global instance
static const char* AP_SSID = "Rannkly Nova Speaker"; // Open portal as requested

// -------- AUDIO CAPTURE --------
static const uint32_t SAMPLE_RATE = 16000u;
static const int EXTRACTION_SHIFT = 8;           // 24-bit MSB aligned in 32b -> >>8
static const size_t I2S_READ_BYTES = 8192;
static const unsigned long RECORD_TIMEOUT_MS = 8000UL; // 8 seconds
static const unsigned long POSTROLL_MS = 200UL;

// Playback robustness
static const size_t PLAY_READ_BUF = 4 * 1024;       // 4KB read buffer (increased for better throughput)
static const size_t RING_BUFFER_SIZE = (size_t)(1.3 * 1024 * 1024); // 1.3 MB ring buffer
static const size_t INITIAL_BUFFER_SIZE = 256 * 1024; // 256KB initial buffer before starting playback
static const size_t SMALL_FILE_THRESHOLD = 256 * 1024; // Files < 256KB: full download, >= 256KB: parallel
static const unsigned long PLAY_STATS_INTERVAL_MS = 1000UL;

// Audio filters
struct Biquad {
  float b0=1,b1=0,b2=0,a1=0,a2=0;
  float x1=0,x2=0,y1=0,y2=0;
  inline float process(float x){
    float y = b0*x + b1*x1 + b2*x2 - a1*y1 - a2*y2;
    x2=x1; x1=x; y2=y1; y1=y;
    return y;
  }
  static Biquad makeNotch(float fs, float f0, float Q){
    float w0 = 2.0f * M_PI * f0 / fs;
    float alpha = sinf(w0)/(2.0f * Q);
    float cosw0 = cosf(w0);
    float b0=1, b1=-2*cosw0, b2=1;
    float a0=1+alpha, a1=-2*cosw0, a2=1-alpha;
    Biquad q; q.b0=b0/a0; q.b1=b1/a0; q.b2=b2/a0; q.a1=a1/a0; q.a2=a2/a0; return q;
  }
  static Biquad makeHPF(float fs, float fc, float Q=0.707f){
    float w0 = 2.0f * M_PI * fc / fs;
    float alpha = sinf(w0)/(2.0f * Q);
    float cosw0 = cosf(w0);
    float b0=(1+cosw0)/2, b1=-(1+cosw0), b2=(1+cosw0)/2;
    float a0=1+alpha, a1=-2*cosw0, a2=1-alpha;
    Biquad q; q.b0=b0/a0; q.b1=b1/a0; q.b2=b2/a0; q.a1=a1/a0; q.a2=a2/a0; return q;
  }
  static Biquad makeLPF(float fs, float fc, float Q=0.707f){
    float w0 = 2.0f * M_PI * fc / fs;
    float alpha = sinf(w0)/(2.0f * Q);
    float cosw0 = cosf(w0);
    float b0=(1-cosw0)/2, b1=1-cosw0, b2=(1-cosw0)/2;
    float a0=1+alpha, a1=-2*cosw0, a2=1-alpha;
    Biquad q; q.b0=b0/a0; q.b1=b1/a0; q.b2=b2/a0; q.a1=a1/a0; q.a2=a2/a0; return q;
  }
};
struct VoiceFilterChain {
  Biquad hpf, lpf;
  void init(float fs){
    // Keep processing minimal: gentle HPF and LPF only
    hpf  = Biquad::makeHPF(fs, 110.0f, 0.707f);
    lpf  = Biquad::makeLPF(fs, 4200.0f, 0.707f);
  }
  inline float process(float x){
    x = hpf.process(x);
    x = lpf.process(x);
    return x;
  }
} g_voice;
struct SmoothNoiseGate {
  float noiseFloor = 80.0f;
  float targetSNR = 4.0f;       // dB (gentler open threshold)
  float attack = 0.08f;         // s
  float release = 0.80f;        // s (longer close to avoid chattering)
  float g = 0.0f;               // 0..1
  void reset(){ g = 0.0f; }
  inline float step(float rms, float fs, size_t n){
    float alphaNoise = 1.0f - expf(-(float)n / (fs * 1.5f));
    if(rms < noiseFloor * 1.6f){
      noiseFloor = (1.0f - alphaNoise) * noiseFloor + alphaNoise * rms;
      if(noiseFloor < 10.0f) noiseFloor = 10.0f;
    }
    float open = noiseFloor * powf(10.0f, targetSNR/20.0f);
    float target = 0.0f;
    if(rms <= noiseFloor) target = 0.0f;
    else if(rms >= open) target = 1.0f;
    else target = (rms - noiseFloor) / (open - noiseFloor);
    float atk = 1.0f - expf(-(float)n / (fs * attack));
    float rel = 1.0f - expf(-(float)n / (fs * release));
    float a = (target > g) ? atk : rel;
    g = g + a * (target - g);
    return g;
  }
} g_gate;

// Normalization
static const float NORMALIZE_TARGET_PEAK = 20000.0f;
static const float MAX_DIGITAL_GAIN      = 6.0f;

// -------- CHUNK UPLOAD --------
static const size_t CHUNK_PUBLISH_SIZE = 8192;
static const unsigned long CHUNK_PUBLISH_DELAY_MS = 20UL;
static const char* SESSION_COUNTER_PATH = "/session_counter";

// -------- BUTTON / UX --------
static const unsigned long DEBOUNCE_MS = 12UL;
static const unsigned long SHORT_PRESS_MAX_MS = 350UL;
static const unsigned long LONG_PRESS_MS = 10000UL;
static const unsigned long IDLE_ANIM_AFTER_MS = 15000UL;
static const unsigned long IDLE_ANIM_STEP_MS = 80UL;

// -------- GLOBALS --------
WiFiClientSecure tlsClient;
PubSubClient mqttClient(tlsClient);
Adafruit_NeoPixel pixels(LED_COUNT, LED_PIN, NEO_GRB + NEO_KHZ800);

volatile bool recorderInstalled = false;
volatile bool playbackActive = false;
volatile bool isRecording = false;
unsigned long sessionCounter = 0;
unsigned long lastMQTTAttempt = 0;
unsigned long lastActivityMs = 0;
String lastPlayedUrl = ""; // Track last played URL to prevent auto-replay
unsigned long lastPlaybackEndTime = 0; // Track when playback ended
const unsigned long PLAYBACK_COOLDOWN_MS = 2000; // 2 second cooldown after playback ends

// Button FSM
enum BtnPhase { BP_IDLE=0, BP_DEBOUNCE, BP_PRESSED };
BtnPhase btnPhase = BP_IDLE;
unsigned long pressTs = 0;
unsigned long releaseTs = 0;

// LED states
enum LedState { LS_BOOT, LS_PROVISION, LS_DISCONNECTED, LS_CONNECTED, LS_IDLE_ANIM, LS_RECORDING, LS_STREAMING, LS_PLAYING, LS_RANDOM_RGB, LS_ERROR, LS_RESET };
volatile LedState ledState = LS_BOOT;
unsigned long idleAnimTick = 0;
uint16_t rainbowPos = 0;
unsigned long lastRandomRgbChange = 0;

// Topics
String TOP_PLAY_CTRL;

// -------- LED helpers --------
void ledInit(){ pixels.begin(); pixels.setBrightness(120); pixels.show(); }
void ledFill(uint8_t r, uint8_t g, uint8_t b){
  for(int i=0;i<LED_COUNT;i++) pixels.setPixelColor(i, pixels.Color(r,g,b));
  pixels.show();
}
void ledRainbowStepFast(){
  static unsigned long lastRainbowUpdate = 0;
  unsigned long now = millis();
  // Update rainbow every 20ms (like reference firmware) for smooth but fast animation
  if(now - lastRainbowUpdate < 20) return;
  lastRainbowUpdate = now;
  
  rainbowPos++; // Increment by 1 (not 2) for smoother animation
  for(int i=0;i<LED_COUNT;i++){
    uint8_t idx = (i * 256 / max(1,LED_COUNT) + rainbowPos) & 0xFF;
    pixels.setPixelColor(i, pixels.ColorHSV(idx * 256));
  }
  pixels.setBrightness(120);
  pixels.show();
}
void ledIdleAmbientStep(){
  static uint16_t base = 0;
  base++;
  for(int i=0;i<LED_COUNT;i++){
    uint16_t hue = (base + i*12) % 360;
    pixels.setPixelColor(i, pixels.ColorHSV(hue * 182));
  }
  pixels.setBrightness(80);
  pixels.show();
}
void ledRandomRgb(){
  static uint8_t r = 0, g = 0, b = 0;
  static unsigned long lastChange = 0;
  unsigned long now = millis();
  if(now - lastChange > 500){ // Change every 500ms
    r = (uint8_t)(esp_random() % 256);
    g = (uint8_t)(esp_random() % 256);
    b = (uint8_t)(esp_random() % 256);
    lastChange = now;
  }
  ledFill(r, g, b);
}

// -------- FS helpers --------
bool ensureLittleFS(){
  if(!LittleFS.begin(true)){ Serial.println("[FS] mount fail"); return false; }
  
  // Print LittleFS size information
  size_t totalBytes = LittleFS.totalBytes();
  size_t usedBytes = LittleFS.usedBytes();
  size_t freeBytes = totalBytes - usedBytes;
  Serial.printf("[FS] LittleFS: total=%u bytes (%.2f MB), used=%u bytes (%.2f MB), free=%u bytes (%.2f MB)\n",
    (unsigned)totalBytes, totalBytes / (1024.0f * 1024.0f),
    (unsigned)usedBytes, usedBytes / (1024.0f * 1024.0f),
    (unsigned)freeBytes, freeBytes / (1024.0f * 1024.0f));
  
  if(LittleFS.exists(SESSION_COUNTER_PATH)){
    File f = LittleFS.open(SESSION_COUNTER_PATH, FILE_READ);
    if(f){ sessionCounter = (unsigned long)f.parseInt(); f.close(); }
  }
  return true;
}
void persistSessionCounter(){
  File f = LittleFS.open(SESSION_COUNTER_PATH, FILE_WRITE);
  if(f){ f.printf("%lu", sessionCounter); f.close(); }
}

// -------- I2S recorder (RX) --------
void setupI2SRecorder(){
  if(recorderInstalled) return;
  i2s_config_t cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_SLAVE | I2S_MODE_RX),
    .sample_rate = SAMPLE_RATE,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_32BIT,
    .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
    .communication_format = (i2s_comm_format_t)(I2S_COMM_FORMAT_I2S | I2S_COMM_FORMAT_I2S_MSB),
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
    .dma_buf_count = 12,
    .dma_buf_len = 1024,
    .use_apll = false,
    .tx_desc_auto_clear = false,
    .fixed_mclk = 0
  };
  i2s_pin_config_t pins = {
    .bck_io_num = I2S_BCK,
    .ws_io_num = I2S_WS,
    .data_out_num = I2S_PIN_NO_CHANGE,
    .data_in_num = I2S_DATA_IN
  };
  if(i2s_driver_install(I2S_NUM_0, &cfg, 0, NULL) != ESP_OK){
    Serial.println("[I2S] RX install failed");
    recorderInstalled = false;
    return;
  }
  i2s_set_pin(I2S_NUM_0, &pins);
  i2s_zero_dma_buffer(I2S_NUM_0);
  recorderInstalled = true;
  Serial.println("[I2S] RX installed");
}
void uninstallI2SRecorder(){
  if(!recorderInstalled) return;
  i2s_driver_uninstall(I2S_NUM_0);
  recorderInstalled = false;
  pinMode(I2S_BCK, INPUT); pinMode(I2S_WS, INPUT); pinMode(I2S_DATA_IN, INPUT);
  delay(10);
}

// -------- I2S playback (TX) --------
bool installI2STX(uint32_t sampleRate){
  // Uninstall existing driver first with proper cleanup
  if(i2s_driver_uninstall(I2S_NUM_1) == ESP_OK){
    delay(50); // Allow driver to fully uninstall
  }
  
  i2s_config_t cfg = {
    .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX),
    .sample_rate = (int)sampleRate,
    .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
    .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
    .communication_format = I2S_COMM_FORMAT_STAND_I2S,
    .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1, // Use interrupt flag for better performance
    .dma_buf_count = 16,  // Increased from 10 for smoother streaming
    .dma_buf_len = 512,   // Reduced from 1024 for lower latency
    .use_apll = false,
    .tx_desc_auto_clear = true,
    .fixed_mclk = 0
  };
  i2s_pin_config_t pins = {
    .bck_io_num = I2S_BCK,
    .ws_io_num = I2S_WS,
    .data_out_num = I2S_DOUT,
    .data_in_num = I2S_PIN_NO_CHANGE
  };
  
  esp_err_t err = i2s_driver_install(I2S_NUM_1, &cfg, 0, NULL);
  if(err != ESP_OK){
    Serial.printf("[I2S] TX install failed: %d\n", err);
    return false;
  }
  
  err = i2s_set_pin(I2S_NUM_1, &pins);
  if(err != ESP_OK){
    Serial.printf("[I2S] TX set_pin failed: %d\n", err);
    i2s_driver_uninstall(I2S_NUM_1);
    return false;
  }
  
  i2s_zero_dma_buffer(I2S_NUM_1);
  delay(10); // Small delay to ensure I2S is ready
  Serial.printf("[I2S] TX ready @%u Hz (DMA: %d buffers x %d bytes)\n", 
    (unsigned)sampleRate, cfg.dma_buf_count, cfg.dma_buf_len);
  return true;
}
void uninstallI2STX(){
  i2s_stop(I2S_NUM_1); // Stop I2S before uninstalling
  i2s_driver_uninstall(I2S_NUM_1);
  delay(20); // Allow time for cleanup
  pinMode(I2S_BCK, INPUT); pinMode(I2S_WS, INPUT); pinMode(I2S_DOUT, INPUT);
}

// -------- MQTT helpers --------
bool connectMQTT(){
  if(mqttClient.connected()) return true;
  if(!WiFi.isConnected()) return false;
  if(strlen(AWS_CERT_CA) > 10) tlsClient.setCACert(AWS_CERT_CA);
  if(strlen(AWS_CERT_CRT) > 10 && strlen(AWS_CERT_PRIVATE) > 10){
    tlsClient.setCertificate(AWS_CERT_CRT);
    tlsClient.setPrivateKey(AWS_CERT_PRIVATE);
  }
  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  mqttClient.setBufferSize(MQTT_BUF_SIZE);
  char cid[48]; snprintf(cid, sizeof(cid), "%s-%04X", DEVICE_ID, (uint16_t)esp_random());
  Serial.printf("[MQTT] connecting as %s ...\n", cid);
  if(mqttClient.connect(cid)){
    String playCtrl = String(DEVICE_ID) + "/audio/play/control";
    mqttClient.subscribe(playCtrl.c_str(), 1);
    Serial.println("[MQTT] connected");
    return true;
  }
  Serial.printf("[MQTT] connect rc=%d\n", mqttClient.state());
  return false;
}

// -------- Recording helpers (unchanged) --------
String makeSessionPath(const char* sid){ String fn="/session_"; fn+=sid; fn+=".pcm"; return fn; }
static inline int16_t processStereoToMonoS16(int32_t left32, int32_t right32){
  int64_t avg = ((int64_t)left32 + (int64_t)right32) / 2;
  int32_t s32 = (int32_t)(avg >> EXTRACTION_SHIFT);
  float s = (float)s32;
  float y = g_voice.process(s);
  if(y > 32767.0f) y = 32767.0f;
  if(y < -32768.0f) y = -32768.0f;
  return (int16_t)lroundf(y);
}

bool publishChunkWithRetries(const char* sid, uint32_t seq, const uint8_t* data, size_t len){
  const size_t total = 4 + len;
  uint8_t *buf = (uint8_t*)malloc(total);
  if(!buf) return false;
  buf[0] = (uint8_t)(seq & 0xff);
  buf[1] = (uint8_t)((seq>>8)&0xff);
  buf[2] = (uint8_t)((seq>>16)&0xff);
  buf[3] = (uint8_t)((seq>>24)&0xff);
  memcpy(buf+4, data, len);
  String top = String(DEVICE_ID) + "/audio/" + sid + "/chunk";
  bool ok=false;
  for(int attempt=0; attempt<6; attempt++){
    if(!mqttClient.connected()){
      connectMQTT();
      unsigned long t0 = millis();
      while(!mqttClient.connected() && millis() - t0 < 1500){ mqttClient.loop(); delay(20); }
    }
    if(mqttClient.connected()){
      ok = mqttClient.publish(top.c_str(), buf, (unsigned int)total, false);
      if(ok) break;
    }
    delay(150);
  }
  free(buf);
  if(!ok) Serial.printf("[MQTT] publish chunk seq=%u failed\n", (unsigned)seq);
  return ok;
}
void publishControlStart(const char* sid, uint32_t totalChunks, uint32_t chunkBytes){
  StaticJsonDocument<256> doc;
  doc["start"]=true; doc["session"]=sid; doc["sampleRate"]=SAMPLE_RATE; doc["channels"]=1;
  doc["totalChunks"]=totalChunks; doc["chunkBytes"]=chunkBytes;
  char out[256]; size_t n = serializeJson(doc, out, sizeof(out));
  String top = String(DEVICE_ID) + "/audio/" + sid + "/control";
  mqttClient.publish(top.c_str(), (uint8_t*)out, (unsigned)n, false);
}
void publishControlFinal(const char* sid, uint32_t totalChunks){
  StaticJsonDocument<256> doc;
  doc["final"]=true; doc["session"]=sid; doc["totalChunks"]=totalChunks; doc["sampleRate"]=SAMPLE_RATE; doc["channels"]=1;
  char out[256]; size_t n = serializeJson(doc, out, sizeof(out));
  String top = String(DEVICE_ID) + "/audio/" + sid + "/control";
  mqttClient.publish(top.c_str(), (uint8_t*)out, (unsigned)n, true);
}

void recordTimedToFile(const char* sid, uint32_t timeoutMs){
  String fn = makeSessionPath(sid);
  if(LittleFS.exists(fn)) LittleFS.remove(fn);
  { File f = LittleFS.open(fn, FILE_WRITE); if(!f){ Serial.printf("[FS] cannot create %s\n", fn.c_str()); return; } }

  uint8_t *i2sBuf = (uint8_t*)malloc(I2S_READ_BYTES);
  if(!i2sBuf){ Serial.println("[ALLOC] i2sBuf failed"); return; }
  size_t bytesRead = 0;
  unsigned long tStop = millis() + timeoutMs;
  Serial.printf("[REC] session %s -> %s (timeout %ums)\n", sid, fn.c_str(), (unsigned)timeoutMs);
  ledState = LS_RECORDING; ledFill(0,0,255);
  g_gate.reset();

  while(millis() < tStop){
    esp_err_t r = i2s_read(I2S_NUM_0, i2sBuf, I2S_READ_BYTES, &bytesRead, 200 / portTICK_PERIOD_MS);
    if(r == ESP_OK && bytesRead > 0){
      size_t stereoFrames = bytesRead / 8;
      if(stereoFrames == 0) continue;
      size_t outBytes = stereoFrames * 2;
      int16_t *out = (int16_t*)malloc(outBytes);
      if(!out){ Serial.println("[ALLOC] out fail"); break; }
      int32_t *s32 = (int32_t*)i2sBuf;
      for(size_t i=0;i<stereoFrames;i++){
        int32_t l32 = s32[2*i + 1];
        int32_t r32 = s32[2*i + 0];
        out[i] = processStereoToMonoS16(l32, r32);
      }
      // chunk RMS for adaptive gating
      double acc=0.0; for(size_t i=0;i<stereoFrames;i++){ double v=out[i]; acc+=v*v; }
      float rms = stereoFrames ? sqrtf(acc/(float)stereoFrames) : 0.0f;
      float gate = g_gate.step(rms, (float)SAMPLE_RATE, stereoFrames);
      if(gate <= 0.02f) memset(out, 0, outBytes);
      else if(gate < 0.98f){
        for(size_t i=0;i<stereoFrames;i++){
          float v = (float)out[i] * gate;
          if(v>32767.0f) v=32767.0f; if(v<-32768.0f) v=-32768.0f;
          out[i] = (int16_t)lrintf(v);
        }
      }
      File fw = LittleFS.open(fn, FILE_APPEND);
      if(fw){ fw.write((uint8_t*)out, outBytes); fw.close(); } else { Serial.println("[FS] append open fail"); }
      free(out);
      lastActivityMs = millis();
    } else if(r != ESP_OK){
      Serial.printf("[I2S] read err %d\n", (int)r);
      delay(5);
    }
    if(mqttClient.connected()) mqttClient.loop();
  }
  free(i2sBuf);
  isRecording = false;
  Serial.println("[REC] saved");
  unsigned long pr = millis();
  while(millis() - pr < POSTROLL_MS){ if(mqttClient.connected()) mqttClient.loop(); delay(5); }
}

// Analyze to compute normalization gain (peak)
bool analyzeFilePeak(const char* path, int32_t &outPeak){
  File f = LittleFS.open(path, FILE_READ);
  if(!f) return false;
  int32_t peak = 1;
  const size_t BUF=4096; int16_t *buf=(int16_t*)malloc(BUF);
  if(!buf){ f.close(); return false; }
  while(true){
    size_t r = f.read((uint8_t*)buf, BUF);
    if(r==0) break;
    size_t n = r/2;
    for(size_t i=0;i<n;i++){ int32_t v=abs((int32_t)buf[i]); if(v>peak) peak=v; }
  }
  free(buf); f.close();
  outPeak = peak<1?1:peak;
  return true;
}

void streamSessionFileToMQTT(const char* sid){
  String fn = makeSessionPath(sid);
  if(!LittleFS.exists(fn)){ Serial.printf("[FS] missing %s\n", fn.c_str()); return; }
  File f = LittleFS.open(fn, FILE_READ);
  if(!f){ Serial.printf("[FS] open fail %s\n", fn.c_str()); return; }
  size_t fileLen = f.size();
  if(fileLen == 0){ f.close(); Serial.println("[FS] empty"); return; }

  uint32_t chunkBytes = (uint32_t)CHUNK_PUBLISH_SIZE;
  uint32_t totalChunks = (fileLen + chunkBytes - 1) / chunkBytes;

  unsigned long startTry = millis();
  while(!mqttClient.connected()){
    if(millis() - startTry > 10000){ Serial.println("[MQTT] cannot connect - abort stream"); f.close(); return; }
    if(millis() - lastMQTTAttempt > MQTT_RECONNECT_MS){ lastMQTTAttempt = millis(); connectMQTT(); }
    mqttClient.loop(); delay(200);
  }
  publishControlStart(sid, totalChunks, chunkBytes);
  delay(10);

  uint8_t *readBuf = (uint8_t*)malloc(chunkBytes);
  if(!readBuf){ Serial.println("[ALLOC] readBuf fail"); f.close(); return; }
  ledState = LS_STREAMING;

  uint32_t seq=0;
  while(f.available()){
    size_t toRead = f.read(readBuf, chunkBytes);
    if(toRead==0) break;
    publishChunkWithRetries(sid, seq++, readBuf, toRead);
    unsigned long t0 = millis();
    while(millis() - t0 < CHUNK_PUBLISH_DELAY_MS){ if(mqttClient.connected()) mqttClient.loop(); delay(1); }
    ledRainbowStepFast();
  }
  free(readBuf); f.close();
  publishControlFinal(sid, seq);
  unsigned long flush=millis(); while(millis()-flush<600){ mqttClient.loop(); delay(10); }
  if(LittleFS.exists(fn)) { 
    LittleFS.remove(fn); 
    Serial.printf("[FS] removed %s\n", fn.c_str()); 
  }
  Serial.printf("[STREAM] session %s sent (%u chunks)\n", sid, (unsigned)seq);
  // After streaming: Random RGB, then idle animation after 15s
  ledState = LS_RANDOM_RGB;
  lastActivityMs = millis();
  lastRandomRgbChange = millis();
}

// -------- Ring Buffer for parallel download/playback --------
struct RingBuffer {
  uint8_t *data;
  size_t size;
  volatile size_t writePos;
  volatile size_t readPos;
  volatile size_t availableBytes; // bytes available to read
  
  bool init(size_t sz) {
    data = (uint8_t*)malloc(sz);
    if(!data) return false;
    size = sz;
    writePos = 0;
    readPos = 0;
    availableBytes = 0;
    return true;
  }
  
  void deinit() {
    if(data) { free(data); data = nullptr; }
    size = 0;
    writePos = 0;
    readPos = 0;
    availableBytes = 0;
  }
  
  size_t available() { return availableBytes; } // Getter function
  size_t space() { return size - availableBytes; } // space available to write
  
  size_t write(const uint8_t *src, size_t len) {
    if(len == 0) return 0;
    size_t availSpace = space();
    size_t toWrite = (len < availSpace) ? len : availSpace;
    if(toWrite == 0) return 0;
    
    size_t wp = writePos;
    size_t firstPart = (toWrite < (size - wp)) ? toWrite : (size - wp);
    memcpy(data + wp, src, firstPart);
    writePos = (wp + firstPart) % size;
    
    if(toWrite > firstPart) {
      memcpy(data + writePos, src + firstPart, toWrite - firstPart);
      writePos = (writePos + toWrite - firstPart) % size;
    }
    
    availableBytes += toWrite;
    return toWrite;
  }
  
  size_t read(uint8_t *dst, size_t len) {
    if(len == 0 || availableBytes == 0) return 0;
    size_t avail = availableBytes;
    size_t toRead = (len < avail) ? len : avail;
    
    size_t rp = readPos;
    size_t firstPart = (toRead < (size - rp)) ? toRead : (size - rp);
    memcpy(dst, data + rp, firstPart);
    readPos = (rp + firstPart) % size;
    
    if(toRead > firstPart) {
      memcpy(dst + firstPart, data + readPos, toRead - firstPart);
      readPos = (readPos + toRead - firstPart) % size;
    }
    
    availableBytes -= toRead;
    return toRead;
  }
  
  void clear() {
    writePos = 0;
    readPos = 0;
    availableBytes = 0;
  }
};

// -------- WAV (HTTPS) playback WITH PREBUFFER --------
static int findDataChunk(const uint8_t *buf, size_t len){
  for(size_t i=0;i+4<=len;i++){ if(buf[i]=='d'&&buf[i+1]=='a'&&buf[i+2]=='t'&&buf[i+3]=='a') return (int)i; }
  return -1;
}

void playUrlWavBlocking(const String &url, uint32_t sampleRateOverride){
  unsigned long playbackStartMs = millis();
  Serial.printf("[PLAY][START] url=%s ringBufferSize=%.1fMB\n", 
    url.substring(0, 60).c_str(), 
    RING_BUFFER_SIZE / (1024.0f * 1024.0f));
  WiFiClientSecure secure;
  secure.setInsecure(); // presigned S3 uses public CA; allow default, or setCACert if needed
  HTTPClient http;
  http.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
  http.setTimeout(15000);
  if(!http.begin(secure, url)){ Serial.println("[HTTP] begin failed"); return; }
  int code = http.GET();
  if(code != HTTP_CODE_OK && code != HTTP_CODE_PARTIAL_CONTENT){
    Serial.printf("[HTTP] GET %d\n", code);
    http.end(); return;
  }
  
  // Get Content-Length if available for accurate end-of-stream detection
  int contentLength = http.getSize();
  
  WiFiClient *stream = http.getStreamPtr();
  if(!stream){ Serial.println("[HTTP] no stream"); http.end(); return; }

  // Read header until "data" chunk (or max)
  const size_t HDR_MAX = 16*1024;
  uint8_t *hdr = (uint8_t*)malloc(HDR_MAX);
  if(!hdr){ http.end(); return; }
  size_t hdrPos=0; int dataAt=-1;
  unsigned long stallStart = millis();
  while(hdrPos < HDR_MAX && millis() - stallStart < 5000){
    while(stream->available() && hdrPos<HDR_MAX){
      int c = stream->read(); if(c<0) break;
      hdr[hdrPos++] = (uint8_t)c;
    }
    dataAt = findDataChunk(hdr, hdrPos);
    if(dataAt>=0) break;
    if(!stream->connected()) break;
    delay(2);
  }
  if(dataAt<0){ Serial.println("[PLAY] not a WAV (no data chunk)"); free(hdr); http.end(); return; }
  
  // Determine expected total bytes (Content-Length minus header we already read)
  size_t expectedTotalBytes = 0;
  if(contentLength > 0){
    expectedTotalBytes = (size_t)contentLength;
    Serial.printf("[HTTP] Content-Length: %d bytes (%.1f kB), header=%u bytes\n", 
      contentLength, contentLength / 1024.0f, (unsigned)hdrPos);
  } else {
    Serial.println("[HTTP] Content-Length not available - using stream detection");
  }

  uint32_t sampleRate = SAMPLE_RATE; int channels = 1;
  if(hdrPos >= 24){
    int fmtPos = -1;
    // Find "fmt " chunk (WAV format specification)
    for(size_t i=0;i+4<=hdrPos;i++){ 
      if(hdr[i]=='f'&&hdr[i+1]=='m'&&hdr[i+2]=='t'&&hdr[i+3]==' '){ 
        fmtPos=(int)i; 
        break; 
      } 
    }
    if(fmtPos>=0 && fmtPos+16 <= (int)hdrPos){
      // Parse WAV format chunk (little-endian)
      // Offset 10-11: NumChannels (uint16_t)
      channels = hdr[fmtPos + 10] | (hdr[fmtPos + 11] << 8);
      // Offset 12-15: SampleRate (uint32_t)
      sampleRate = (uint32_t)hdr[fmtPos + 12] | 
                   ((uint32_t)hdr[fmtPos+13]<<8) | 
                   ((uint32_t)hdr[fmtPos+14]<<16) | 
                   ((uint32_t)hdr[fmtPos+15]<<24);
      
      // Validate parsed values to prevent breaking
      if(channels < 1 || channels > 2) channels = 1;
      if(sampleRate < 8000 || sampleRate > 48000) sampleRate = SAMPLE_RATE;
      
      Serial.printf("[PLAY] WAV format: %uHz, %d channel(s)\n", (unsigned)sampleRate, channels);
    } else {
      Serial.println("[PLAY] WAV fmt chunk not found or incomplete - using defaults");
    }
  }
  if(sampleRateOverride) sampleRate = sampleRateOverride;

  // Install I2S TX for playback
  if(!installI2STX(sampleRate)){ free(hdr); http.end(); return; }

  struct StereoScratch {
    uint8_t *buf = nullptr;
    size_t cap = 0;
    ~StereoScratch(){
      if(buf){
        free(buf);
        buf = nullptr;
        cap = 0;
      }
    }
    bool ensure(size_t need){
      if(cap >= need) return true;
      uint8_t *tmp = (uint8_t*)realloc(buf, need);
      if(!tmp) return false;
      buf = tmp;
      cap = need;
      return true;
    }
  } stereoScratch;

  // Note: writeMonoAsStereo lambda removed - now handled in main playback loop

  // Determine pcm start within hdr buffer
  size_t pcmStart = dataAt + 8; // "data"+len(4) -> payload
  size_t firstBody = hdrPos > pcmStart ? hdrPos - pcmStart : 0;

  // Initialize ring buffer FIRST - use it for entire playback (prebuffer + streaming)
  RingBuffer ringBuf;
  if(!ringBuf.init(RING_BUFFER_SIZE)){
    Serial.println("[PLAY] ring buffer alloc failed");
    uninstallI2STX(); free(hdr); http.end(); return;
  }
  
  const size_t READ_BUF = PLAY_READ_BUF;
  uint8_t *httpReadBuf = (uint8_t*)malloc(READ_BUF);
  uint8_t *playbackBuf = (uint8_t*)malloc(READ_BUF * 2); // Larger for stereo conversion
  if(!httpReadBuf || !playbackBuf){
    ringBuf.deinit();
    if(httpReadBuf) free(httpReadBuf);
    if(playbackBuf) free(playbackBuf);
    uninstallI2STX(); free(hdr); http.end(); return;
  }

  // Streaming telemetry
  size_t statsTotalHttp = 0;
  size_t statsTotalI2S = 0;
  size_t statsWindowHttp = 0;
  size_t statsWindowI2S = 0;
  uint32_t httpChunkCount = 0;
  uint32_t i2sChunkCount = 0;
  size_t httpBytesDownloadedMono = 0; // Track mono-equivalent bytes downloaded
  size_t i2sBytesPlayedMono = 0; // Track mono-equivalent bytes played
  unsigned long statsWindowStart = millis();
  unsigned long lastUnderrunWarning = 0;
  const size_t CHUNK_SIZE_LOG = 8192; // 8KB logical chunks for logging
  const size_t MIN_BUFFER_WARNING = 8 * 1024; // Warn if buffer < 8KB
  
  // Put firstBody into ring buffer immediately
  if(firstBody > 0){
    size_t written = ringBuf.write(hdr + pcmStart, firstBody);
    statsTotalHttp += written;
    statsWindowHttp += written;
    httpBytesDownloadedMono += written;
    httpChunkCount += ((written + CHUNK_SIZE_LOG - 1) / CHUNK_SIZE_LOG);
  }
  
  // Determine if file is small (< 256KB) or large (>= 256KB)
  bool isSmallFile = false;
  if(expectedTotalBytes > 0){
    size_t headerSize = pcmStart;
    size_t expectedDataBytes = expectedTotalBytes > headerSize ? (expectedTotalBytes - headerSize) : expectedTotalBytes;
    isSmallFile = (expectedDataBytes < SMALL_FILE_THRESHOLD);
    Serial.printf("[PLAY] File size: %.1fkB - %s\n", 
      expectedDataBytes / 1024.0f,
      isSmallFile ? "SMALL (full download)" : "LARGE (parallel download+playback)");
  } else {
    // No Content-Length - assume large file, use parallel approach
    isSmallFile = false;
    Serial.println("[PLAY] No Content-Length - using parallel download+playback");
  }
  
  // ===== PHASE 1: INITIAL BUFFER OR FULL DOWNLOAD =====
  unsigned long downloadStart = millis();
  bool downloadComplete = false;
  bool playbackStarted = false;
  size_t totalDownloaded = firstBody;
  
  if(isSmallFile){
    // Small file: Download entire file first, then play
    Serial.printf("[PLAY][DOWNLOAD] Small file - downloading fully to ring buffer (max %.1fMB)\n", 
      RING_BUFFER_SIZE / (1024.0f * 1024.0f));
    
    while(stream->connected() && !downloadComplete){
      int avail = stream->available();
      size_t ringSpace = ringBuf.space();
      
      if(avail > 0 && ringSpace > 0){
        int toRead = (avail < (int)READ_BUF) ? avail : (int)READ_BUF;
        if(toRead > (int)ringSpace) toRead = (int)ringSpace;
        
        int r = stream->readBytes(httpReadBuf, toRead);
        if(r > 0){
          size_t written = ringBuf.write(httpReadBuf, r);
          statsTotalHttp += written;
          statsWindowHttp += written;
          httpBytesDownloadedMono += written;
          httpChunkCount += ((written + CHUNK_SIZE_LOG - 1) / CHUNK_SIZE_LOG);
          totalDownloaded += written;
        }
      } else if(avail <= 0 && !stream->connected()){
        if(expectedTotalBytes > 0){
          size_t headerSize = pcmStart;
          size_t expectedDataBytes = expectedTotalBytes > headerSize ? (expectedTotalBytes - headerSize) : expectedTotalBytes;
          if(httpBytesDownloadedMono >= expectedDataBytes || 
             (expectedDataBytes - httpBytesDownloadedMono) < 4096){
            downloadComplete = true;
          }
        } else {
          delay(1000);
          if(!stream->connected() && stream->available() == 0){
            downloadComplete = true;
          }
        }
      }
      
      if(mqttClient.connected() && (millis() % 200 < 10)){
        mqttClient.loop();
      }
      delay(1);
    }
    
    Serial.printf("[PLAY][DOWNLOAD] Small file download complete: %.1fkB in %.2fs\n",
      totalDownloaded / 1024.0f, (millis() - downloadStart) / 1000.0f);
  } else {
    // Large file: Download 256KB initial buffer, then start playback while continuing download
    Serial.printf("[PLAY][DOWNLOAD] Large file - downloading initial buffer (%.1fkB)\n",
      INITIAL_BUFFER_SIZE / 1024.0f);
    
    while(stream->connected() && ringBuf.available() < INITIAL_BUFFER_SIZE){
      int avail = stream->available();
      size_t ringSpace = ringBuf.space();
      
      if(avail > 0 && ringSpace > 0){
        int toRead = (avail < (int)READ_BUF) ? avail : (int)READ_BUF;
        if(toRead > (int)ringSpace) toRead = (int)ringSpace;
        
        int r = stream->readBytes(httpReadBuf, toRead);
        if(r > 0){
          size_t written = ringBuf.write(httpReadBuf, r);
          statsTotalHttp += written;
          statsWindowHttp += written;
          httpBytesDownloadedMono += written;
          httpChunkCount += ((written + CHUNK_SIZE_LOG - 1) / CHUNK_SIZE_LOG);
          totalDownloaded += written;
        }
      }
      
      if(mqttClient.connected() && (millis() % 200 < 10)){
        mqttClient.loop();
      }
      delay(1);
    }
    
    Serial.printf("[PLAY][DOWNLOAD] Initial buffer ready: %.1fkB in %.2fs - starting playback\n",
      ringBuf.available() / 1024.0f, (millis() - downloadStart) / 1000.0f);
  }
  
  if(ringBuf.available() == 0){
    Serial.println("[PLAY] ERROR: Ring buffer is empty!");
    ringBuf.deinit();
    free(httpReadBuf);
    free(playbackBuf);
    free(hdr);
    http.end();
    uninstallI2STX();
    return;
  }
  
  // ===== PHASE 2: STREAM FROM RING BUFFER TO I2S (with parallel download for large files) =====
  Serial.printf("[PLAY][STREAM] Starting playback from ring buffer (%.1fkB)\n",
    ringBuf.available() / 1024.0f);
  
  // DO NOT set ledState here - rainbow will start only when first I2S write succeeds
  bool rainbowStarted = false;
  
  auto logPlaybackStats = [&](const char* phase, int streamAvailable = -1, bool isPaused = false, size_t ringBufFill = 0) {
    unsigned long now = millis();
    unsigned long elapsed = now - statsWindowStart;
    if(elapsed >= PLAY_STATS_INTERVAL_MS) {
      float sec = elapsed / 1000.0f;
      float netKBps = sec > 0 ? (statsWindowHttp / 1024.0f) / sec : 0.0f;
      float playKBps = sec > 0 ? (statsWindowI2S / 1024.0f) / sec : 0.0f;
      float httpChunksPerSec = sec > 0 ? (httpChunkCount / sec) : 0.0f;
      float i2sChunksPerSec = sec > 0 ? (i2sChunkCount / sec) : 0.0f;
      
      // Use ring buffer fill (most accurate)
      size_t bufferFillBytes = ringBufFill;
      float bufferFillKB = bufferFillBytes / 1024.0f;
      uint32_t bufferChunks = bufferFillBytes / CHUNK_SIZE_LOG;
      
      // Check for buffer underrun risk
      bool lowBuffer = bufferFillBytes < MIN_BUFFER_WARNING;
      if(lowBuffer && (now - lastUnderrunWarning) > 2000) { // Warn max once per 2s
        Serial.printf("[PLAY][WARN] LOW_BUFFER bufferFill=%.1fkB downloadSpeed=%.1fkB/s playbackSpeed=%.1fkB/s streamAvailable=%d\n",
          bufferFillKB, netKBps, playKBps, streamAvailable);
        lastUnderrunWarning = now;
      }
      
      float speedRatio = playKBps > 0 ? (netKBps / playKBps) : 0.0f;
      const char* pauseStatus = isPaused ? "PAUSED" : "PLAYING";
      Serial.printf("[PLAY][STAT] %s %s win=%.2fs net=%.1fkB/s play=%.1fkB/s speedRatio=%.2f httpChunks=%u i2sChunks=%u httpChunks/s=%.1f i2sChunks/s=%.1f ringBufFill=%.1fkB bufferChunks=%u streamAvail=%d totalDl=%.1fkB totalPlay=%.1fkB\n",
        phase,
        pauseStatus,
        sec,
        netKBps,
        playKBps,
        speedRatio,
        httpChunkCount,
        i2sChunkCount,
        httpChunksPerSec,
        i2sChunksPerSec,
        bufferFillKB,
        bufferChunks,
        streamAvailable,
        statsTotalHttp / 1024.0f,
        statsTotalI2S / 1024.0f);
      statsWindowHttp = 0;
      statsWindowI2S = 0;
      httpChunkCount = 0;
      i2sChunkCount = 0;
      statsWindowStart = now;
    }
  };

  // Main playback loop - stream from ring buffer to I2S (with parallel download for large files)
  bool playbackComplete = false;
  
  while(!playbackComplete){
    unsigned long now = millis();
    size_t ringBufferFill = ringBuf.available();
    
    // ===== PARALLEL DOWNLOAD TASK (for large files only) =====
    if(!isSmallFile && stream->connected() && !downloadComplete){
      int avail = stream->available();
      size_t ringSpace = ringBuf.space();
      
      // Priority: Always try to download when data is available
      // This ensures continuous download to prevent buffer underrun
      if(avail > 0 && ringSpace > 0){
        int toRead = (avail < (int)READ_BUF) ? avail : (int)READ_BUF;
        if(toRead > (int)ringSpace) toRead = (int)ringSpace;
        
        // Read aggressively to maintain buffer level
        int r = stream->readBytes(httpReadBuf, toRead);
        if(r > 0){
          size_t written = ringBuf.write(httpReadBuf, r);
          statsTotalHttp += written;
          statsWindowHttp += written;
          httpBytesDownloadedMono += written;
          httpChunkCount += ((written + CHUNK_SIZE_LOG - 1) / CHUNK_SIZE_LOG);
          totalDownloaded += written;
        }
      } else if(avail <= 0 && !stream->connected()){
        // Check if download is complete
        if(expectedTotalBytes > 0){
          size_t headerSize = pcmStart;
          size_t expectedDataBytes = expectedTotalBytes > headerSize ? (expectedTotalBytes - headerSize) : expectedTotalBytes;
          if(httpBytesDownloadedMono >= expectedDataBytes || 
             (expectedDataBytes - httpBytesDownloadedMono) < 4096){
            downloadComplete = true;
            Serial.printf("[PLAY][DOWNLOAD] Parallel download complete: %.1fkB total\n",
              httpBytesDownloadedMono / 1024.0f);
          }
        } else {
          delay(500);
          if(!stream->connected() && stream->available() == 0){
            downloadComplete = true;
          }
        }
      }
    } else if(isSmallFile){
      downloadComplete = true; // Small file already fully downloaded
    }
    
    // ===== PLAYBACK TASK: Read from ring buffer and feed I2S =====
    // For large files: Ensure minimum buffer before playing to prevent breaking
    if(!isSmallFile && ringBufferFill < (INITIAL_BUFFER_SIZE / 4) && !downloadComplete){
      // Buffer too low during parallel download - wait for more data
      delay(5);
      continue;
    }
    
    if(ringBufferFill == 0){
      // No data available - check if we're done
      if(downloadComplete){
        playbackComplete = true;
        Serial.printf("[PLAY][STREAM] Playback complete - all data played from ring buffer\n");
        break;
      } else {
        // Still downloading, wait a bit (but not too long to avoid breaking)
        delay(5);
        continue;
      }
    }
    
    // Read available data from ring buffer (up to READ_BUF)
    // Ensure we don't read too much at once to maintain smooth flow
    size_t toPlay = (ringBufferFill < READ_BUF) ? ringBufferFill : READ_BUF;
    // For large files, read smaller chunks to maintain buffer level
    if(!isSmallFile && ringBufferFill < (INITIAL_BUFFER_SIZE / 2)){
      toPlay = (toPlay < (READ_BUF / 2)) ? toPlay : (READ_BUF / 2);
    }
    size_t readFromRing = ringBuf.read(playbackBuf, toPlay);
      
      if(readFromRing > 0){
        bool okWrite = true;
        size_t playedBytes = 0;
        
        if(channels == 1){
          // Mono-to-stereo conversion
          size_t samples = readFromRing / 2;
          if(samples > 0){
            size_t outBytes = samples * 4;
            if(!stereoScratch.ensure(outBytes)){
              Serial.println("[PLAY] stereo buffer alloc failed");
              okWrite = false;
            } else {
              uint8_t *dst = stereoScratch.buf;
              for(size_t i=0;i<samples;i++){
                uint8_t lo = playbackBuf[i*2+0];
                uint8_t hi = playbackBuf[i*2+1];
                size_t o = i*4;
                dst[o+0]=lo; dst[o+1]=hi;
                dst[o+2]=lo; dst[o+3]=hi;
              }
              size_t written = 0;
              // Use blocking write with longer timeout (500ms) to ensure data is written
              // This prevents data loss and ensures smooth playback without breaking
              TickType_t timeout = pdMS_TO_TICKS(500);
              esp_err_t err = i2s_write(I2S_NUM_1, dst, outBytes, &written, timeout);
              if(err != ESP_OK){
                Serial.printf("[PLAY] i2s_write failed: %d\n", err);
                okWrite = false;
              } else {
                playedBytes = written;
                // Start rainbow LED only when first I2S write succeeds (actual playback starts)
                if(!rainbowStarted && written > 0){
                  rainbowStarted = true;
                  ledState = LS_PLAYING;
                  Serial.println("[PLAY] Playback started - rainbow LED enabled");
                }
                // If not all data was written, log warning but continue
                if(written < outBytes){
                  Serial.printf("[PLAY][WARN] Partial write: %u/%u bytes (DMA buffer may be full)\n",
                    (unsigned)written, (unsigned)outBytes);
                }
              }
            }
          }
        } else {
          // Stereo data - write directly
          size_t written = 0;
          TickType_t timeout = pdMS_TO_TICKS(500);
          esp_err_t err = i2s_write(I2S_NUM_1, playbackBuf, readFromRing, &written, timeout);
          if(err != ESP_OK){
            Serial.printf("[PLAY] i2s_write failed: %d\n", err);
            okWrite = false;
          } else {
            playedBytes = written;
            // Start rainbow LED only when first I2S write succeeds (actual playback starts)
            if(!rainbowStarted && written > 0){
              rainbowStarted = true;
              ledState = LS_PLAYING;
              Serial.println("[PLAY] Playback started - rainbow LED enabled");
            }
            // If not all data was written, log warning
            if(written < readFromRing){
              Serial.printf("[PLAY][WARN] Partial write: %u/%u bytes\n",
                (unsigned)written, (unsigned)readFromRing);
            }
          }
        }
        
        if(!okWrite){
          Serial.println("[PLAY] write failed, aborting playback");
          break;
        }
        
        if(playedBytes > 0){
          // For stats: convert stereo bytes to mono-equivalent for fair comparison
          size_t monoEquivalentBytes = (channels == 1) ? (playedBytes / 2) : playedBytes;
          statsTotalI2S += monoEquivalentBytes;  // Track mono-equivalent bytes
          statsWindowI2S += monoEquivalentBytes;
          i2sChunkCount += ((monoEquivalentBytes + CHUNK_SIZE_LOG - 1) / CHUNK_SIZE_LOG);
        }
      }
    
    // Check if playback is complete
    if(ringBuf.available() == 0 && downloadComplete){
      playbackComplete = true;
      Serial.printf("[PLAY][STREAM] Playback complete - all data played from ring buffer\n");
      break;
    }
    
    // Log stats periodically
    logPlaybackStats("playback", stream->available(), false, ringBufferFill);
    
    // Update rainbow LED only if playback has started
    if(rainbowStarted){
      ledRainbowStepFast();
    }
    
    // Allow MQTT to process during playback (but less frequently to avoid breaking)
    static unsigned long lastMQTTLoop = 0;
    if(mqttClient.connected() && (now - lastMQTTLoop > 100)){
      mqttClient.loop();
      lastMQTTLoop = now;
    }
    
    // Small yield to allow other tasks (but minimal to prevent breaking)
    // No delay - let FreeRTOS handle scheduling naturally
  }
  
  // Stop rainbow LED immediately when playback ends
  if(rainbowStarted){
    ledState = LS_RANDOM_RGB;
    lastActivityMs = millis();
    lastRandomRgbChange = millis();
    Serial.println("[PLAY] Playback ended - rainbow LED stopped");
  }
  
  // ===== PHASE 3: CLEANUP =====
  // Clean ring buffer after playback completes
  Serial.printf("[PLAY][CLEANUP] Cleaning ring buffer (was %.1fkB)\n", 
    ringBuf.available() / 1024.0f);
  
  ringBuf.clear();
  ringBuf.deinit();
  free(httpReadBuf);
  free(playbackBuf);
  free(hdr); 
  http.end();
  uninstallI2STX();
  logPlaybackStats("final");
  unsigned long totalMs = millis() - playbackStartMs;
  float totalSec = totalMs / 1000.0f;
  float avgDownloadKBps = totalSec > 0 ? (statsTotalHttp / 1024.0f) / totalSec : 0.0f;
  float avgPlaybackKBps = totalSec > 0 ? (statsTotalI2S / 1024.0f) / totalSec : 0.0f;
  uint32_t totalHttpChunks = (statsTotalHttp + CHUNK_SIZE_LOG - 1) / CHUNK_SIZE_LOG;
  uint32_t totalI2SChunks = (statsTotalI2S + CHUNK_SIZE_LOG - 1) / CHUNK_SIZE_LOG;
  float avgHttpChunksPerSec = totalSec > 0 ? totalHttpChunks / totalSec : 0.0f;
  float avgI2SChunksPerSec = totalSec > 0 ? totalI2SChunks / totalSec : 0.0f;
  
  Serial.printf("[PLAY][DONE] downloaded=%.1fkB played=%.1fkB duration=%.2fs avgDownloadSpeed=%.1fkB/s avgPlaybackSpeed=%.1fkB/s\n",
    statsTotalHttp / 1024.0f,
    statsTotalI2S / 1024.0f,
    totalSec,
    avgDownloadKBps,
    avgPlaybackKBps);
  Serial.printf("[PLAY][CHUNKS] httpChunks=%u i2sChunks=%u httpChunks/s=%.1f i2sChunks/s=%.1f chunkSize=%u\n",
    totalHttpChunks,
    totalI2SChunks,
    avgHttpChunksPerSec,
    avgI2SChunksPerSec,
    (unsigned)CHUNK_SIZE_LOG);
}

// -------- MQTT message handling --------
void handlePlayControlJson(const JsonDocument &doc){
  if(!doc.containsKey("url")){ Serial.println("[MQTT] play/control missing url"); return; }
  
  // Prevent auto-replay: Check if busy, same URL, or too soon after last playback
  if(playbackActive || isRecording){ 
    Serial.println("[PLAY] busy, ignoring"); 
    return; 
  }
  
  String url = doc["url"].as<String>();
  
  // Prevent duplicate playback of same URL within cooldown period
  if(url == lastPlayedUrl && (millis() - lastPlaybackEndTime) < PLAYBACK_COOLDOWN_MS){
    Serial.printf("[PLAY] ignoring duplicate URL (cooldown: %lums)\n", 
      (unsigned long)(millis() - lastPlaybackEndTime));
    return;
  }
  
  uint32_t sr = doc.containsKey("sampleRate") ? (uint32_t)doc["sampleRate"].as<uint32_t>() : 0;
  playbackActive = true; // Button will be disabled automatically
  lastPlayedUrl = url; // Store URL to prevent duplicate playback
  if(recorderInstalled){ uninstallI2SRecorder(); delay(40); }
  // DO NOT set ledState here - rainbow will start only when actual playback starts (first I2S write)
  playUrlWavBlocking(url, sr);
  // After streaming: Random RGB, then idle animation after 15s (already set in playUrlWavBlocking)
  lastPlaybackEndTime = millis(); // Track when playback ended
  setupI2SRecorder();
  playbackActive = false; // Button enabled again
}
void mqttCallback(char* topic, byte* payload, unsigned int length){
  String top = String(topic);
  if(top.endsWith("/audio/play/control")){
    DynamicJsonDocument doc(1024);
    DeserializationError err = deserializeJson(doc, payload, length);
    if(err){ Serial.println("[MQTT] JSON parse failed"); return; }
    handlePlayControlJson(doc);
  }
}

// -------- Factory reset --------
void doFactoryReset(){
  Serial.println("[RESET] factory reset");
  ledState = LS_RESET; ledFill(255,0,0); delay(300);
  wm.resetSettings();
  WiFi.disconnect(true, true);
  if(LittleFS.format()) Serial.println("[FS] formatted");
  else Serial.println("[FS] format FAILED");
  delay(200);
  ESP.restart();
}

// -------- Button handling --------
void handleButtonTick(){
  // Disable button during streaming and recording
  if(playbackActive || isRecording){
    // Reset button state if pressed during busy state
    if(btnPhase != BP_IDLE){
      btnPhase = BP_IDLE;
      pressTs = 0;
      releaseTs = 0;
    }
    return; // Button disabled
  }
  
  bool raw = (digitalRead(BUTTON_PIN) == LOW);
  unsigned long now = millis();
  if(btnPhase == BP_IDLE){
    if(raw) { btnPhase = BP_DEBOUNCE; pressTs = now; }
  } else if(btnPhase == BP_DEBOUNCE){
    if(now - pressTs < DEBOUNCE_MS){ if(!raw) btnPhase = BP_IDLE; }
    else { if(raw){ btnPhase = BP_PRESSED; pressTs = now; } else btnPhase = BP_IDLE; }
  } else if(btnPhase == BP_PRESSED){
    if(!raw){
      releaseTs = now;
      unsigned long held = releaseTs - pressTs;
      if(held >= LONG_PRESS_MS){
        Serial.printf("[BTN] long press %u ms -> reset\n", (unsigned)held);
        doFactoryReset();
      } else {
        if(held <= SHORT_PRESS_MAX_MS){
          // Short press: Start recording
          isRecording = true;
          sessionCounter++; persistSessionCounter();
          char sid[32]; snprintf(sid, sizeof(sid), "%lu", sessionCounter);
          Serial.printf("[BTN] short press -> record sid=%s\n", sid);
          ledState = LS_RECORDING; ledFill(0,0,255);
          lastActivityMs = now;
          recordTimedToFile(sid, RECORD_TIMEOUT_MS);
          // After recording: Stream to MQTT (LED stays blue during recording, then streaming)
          ledState = LS_STREAMING;
          streamSessionFileToMQTT(sid);
          // After recording and streaming: Random RGB, then idle animation after 15s
          ledState = LS_RANDOM_RGB;
          lastActivityMs = millis();
          lastRandomRgbChange = millis();
          isRecording = false;
        } else {
          // Medium press: treat as short
          isRecording = true;
          sessionCounter++; persistSessionCounter();
          char sid[32]; snprintf(sid, sizeof(sid), "%lu", sessionCounter);
          Serial.printf("[BTN] medium press -> record sid=%s\n", sid);
          ledState = LS_RECORDING; ledFill(0,0,255);
          lastActivityMs = now;
          recordTimedToFile(sid, RECORD_TIMEOUT_MS);
          ledState = LS_STREAMING;
          streamSessionFileToMQTT(sid);
          ledState = LS_RANDOM_RGB;
          lastActivityMs = millis();
          isRecording = false;
        }
      }
      btnPhase = BP_IDLE;
    } else {
      if(millis() - pressTs > 3000 && ledState != LS_RECORDING) ledFill(128,0,0);
    }
  }
}

// -------- Setup / Loop --------
void setup(){
  Serial.begin(115200);
  delay(50);
  pinMode(BUTTON_PIN, INPUT_PULLUP);
  randomSeed(esp_random());

  ledInit(); ledState = LS_BOOT; ledFill(0,0,0);
  if(!ensureLittleFS()) while(1){ delay(1000); }

  // WiFi: open portal with SSID only (no password)
  wm.setTimeout(120);
  ledState = LS_PROVISION; ledFill(0,0,255);
  if(!wm.autoConnect(AP_SSID)){
    Serial.println("[WiFi] autoConnect failed; restarting...");
    delay(2000); ESP.restart();
  }
  // smoother streaming: keep radio awake
  WiFi.setSleep(false);
  Serial.printf("[WiFi] connected: %s\n", WiFi.localIP().toString().c_str());
  ledState = LS_CONNECTED; ledFill(0,255,0);
  lastActivityMs = millis();

  // TLS time (NTP)
  configTime(19800, 0, "pool.ntp.org", "time.google.com");
  delay(600);

  // MQTT
  mqttClient.setCallback(mqttCallback);
  connectMQTT();

  // Filters
  g_voice.init((float)SAMPLE_RATE);
  g_gate.reset();

  // Start recorder by default
  setupI2SRecorder();

  // Topics
  TOP_PLAY_CTRL = String(DEVICE_ID) + "/audio/play/control";

  Serial.println("[READY] Short press -> record 8s (upload). Long press >=10s -> factory reset.");
}

unsigned long lastIdleAnim = 0;
void loop(){
  // MQTT keepalive
  if(WiFi.isConnected()){
    if(!mqttClient.connected()){
      if(millis() - lastMQTTAttempt > MQTT_RECONNECT_MS){
        lastMQTTAttempt = millis();
        connectMQTT();
      }
    } else mqttClient.loop();
  }

  // Button handling (locked-out while busy handled inside FSM conditions)
  handleButtonTick();

  // LED policy
  unsigned long now = millis();
  
  // Priority 1: WiFi disconnected - Blue (unless recording/reset)
  if(!WiFi.isConnected()){
    if(ledState != LS_RECORDING && ledState != LS_RESET && !playbackActive) { 
      ledState = LS_DISCONNECTED; 
      ledFill(0,0,255); 
    }
  } 
  // Priority 2: WiFi connected - Green (if no other active state)
  else if(ledState == LS_DISCONNECTED && !playbackActive && !isRecording && ledState != LS_RESET){
    ledState = LS_CONNECTED;
    ledFill(0,255,0);
    lastActivityMs = now;
  }
  
  // Priority 3: Recording - Blue (8 seconds)
  // This is handled in recordTimedToFile() - no change needed
  
  // Priority 4: Streaming/Playback - Fast rainbow animation (STOPS when streaming stops)
  // This is handled in playUrlWavBlocking() - ledRainbowStepFast() called during playback
  // After playback, state changes to LS_RANDOM_RGB
  
  // Priority 5: After streaming/recording - Random RGB, then idle animation after 15s
  if(ledState == LS_RANDOM_RGB){
    // Update random RGB every 500ms while in this state
    if(now - lastRandomRgbChange > 500){
      ledRandomRgb();
      lastRandomRgbChange = now;
    }
    // After 15s idle, switch to animation theme
    if(now - lastActivityMs > IDLE_ANIM_AFTER_MS){
      ledState = LS_IDLE_ANIM;
      lastIdleAnim = now; // Initialize animation timer
    }
  }
  
  // Priority 6: After 15s idle (when connected and not busy) - Start animation theme
  if(ledState == LS_CONNECTED && (now - lastActivityMs > IDLE_ANIM_AFTER_MS) && 
     !playbackActive && !isRecording && WiFi.isConnected()){
    ledState = LS_IDLE_ANIM;
    lastIdleAnim = now; // Initialize animation timer
  }
  
  // LED state machine - render current state
  switch(ledState){
    case LS_CONNECTED: 
      ledFill(0,255,0); // Green - WiFi connected
      break;
    case LS_IDLE_ANIM:
      // Slow RGB animation theme (ambient)
      if(now - lastIdleAnim > IDLE_ANIM_STEP_MS){
        lastIdleAnim = now;
        ledIdleAmbientStep();
      }
      break;
    case LS_RECORDING:
      ledFill(0,0,255); // Blue - Recording (8 seconds)
      break;
    case LS_STREAMING:
    case LS_PLAYING:
      // Fast rainbow animation during streaming (handled in playUrlWavBlocking loop)
      // This should only be active during actual playback
      if(playbackActive){
        ledRainbowStepFast();
      } else {
        // If playback stopped but state didn't update, switch to random RGB
        ledState = LS_RANDOM_RGB;
        lastActivityMs = now;
        lastRandomRgbChange = now;
      }
      break;
    case LS_RANDOM_RGB:
      // Random RGB (handled above in priority 5)
      break;
    case LS_DISCONNECTED:
      ledFill(0,0,255); // Blue - WiFi disconnected
      break;
    case LS_RESET:
      ledFill(255,0,0); // Red - Factory reset
      break;
    default: 
      break;
  }

  // Activity tick
  if(mqttClient.connected()) lastActivityMs = now;
  delay(4);
}
