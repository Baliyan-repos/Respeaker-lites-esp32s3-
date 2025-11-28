  // esp32_respeaker_prod_final.ino
  // Production firmware - Complete redesign with proper state management

  #include <Arduino.h>
  #include <WiFi.h>
  #include <WiFiClientSecure.h>
  #include <WiFiManager.h>
  #include <PubSubClient.h>
  #include <ArduinoJson.h>
  #include <LittleFS.h>
  #include <Wire.h>
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
  WiFiManager wm;
  static const char* AP_SSID = "Rannkly Nova Speaker"; // AP hotspot name - no password

  // -------- AUDIO CAPTURE --------
  static const uint32_t SAMPLE_RATE = 16000u;
  static const int EXTRACTION_SHIFT = 16;
  static const size_t I2S_READ_BYTES = 8192;
  static const unsigned long RECORD_TIMEOUT_MS = 8000UL; // 8 seconds
  static const unsigned long POSTROLL_MS = 200UL;

  // Playback robustness - Adaptive streaming with 712KB ring buffer
  static const size_t PLAY_READ_BUF = 8 * 1024; // Increased from 4KB to 8KB for faster download
  static const size_t RING_BUFFER_SIZE = 712 * 1024; // 712KB ring buffer
  static const size_t INITIAL_BUFFER_TARGET = 512 * 1024;
  static const size_t LOW_BUFFER_THRESHOLD = 32 * 1024;
  static const size_t RESUME_BUFFER_THRESHOLD = 128 * 1024;
  static const unsigned long PLAY_STATS_INTERVAL_MS = 1000UL;

  // Audio filters - HPF and Notch for recording (from provided firmware)
  struct HPFState {
    float a = 0.999f;
    float prevX = 0.0f;
    float prevY = 0.0f;
    void init(float cutoffHz, float fs) {
      if (cutoffHz <= 0.0f) { a = 1.0f; return; }
      float rc = 1.0f / (2.0f * 3.14159265358979323846f * cutoffHz);
      float dt = 1.0f / fs;
      a = rc / (rc + dt);
      prevX = prevY = 0.0f;
    }
    inline float process(float x) {
      float y = a * (prevY + x - prevX);
      prevX = x; prevY = y;
      return y;
    }
  } g_hpf;

  struct Notch {
    bool enabled = false;
    float b0,b1,b2,a1,a2;
    float x1=0,x2=0,y1=0,y2=0;
    void init50Hz(float fs){
      float f0 = 50.0f;
      float Q = 8.0f;
      float w0 = 2*M_PI*f0/fs;
      float alpha = sin(w0)/(2*Q);
      float cosw0 = cos(w0);
      float b0t = 1;
      float b1t = -2*cosw0;
      float b2t = 1;
      float a0t = 1 + alpha;
      float a1t = -2*cosw0;
      float a2t = 1 - alpha;
      b0 = b0t/a0t; b1 = b1t/a0t; b2 = b2t/a0t;
      a1 = a1t/a0t; a2 = a2t/a0t;
      enabled = true;
      x1=x2=y1=y2=0;
    }
    inline float process(float x){
      if(!enabled) return x;
      float y = b0*x + b1*x1 + b2*x2 - a1*y1 - a2*y2;
      x2 = x1; x1 = x;
      y2 = y1; y1 = y;
      return y;
    }
  } g_notch;

  // Audio processing constants for streaming
  #define NORMALIZE_TARGET_PEAK 20000.0f
  #define MAX_GAIN              6.0f
  #define NOISE_GATE_RMS        50.0f

  // -------- CHUNK UPLOAD --------
  static const size_t CHUNK_PUBLISH_SIZE = 16384; // Increased from 8192 to 16384 for faster streaming
  static const unsigned long CHUNK_PUBLISH_DELAY_MS = 5UL; // Reduced from 20ms to 5ms for faster streaming
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
  volatile bool isStreaming = false;
  static bool i2sTxInstalled = false;
  unsigned long sessionCounter = 0;
  unsigned long lastMQTTAttempt = 0;
  unsigned long lastActivityMs = 0;
  String lastPlayedUrl = "";
  unsigned long lastPlaybackEndTime = 0;
  const unsigned long PLAYBACK_COOLDOWN_MS = 2000;

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

  String TOP_PLAY_CTRL;

  // -------- LED helpers --------
  void ledInit(){ pixels.begin(); pixels.setBrightness(200); pixels.show(); } // Increased from 120 to 200
  void ledFill(uint8_t r, uint8_t g, uint8_t b){
    for(int i=0;i<LED_COUNT;i++) pixels.setPixelColor(i, pixels.Color(r,g,b));
    pixels.show();
  }
  void ledRainbowStepFast(){
    static unsigned long lastRainbowStep = 0;
    unsigned long now = millis();
    const unsigned long PLAY_RAINBOW_MS = 2; // Very fast rainbow (2ms) for streaming - faster than before
    if(now - lastRainbowStep < PLAY_RAINBOW_MS) return;
    lastRainbowStep = now;
    rainbowPos += 6; // Increment by 6 for faster and more colorful transitions
    for(int i=0;i<LED_COUNT;i++){
      // Enhanced rainbow with more colors - full 360 degree hue range
      uint16_t hue = ((i * 360 / max(1,LED_COUNT) + rainbowPos) % 360);
      // Use full saturation and value for vibrant colors
      uint32_t color = pixels.ColorHSV(hue * 65536 / 360, 255, 255);
      pixels.setPixelColor(i, color);
    }
    pixels.setBrightness(220); // Increased from 150 to 220
    pixels.show();
  }
  void ledIdleAmbientStep(){
    static unsigned long lastIdleAnimUpdate = 0;
    unsigned long now = millis();
    const unsigned long IDLE_ANIM_STEP_MS = 80; // Slow ambient (80ms)
    if(now - lastIdleAnimUpdate < IDLE_ANIM_STEP_MS) return;
    lastIdleAnimUpdate = now;
    static uint16_t hueBase = 0;
    hueBase++;
    for(int i=0;i<LED_COUNT;i++){
      uint16_t hue = (hueBase + i*12) % 360;
      uint32_t col = pixels.ColorHSV(hue * 182);
      pixels.setPixelColor(i, col);
    }
    pixels.setBrightness(180); // Increased from 90 to 180
    pixels.show();
  }
  void ledRandomRgb(){
    static uint8_t r = 0, g = 0, b = 0;
    static unsigned long lastChange = 0;
    unsigned long now = millis();
    if(now - lastChange > 500){
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
    i2s_stop(I2S_NUM_0);
    esp_err_t uninstallResult = i2s_driver_uninstall(I2S_NUM_0);
    if(uninstallResult == ESP_OK || uninstallResult == ESP_ERR_INVALID_STATE){
      recorderInstalled = false;
      delay(10);
    }
    pinMode(I2S_BCK, INPUT); pinMode(I2S_WS, INPUT); pinMode(I2S_DATA_IN, INPUT);
  }

  // -------- I2C codec init (minimal) --------
  #define AIC3204_ADDR 0x18
  bool i2cWrite8(uint8_t reg, uint8_t val){
    Wire.beginTransmission(AIC3204_ADDR);
    Wire.write(reg & 0xFF);
    Wire.write(val & 0xFF);
    uint8_t error = Wire.endTransmission();
    delay(2); // Increased delay for stability
    if(error != 0){
      // I2C error - but continue anyway (codec might work without init)
      return false;
    }
    return true;
  }
  void initCodec(){
    Serial.println("[CODEC] init start");
    // Try to detect if codec is present
    Wire.beginTransmission(AIC3204_ADDR);
    uint8_t error = Wire.endTransmission();
    if(error != 0){
      Serial.printf("[CODEC] not detected (I2C error %d) - continuing without codec init\n", error);
      return;
    }
    
    // Codec detected - try to initialize (errors are non-critical)
    i2cWrite8(0x00, 0x00); // page 0
    i2cWrite8(0x01, 0x01); delay(10);
    i2cWrite8(0x01, 0x00); delay(10);
    i2cWrite8(0x0E, 0x01);
    i2cWrite8(0x0F, 0x00); delay(5);
    i2cWrite8(0x3A, 0x3A);
    i2cWrite8(0x3B, 0x3A);
    i2cWrite8(0x09, 0x10);
    i2cWrite8(0x0A, 0x00);
    Serial.println("[CODEC] init done (basic). If capture low, raise PGA regs 0x3A/0x3B.");
    Serial.println("[CODEC] Note: I2C timeout errors are non-critical if recording works.");
  }

  // -------- I2S playback (TX) --------
  bool installI2STX(uint32_t sampleRate){
    // Safe uninstall with proper checks
    if(i2sTxInstalled){
      i2s_stop(I2S_NUM_1);
      esp_err_t uninstallResult = i2s_driver_uninstall(I2S_NUM_1);
      if(uninstallResult == ESP_OK || uninstallResult == ESP_ERR_INVALID_STATE){
        delay(50);
      }
      i2sTxInstalled = false;
    }
    
    i2s_config_t cfg = {
      .mode = (i2s_mode_t)(I2S_MODE_MASTER | I2S_MODE_TX),
      .sample_rate = (int)sampleRate,
      .bits_per_sample = I2S_BITS_PER_SAMPLE_16BIT,
      .channel_format = I2S_CHANNEL_FMT_RIGHT_LEFT,
      .communication_format = I2S_COMM_FORMAT_STAND_I2S,
      .intr_alloc_flags = ESP_INTR_FLAG_LEVEL1,
      .dma_buf_count = 16,
      .dma_buf_len = 512,
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
    delay(10);
    i2sTxInstalled = true;
    Serial.printf("[I2S] TX ready @%u Hz\n", (unsigned)sampleRate);
    return true;
  }
  void uninstallI2STX(){
    if(i2sTxInstalled){
      i2s_stop(I2S_NUM_1);
      esp_err_t uninstallResult = i2s_driver_uninstall(I2S_NUM_1);
      if(uninstallResult == ESP_OK || uninstallResult == ESP_ERR_INVALID_STATE){
        delay(20);
      }
      i2sTxInstalled = false;
    }
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

  // -------- Recording helpers --------
  String makeSessionPath(const char* sid){ String fn="/session_"; fn+=sid; fn+=".pcm"; return fn; }
  static inline int16_t downmix32stereo_to_s16_processed(int32_t left32, int32_t right32){
    int64_t avg = ((int64_t)left32 + (int64_t)right32) / 2;
    int32_t s32 = (int32_t)(avg >> EXTRACTION_SHIFT);
    float s = (float)s32;
    float h = g_hpf.process(s);
    float n = g_notch.process(h);
    if(n > 32767.0f) n = 32767.0f;
    if(n < -32768.0f) n = -32768.0f;
    return (int16_t)lroundf(n);
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
    // Reduced retry attempts and delays for faster streaming
    for(int attempt=0; attempt<4; attempt++){ // Reduced from 6 to 4 attempts
      if(!mqttClient.connected()){
        connectMQTT();
        unsigned long t0 = millis();
        while(!mqttClient.connected() && millis() - t0 < 1000){ // Reduced from 1500ms to 1000ms
          mqttClient.loop(); 
          delay(10); // Reduced from 20ms to 10ms
        }
      }
      if(mqttClient.connected()){
        ok = mqttClient.publish(top.c_str(), buf, (unsigned int)total, false);
        if(ok) break;
      }
      delay(50); // Reduced from 150ms to 50ms for faster retries
    }
    free(buf);
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
    File f = LittleFS.open(fn, FILE_WRITE);
    if(!f){ Serial.printf("[FS] cannot create %s\n", fn.c_str()); return; }
    f.close();

    uint8_t *i2sBuf = (uint8_t*)malloc(I2S_READ_BYTES);
    if(!i2sBuf){ Serial.println("[ALLOC] i2sBuf failed"); return; }
    size_t bytesRead = 0;
    unsigned long tStart = millis();
    unsigned long tStop = tStart + timeoutMs;
    unsigned long lastAudioTime = tStart;

    Serial.printf("\n🎙️ Timed Recording session %s -> %s (timeout %ums)\n", sid, fn.c_str(), (unsigned)timeoutMs);
    
    isRecording = true;
    ledState = LS_RECORDING;
    ledFill(0,0,255); // Blue light during recording (8 seconds)
    lastActivityMs = millis();

    while(millis() < tStop){
      esp_err_t r = i2s_read(I2S_NUM_0, i2sBuf, I2S_READ_BYTES, &bytesRead, 200 / portTICK_PERIOD_MS);
      if(r == ESP_OK && bytesRead > 0){
        size_t stereoFrames = bytesRead / 8;
        if(stereoFrames == 0) continue;
        size_t outBytes = stereoFrames * 2;
        uint8_t *outBuf = (uint8_t*)malloc(outBytes);
        if(!outBuf){ Serial.println("[ALLOC] outBuf failed"); break; }
        int32_t *s32 = (int32_t*)i2sBuf;
        int16_t *o16 = (int16_t*)outBuf;
        for(size_t i=0;i<stereoFrames;i++){
          int32_t l32 = s32[2*i + 1];
          int32_t r32 = s32[2*i + 0];
          o16[i] = downmix32stereo_to_s16_processed(l32, r32);
        }
        File fw = LittleFS.open(fn, FILE_APPEND);
        if(!fw) { Serial.println("[FS] open append failed"); free(outBuf); break; }
        size_t w = fw.write((uint8_t*)outBuf, outBytes);
        fw.close();
        free(outBuf);
        if(w != outBytes) { Serial.println("[FS] append failed - abort recording"); break; }
        lastAudioTime = millis();
      } else if(r != ESP_OK){
        Serial.printf("[I2S] read err %d\n", (int)r);
        delay(5);
      }
      if(mqttClient.connected()) mqttClient.loop();
    }
    free(i2sBuf);
    isRecording = false;
    Serial.println("🎤 Timed Recording saved.");
    
    // postroll wait
    unsigned long prStart = millis();
    while(millis() - prStart < POSTROLL_MS){
      if(mqttClient.connected()) mqttClient.loop();
      delay(5);
    }
  }

  bool analyzeFilePeak(const char* path, int32_t &outPeak, float &outMeanAbs){
    File f = LittleFS.open(path, FILE_READ);
    if(!f) return false;
    size_t fileLen = f.size();
    if(fileLen == 0){ f.close(); return false; }

    const size_t SCAN_BUF = 4096;
    int16_t *scan_buf = (int16_t*)malloc(SCAN_BUF);
    if(!scan_buf){ f.close(); return false; }
    size_t bytesLeft = fileLen;
    int32_t filePeak = 1;
    uint64_t sumAbs = 0;
    uint64_t sampleCount = 0;
    f.seek(0);
    while(bytesLeft){
      size_t toRead = bytesLeft > SCAN_BUF ? SCAN_BUF : bytesLeft;
      size_t r = f.read((uint8_t*)scan_buf, toRead);
      if(r==0) break;
      size_t samples = r / 2;
      for(size_t i=0;i<samples;i++){
        int32_t v = abs((int32_t)scan_buf[i]);
        if(v > filePeak) filePeak = v;
        sumAbs += (uint32_t)abs((int32_t)scan_buf[i]);
      }
      sampleCount += samples;
      bytesLeft -= r;
    }
    free(scan_buf);
    f.close();
    outPeak = filePeak < 1 ? 1 : filePeak;
    outMeanAbs = sampleCount ? ((float)sumAbs / (float)sampleCount) : 0.0f;
    return true;
  }

  void streamSessionFileToMQTT(const char* sid){
    String fn = makeSessionPath(sid);
    if(!LittleFS.exists(fn)){ Serial.printf("[FS] session missing: %s\n", fn.c_str()); isRecording = false; return; }
    
    char path[128];
    fn.toCharArray(path, sizeof(path));
    File f = LittleFS.open(path, FILE_READ);
    if(!f){ Serial.printf("[FS] cannot open %s\n", path); isRecording = false; return; }
    size_t fileLen = f.size();
    if(fileLen == 0){ f.close(); Serial.println("[FS] file empty"); isRecording = false; return; }

    uint32_t chunkBytes = (uint32_t)CHUNK_PUBLISH_SIZE;
    uint32_t totalChunks = (fileLen + chunkBytes - 1) / chunkBytes;
    Serial.printf("[FS] len=%u -> chunks=%u (chunk=%u)\n", (unsigned)fileLen, (unsigned)totalChunks, (unsigned)chunkBytes);

    int32_t filePeak = 1;
    float meanAbs = 0.0f;
    if(!analyzeFilePeak(path, filePeak, meanAbs)){ Serial.println("[ANALYZE] failed"); f.close(); isRecording = false; return; }
    Serial.printf("[ANALYZE] peak=%d meanAbs=%.1f\n", (int)filePeak, meanAbs);

    float desired = NORMALIZE_TARGET_PEAK;
    float gain = desired / (float)filePeak;
    if(gain > MAX_GAIN) gain = MAX_GAIN;
    if(gain < 1.0f) gain = 1.0f;
    Serial.printf("[ANALYZE] gain=%f\n", gain);

    unsigned long startTry = millis();
    while(!mqttClient.connected()){
      if(millis() - startTry > 10000){ Serial.println("[MQTT] cannot connect - abort"); f.close(); isRecording = false; return; }
      connectMQTT();
      delay(200);
    }
    publishControlStart(sid, totalChunks, chunkBytes);
    delay(10);

    uint8_t *readBuf = (uint8_t*)malloc(chunkBytes);
    if(!readBuf){ Serial.println("[ALLOC] readBuf failed"); f.close(); isRecording = false; return; }
    
    isStreaming = true; // Button disabled during streaming
    ledState = LS_RANDOM_RGB; // NO rainbow - just random RGB during streaming
    lastActivityMs = millis();
    lastRandomRgbChange = millis();
    ledRandomRgb(); // Show random RGB, not rainbow

    auto computeChunkRms = [](const int16_t* s, size_t n)->float{
      if(n==0) return 0.0f;
      double acc = 0.0;
      for(size_t i=0;i<n;i++){ double v = s[i]; acc += v*v; }
      return sqrt(acc / (double)n);
    };

    uint32_t seq = 0;
    while(f.available()){
      size_t toRead = f.read(readBuf, chunkBytes);
      if(toRead == 0) break;
      int16_t *s16 = (int16_t*)readBuf;
      size_t nSamples = toRead / 2;
      if(gain != 1.0f){
        for(size_t i=0;i<nSamples;i++){
          float v = (float)s16[i] * gain;
          if(v > 32767.0f) v = 32767.0f;
          if(v < -32768.0f) v = -32768.0f;
          s16[i] = (int16_t)lrintf(v);
        }
      }
      float rms = computeChunkRms(s16, nSamples);
      if(rms < NOISE_GATE_RMS){
        memset(readBuf, 0, toRead);
      }
      publishChunkWithRetries(sid, seq, readBuf, toRead);
      seq++;
      
      unsigned long t0 = millis();
      while(millis() - t0 < CHUNK_PUBLISH_DELAY_MS){
        mqttClient.loop();
        delay(1);
      }
      lastActivityMs = millis();
    }
    free(readBuf);
    f.close();
    publishControlFinal(sid, seq);
    Serial.printf("[STREAM] session %s sent (%u chunks) gain=%f peak=%d\n", sid, (unsigned)seq, gain, filePeak);
    
    // Flush MQTT messages
    unsigned long flushStart = millis();
    while(millis() - flushStart < 600){
      mqttClient.loop();
      delay(10);
    }
    
    if(LittleFS.exists(path)){ 
      LittleFS.remove(path); 
      Serial.printf("[FS] removed %s\n", path);
    }
    
    // Button enabled again - NO rainbow, just random RGB
    isStreaming = false;
    isRecording = false;
    ledState = LS_RANDOM_RGB;
    lastActivityMs = millis();
    lastRandomRgbChange = millis();
    ledRandomRgb();
  }

  // -------- Ring Buffer for parallel download/playback --------
  struct RingBuffer {
    uint8_t *data;
    size_t size;
    volatile size_t writePos;
    volatile size_t readPos;
    volatile size_t availableBytes;
    volatile bool initialized;
    
    RingBuffer() : data(nullptr), size(0), writePos(0), readPos(0), availableBytes(0), initialized(false) {}
    
    bool init(size_t sz) {
      if(data) deinit(); // Clean up if already initialized
      data = (uint8_t*)malloc(sz);
      if(!data) {
        initialized = false;
        return false;
      }
      size = sz;
      writePos = 0;
      readPos = 0;
      availableBytes = 0;
      initialized = true;
      return true;
    }
    
    void deinit() {
      initialized = false;
      if(data) { 
        free(data); 
        data = nullptr; 
      }
      size = 0;
      writePos = 0;
      readPos = 0;
      availableBytes = 0;
    }
    
    size_t available() { 
      if(!initialized || !data || size == 0) return 0;
      size_t avail = availableBytes;
      if(avail > size) return 0; // Corruption detected
      return avail; 
    }
    size_t space() { 
      if(!initialized || !data || size == 0) return 0;
      size_t avail = availableBytes;
      if(avail > size) return 0; // Corruption detected
      return size - avail; 
    }
    
    size_t write(const uint8_t *src, size_t len) {
      if(!initialized || len == 0 || !data || !src || size == 0) return 0;
      
      // Calculate available space safely with atomic-like protection
      size_t currentAvailable = availableBytes;
      if(currentAvailable > size) {
        // Corruption detected - reset safely
        writePos = 0;
        readPos = 0;
        availableBytes = 0;
        currentAvailable = 0;
      }
      size_t availSpace = size - currentAvailable;
      if(availSpace == 0) return 0;
      
      size_t toWrite = (len < availSpace) ? len : availSpace;
      if(toWrite == 0 || toWrite > size) return 0; // Additional safety check
      
      size_t wp = writePos;
      if(wp >= size) wp = 0; // Safety clamp
      
      // Write in two parts if wrap-around needed
      size_t firstPart = (toWrite <= (size - wp)) ? toWrite : (size - wp);
      if(firstPart > 0 && (wp + firstPart) <= size) {
        memcpy(data + wp, src, firstPart);
      } else {
        firstPart = 0; // Safety: don't write if bounds check fails
      }
      
      // Second part if wrap-around
      if(toWrite > firstPart) {
        size_t secondPart = toWrite - firstPart;
        if(secondPart > 0 && secondPart <= size && secondPart <= wp) {
          memcpy(data, src + firstPart, secondPart);
        } else {
          toWrite = firstPart; // Adjust toWrite if second part fails
        }
      }
      
      if(toWrite == 0) return 0; // Safety: don't update if nothing written
      
      // Update write position atomically
      writePos = (wp + toWrite) % size;
      
      // Update available bytes with safety clamp
      size_t newAvailable = currentAvailable + toWrite;
      if(newAvailable > size) {
        // Corruption detected - reset
        writePos = 0;
        readPos = 0;
        availableBytes = 0;
        return 0;
      }
      availableBytes = newAvailable;
      
      return toWrite;
    }
    
    size_t read(uint8_t *dst, size_t len) {
      if(!initialized || len == 0 || !data || !dst || size == 0) return 0;
      
      size_t avail = availableBytes;
      if(avail > size) {
        // Corruption detected - reset safely
        writePos = 0;
        readPos = 0;
        availableBytes = 0;
        avail = 0;
      }
      if(avail == 0) return 0;
      
      size_t toRead = (len < avail) ? len : avail;
      if(toRead == 0 || toRead > size) return 0; // Additional safety check
      
      size_t rp = readPos;
      if(rp >= size) rp = 0; // Safety clamp
      
      // Read in two parts if wrap-around needed
      size_t firstPart = (toRead <= (size - rp)) ? toRead : (size - rp);
      if(firstPart > 0 && (rp + firstPart) <= size) {
        memcpy(dst, data + rp, firstPart);
      } else {
        firstPart = 0; // Safety: don't read if bounds check fails
      }
      
      // Second part if wrap-around
      if(toRead > firstPart) {
        size_t secondPart = toRead - firstPart;
        if(secondPart > 0 && secondPart <= size && secondPart <= rp) {
          memcpy(dst + firstPart, data, secondPart);
        } else {
          toRead = firstPart; // Adjust toRead if second part fails
        }
      }
      
      if(toRead == 0) return 0; // Safety: don't update if nothing read
      
      // Update read position atomically
      readPos = (rp + toRead) % size;
      
      // Update available bytes with safety clamp
      size_t newAvailable = avail - toRead;
      if(newAvailable > size || newAvailable > avail) {
        // Corruption detected - reset
        writePos = 0;
        readPos = 0;
        availableBytes = 0;
        return 0;
      }
      availableBytes = newAvailable;
      
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
    
    WiFiClientSecure secure;
    secure.setInsecure();
    HTTPClient http;
    http.setFollowRedirects(HTTPC_STRICT_FOLLOW_REDIRECTS);
    http.setTimeout(30000);
    http.setReuse(true);
    if(!http.begin(secure, url)){ return; }
    int code = http.GET();
    if(code != HTTP_CODE_OK && code != HTTP_CODE_PARTIAL_CONTENT){
      Serial.printf("[HTTP] GET %d\n", code);
      http.end(); return;
    }
    
    int contentLength = http.getSize();
    WiFiClient *stream = http.getStreamPtr();
    if(!stream){ http.end(); return; }

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
    if(dataAt<0){ free(hdr); http.end(); return; }
    
    size_t pcmStart = dataAt + 8;
    size_t expectedTotalBytes = 0;
    if(contentLength > 0){
      expectedTotalBytes = (size_t)contentLength;
    }
    
    size_t expectedDataBytes = 0;
    if(expectedTotalBytes > 0){
      size_t headerSize = pcmStart;
      expectedDataBytes = (expectedTotalBytes > headerSize) ? (expectedTotalBytes - headerSize) : expectedTotalBytes;
    }

    uint32_t sampleRate = SAMPLE_RATE; int channels = 1;
    if(hdrPos >= 24){
      int fmtPos = -1;
      for(size_t i=0;i+4<=hdrPos;i++){ 
        if(hdr[i]=='f'&&hdr[i+1]=='m'&&hdr[i+2]=='t'&&hdr[i+3]==' '){ 
          fmtPos=(int)i; break; 
        } 
      }
      if(fmtPos>=0 && fmtPos+16 <= (int)hdrPos){
        channels = hdr[fmtPos + 10] | (hdr[fmtPos + 11] << 8);
        sampleRate = (uint32_t)hdr[fmtPos + 12] | 
                    ((uint32_t)hdr[fmtPos+13]<<8) | 
                    ((uint32_t)hdr[fmtPos+14]<<16) | 
                    ((uint32_t)hdr[fmtPos+15]<<24);
        if(channels < 1 || channels > 2) channels = 1;
        if(sampleRate < 8000 || sampleRate > 48000) sampleRate = SAMPLE_RATE;
      }
    }
    if(sampleRateOverride) sampleRate = sampleRateOverride;

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
        // Only allocate once - don't realloc to avoid heap fragmentation
        if(buf) {
          // If already allocated but too small, we can't resize safely
          // Return false to indicate failure
          return false;
        }
        if(need == 0) return false; // Safety check
        buf = (uint8_t*)malloc(need);
        if(!buf) return false;
        cap = need;
        memset(buf, 0, need); // Initialize to zero
        return true;
      }
    } stereoScratch;

    size_t firstBody = hdrPos > pcmStart ? hdrPos - pcmStart : 0;

    RingBuffer ringBuf;
    if(!ringBuf.init(RING_BUFFER_SIZE)){
      uninstallI2STX(); free(hdr); http.end(); return;
    }
    
    const size_t READ_BUF = PLAY_READ_BUF;
    // Increased HTTP read buffer for faster download (8KB -> 64KB to support READ_BUF * 8 reads)
    uint8_t *httpReadBuf = (uint8_t*)malloc(READ_BUF * 8);
    uint8_t *playbackBuf = (uint8_t*)malloc(READ_BUF * 2);
    if(!httpReadBuf || !playbackBuf){
      ringBuf.deinit();
      if(httpReadBuf) free(httpReadBuf);
      if(playbackBuf) free(playbackBuf);
      uninstallI2STX(); free(hdr); http.end(); return;
    }
    
    const size_t MAX_STEREO_BUF = READ_BUF * 2;
    if(!stereoScratch.ensure(MAX_STEREO_BUF)){
      ringBuf.deinit();
      free(httpReadBuf);
      free(playbackBuf);
      uninstallI2STX(); free(hdr); http.end(); return;
    }

    size_t statsTotalHttp = 0;
    size_t statsTotalI2S = 0;
    size_t httpBytesDownloadedMono = 0;
    size_t i2sBytesPlayedMono = 0;
    unsigned long statsWindowStart = millis();
    
    if(firstBody > 0){
      size_t written = ringBuf.write(hdr + pcmStart, firstBody);
      statsTotalHttp += written;
      httpBytesDownloadedMono += written;
    }
    
    bool isSmallFile = (expectedDataBytes > 0 && expectedDataBytes < INITIAL_BUFFER_TARGET);
    bool downloadComplete = false;
    bool rainbowStarted = false;
    
    if(isSmallFile){
      size_t maxDownloadBytes = (expectedDataBytes > RING_BUFFER_SIZE) ? RING_BUFFER_SIZE : expectedDataBytes;
      while(stream->connected() && httpBytesDownloadedMono < maxDownloadBytes){
        int avail = stream->available();
        size_t ringSpace = ringBuf.space();
        
        if(avail > 0 && ringSpace > 0){
          // Increased from READ_BUF * 4 to READ_BUF * 8 for faster download
          int toRead = (avail < (int)READ_BUF * 8) ? avail : (int)READ_BUF * 8;
          if(toRead > (int)ringSpace) toRead = (int)ringSpace;
          size_t remainingToDownload = maxDownloadBytes - httpBytesDownloadedMono;
          if(toRead > (int)remainingToDownload) toRead = (int)remainingToDownload;
          
          int r = stream->readBytes(httpReadBuf, toRead);
          if(r > 0){
            size_t written = ringBuf.write(httpReadBuf, r);
            statsTotalHttp += written;
            httpBytesDownloadedMono += written;
          }
        } else {
          delay(1);
        }
        
        if(mqttClient.connected() && (millis() % 200 < 10)){
          mqttClient.loop();
        }
      }
      downloadComplete = true;
    } else {
      size_t maxDownloadBytes = (expectedDataBytes > RING_BUFFER_SIZE) ? RING_BUFFER_SIZE : expectedDataBytes;
      size_t initialTarget = (INITIAL_BUFFER_TARGET < maxDownloadBytes) ? INITIAL_BUFFER_TARGET : maxDownloadBytes;
      
      while(stream->connected() && ringBuf.available() < initialTarget && httpBytesDownloadedMono < maxDownloadBytes){
        int avail = stream->available();
        size_t ringSpace = ringBuf.space();
        
        if(avail > 0 && ringSpace > 0){
          // Increased read size for faster download - read as much as possible
          int toRead = (avail < (int)READ_BUF * 12) ? avail : (int)READ_BUF * 12; // Increased from 8 to 12
          if(toRead > (int)ringSpace) toRead = (int)ringSpace;
          size_t remainingToDownload = maxDownloadBytes - httpBytesDownloadedMono;
          if(toRead > (int)remainingToDownload) toRead = (int)remainingToDownload;
          
          int r = stream->readBytes(httpReadBuf, toRead);
          if(r > 0){
            size_t written = ringBuf.write(httpReadBuf, r);
            statsTotalHttp += written;
            httpBytesDownloadedMono += written;
          }
        } else {
          delay(0); // No delay if data available
        }
        
        // MQTT loop less frequently during download for speed
        if(mqttClient.connected() && (millis() % 300 < 5)){
          mqttClient.loop();
        }
      }
      
      if(httpBytesDownloadedMono >= maxDownloadBytes){
        downloadComplete = true;
      }
    }
    
    if(ringBuf.available() == 0){
      ringBuf.deinit();
      free(httpReadBuf);
      free(playbackBuf);
      free(hdr);
      http.end();
      uninstallI2STX();
      return;
    }
    
    bool playbackComplete = false;
    bool playbackPaused = false;
    unsigned long lastResumeTime = 0;
    const unsigned long RESUME_GRACE_PERIOD_MS = 1000UL;
    
    while(!playbackComplete){
      unsigned long now = millis();
      size_t ringBufferFill = ringBuf.available();
      
      // Download task
      if(stream->connected() && !downloadComplete){
        if(httpBytesDownloadedMono >= RING_BUFFER_SIZE){
          downloadComplete = true;
        } else {
          int avail = stream->available();
          size_t ringSpace = ringBuf.space();
          
          if(avail > 0 && ringSpace > 0){
            // Increased read size for faster download - read as much as possible
            int toRead = (avail < (int)READ_BUF * 16) ? avail : (int)READ_BUF * 16; // Increased from 8 to 16 for faster download
            if(toRead > (int)ringSpace) toRead = (int)ringSpace;
            size_t remainingToDownload = RING_BUFFER_SIZE - httpBytesDownloadedMono;
            if(toRead > (int)remainingToDownload) toRead = (int)remainingToDownload;
            
            if(ringBufferFill < LOW_BUFFER_THRESHOLD && avail > toRead){
              int maxRead = (int)READ_BUF * 20; // Increased from 12 to 20 for faster buffering when low
              if(maxRead > (int)ringSpace) maxRead = (int)ringSpace;
              if(maxRead > (int)remainingToDownload) maxRead = (int)remainingToDownload;
              if(maxRead > toRead && avail >= maxRead) toRead = maxRead;
            }
            
            int r = stream->readBytes(httpReadBuf, toRead);
            if(r > 0){
              size_t written = ringBuf.write(httpReadBuf, r);
              statsTotalHttp += written;
              httpBytesDownloadedMono += written;
              
              if(httpBytesDownloadedMono >= RING_BUFFER_SIZE){
                downloadComplete = true;
              }
            }
          } else if(avail <= 0 && !stream->connected()){
            if(expectedDataBytes > 0){
              if(httpBytesDownloadedMono >= expectedDataBytes || 
                (expectedDataBytes - httpBytesDownloadedMono) < 4096 ||
                httpBytesDownloadedMono >= RING_BUFFER_SIZE){
                downloadComplete = true;
              }
            } else {
              delay(500);
              if(!stream->connected() && stream->available() == 0){
                downloadComplete = true;
              }
            }
          }
        }
      }
      
      // Playback task
      unsigned long timeSinceResume = millis() - lastResumeTime;
      bool inGracePeriod = timeSinceResume < RESUME_GRACE_PERIOD_MS;
      
      if(ringBufferFill < LOW_BUFFER_THRESHOLD && !downloadComplete && !inGracePeriod){
        if(!playbackPaused){
          playbackPaused = true;
        }
        delay(20);
        continue;
      }
      
      if(playbackPaused && ringBufferFill >= RESUME_BUFFER_THRESHOLD){
        playbackPaused = false;
        lastResumeTime = millis();
      }
      
      if(playbackPaused){
        delay(10);
        continue;
      }
      
      if(ringBufferFill == 0){
        if(downloadComplete){
          playbackComplete = true;
          break;
        } else {
          delay(5);
          continue;
        }
      }
      
      size_t toPlay = READ_BUF;
      if(ringBufferFill >= RESUME_BUFFER_THRESHOLD){
        toPlay = READ_BUF;
      } else if(ringBufferFill >= LOW_BUFFER_THRESHOLD){
        toPlay = 2 * 1024;
      } else if(ringBufferFill >= (LOW_BUFFER_THRESHOLD / 2)){
        toPlay = 1 * 1024;
      } else {
        toPlay = 512;
      }
      
      if(ringBufferFill < toPlay) toPlay = ringBufferFill;
      
      if(inGracePeriod && toPlay > 1 * 1024){
        toPlay = 1 * 1024;
        if(ringBufferFill < toPlay) toPlay = ringBufferFill;
      }
      
      size_t readFromRing = ringBuf.read(playbackBuf, toPlay);
        
      if(readFromRing > 0){
        bool okWrite = true;
        size_t playedBytes = 0;
        
        if(channels == 1){
          size_t samples = readFromRing / 2;
          if(samples > 0){
            size_t outBytes = samples * 4;
            if(outBytes > stereoScratch.cap){
              outBytes = stereoScratch.cap;
              samples = outBytes / 4;
              readFromRing = samples * 2;
            }
            if(!stereoScratch.ensure(outBytes)){
              okWrite = false;
            } else {
              uint8_t *dst = stereoScratch.buf;
              if(!dst || stereoScratch.cap < outBytes) {
                okWrite = false;
              } else {
                for(size_t i=0;i<samples;i++){
                  uint8_t lo = playbackBuf[i*2+0];
                  uint8_t hi = playbackBuf[i*2+1];
                  size_t o = i*4;
                  if(o + 3 < stereoScratch.cap) {
                    dst[o+0]=lo; dst[o+1]=hi;
                    dst[o+2]=lo; dst[o+3]=hi;
                  }
                }
                size_t written = 0;
                TickType_t timeout = pdMS_TO_TICKS(500);
                esp_err_t err = i2s_write(I2S_NUM_1, dst, outBytes, &written, timeout);
                if(err != ESP_OK){
                  okWrite = false;
                } else {
                  playedBytes = written;
                  if(!rainbowStarted && written > 0){
                    // Sound started - start rainbow immediately
                    rainbowStarted = true;
                    ledState = LS_PLAYING;
                    ledRainbowStepFast(); // Start rainbow immediately when sound starts
                  }
                }
              }
            }
          }
        } else {
          size_t written = 0;
          TickType_t timeout = pdMS_TO_TICKS(500);
          esp_err_t err = i2s_write(I2S_NUM_1, playbackBuf, readFromRing, &written, timeout);
          if(err != ESP_OK){
            okWrite = false;
          } else {
            playedBytes = written;
            if(!rainbowStarted && written > 0){
              // Sound started - start rainbow immediately
              rainbowStarted = true;
              ledState = LS_PLAYING;
              ledRainbowStepFast(); // Start rainbow immediately when sound starts
            }
          }
        }
        
        if(!okWrite){
          break;
        }
        
        if(playedBytes > 0){
          size_t monoEquivalentBytes = (channels == 1) ? (playedBytes / 2) : playedBytes;
          statsTotalI2S += monoEquivalentBytes;
          
          if(ringBufferFill < LOW_BUFFER_THRESHOLD){
            delay(2);
          }
        }
      }
      
      if(toPlay < (READ_BUF / 2)){
        delay(2);
      }
      
      if(ringBuf.available() == 0 && downloadComplete){
        playbackComplete = true;
        // Playback ended - stop rainbow immediately before breaking
        if(rainbowStarted){
          rainbowStarted = false;
          ledState = LS_RANDOM_RGB;
          lastActivityMs = millis();
          lastRandomRgbChange = millis();
          ledRandomRgb(); // Stop rainbow immediately
        }
        break;
      }
      
      // Update rainbow continuously while sound is playing
      // Only update if playback is actually active and not paused
      if(rainbowStarted && !playbackPaused && ledState == LS_PLAYING){
        ledRainbowStepFast(); // Rainbow synchronized with sound playback
      }
      
      static unsigned long lastMQTTLoop = 0;
      if(mqttClient.connected() && (now - lastMQTTLoop > 100)){
        mqttClient.loop();
        lastMQTTLoop = now;
      }
    }
    
    // Sound stopped - stop rainbow IMMEDIATELY (before any cleanup)
    if(rainbowStarted){
      rainbowStarted = false;
      // CRITICAL: Stop rainbow immediately - set state and update LEDs BEFORE any delays
      ledState = LS_RANDOM_RGB; // Rainbow stops immediately when sound stops
      lastActivityMs = millis();
      lastRandomRgbChange = millis();
      ledRandomRgb(); // Switch to random RGB immediately - NO rainbow after this
      Serial.println("[LED] playback ended - rainbow stopped, random RGB");
    }
    
    // Cleanup: StereoScratch destructor will free its buffer automatically when it goes out of scope
    // Clear ring buffer first to ensure no pending operations
    ringBuf.clear();
    delay(5); // Reduced delay
    
    // Deinit ring buffer before freeing other buffers
    ringBuf.deinit();
    delay(2); // Reduced delay
    
    // Free allocated buffers in safe order
    if(httpReadBuf) {
      free(httpReadBuf);
      httpReadBuf = nullptr;
    }
    if(playbackBuf) {
      free(playbackBuf);
      playbackBuf = nullptr;
    }
    if(hdr) {
      free(hdr);
      hdr = nullptr;
    }
    
    // Close HTTP connection
    http.end();
    delay(2); // Reduced delay
    
    // Uninstall I2S
    uninstallI2STX();
    delay(5); // Reduced delay
    
    // Ensure LED state is still random RGB (not playing) after cleanup
    if(ledState == LS_PLAYING){
      ledState = LS_RANDOM_RGB;
      lastActivityMs = millis();
      lastRandomRgbChange = millis();
      ledRandomRgb();
    }
  }

  // -------- MQTT message handling --------
  void handlePlayControlJson(const JsonDocument &doc){
    if(!doc.containsKey("url")){ Serial.println("[MQTT] play/control missing url"); return; }
    
    if(playbackActive || isRecording || isStreaming){ 
      Serial.println("[PLAY] busy, ignoring"); 
      return; 
    }
    
    String url = doc["url"].as<String>();
    
    if(url == lastPlayedUrl && (millis() - lastPlaybackEndTime) < PLAYBACK_COOLDOWN_MS){
      Serial.printf("[PLAY] ignoring duplicate URL (cooldown: %lums)\n", 
        (unsigned long)(millis() - lastPlaybackEndTime));
      return;
    }
    
    uint32_t sr = doc.containsKey("sampleRate") ? (uint32_t)doc["sampleRate"].as<uint32_t>() : 0;
    Serial.printf("[PLAY] start url=%s sampleRate=%u\n", url.substring(0, 60).c_str(), (unsigned)sr);
    playbackActive = true; // Button disabled
    lastPlayedUrl = url;
    if(recorderInstalled){ uninstallI2SRecorder(); delay(40); }
    playUrlWavBlocking(url, sr);
    // Rainbow already stopped in playUrlWavBlocking when sound ends
    // CRITICAL: Ensure playbackActive is false and LED state is random RGB immediately after playback
    playbackActive = false; // Button enabled - set BEFORE any other operations
    if(ledState == LS_PLAYING){
      ledState = LS_RANDOM_RGB; // Force state to random RGB if still playing
      lastActivityMs = millis();
      lastRandomRgbChange = millis();
      ledRandomRgb(); // Update LEDs immediately
    }
    lastPlaybackEndTime = millis();
    lastActivityMs = millis();
    setupI2SRecorder();
    Serial.println("[PLAY] done");
  }
  void mqttCallback(char* topic, byte* payload, unsigned int length){
    String top = String(topic);
    if(top.endsWith("/audio/play/control")){
      DynamicJsonDocument doc(1024);
      DeserializationError err = deserializeJson(doc, payload, length);
      if(err){ Serial.println("[MQTT] JSON parse failed"); return; }
      lastActivityMs = millis();
      handlePlayControlJson(doc);
    }
  }

  // -------- Factory reset --------
  void doFactoryReset(){
    Serial.println("[RESET] factory reset - long press 10s+");
    ledState = LS_RESET;
    ledFill(255,0,0); // Red light for factory reset
    delay(500);
    // Stop audio if playing
    if(playbackActive){
      playbackActive = false;
      uninstallI2STX();
    }
    // Reset WiFi settings
    wm.resetSettings();
    WiFi.disconnect(true, true);
    // Format LittleFS
    if(LittleFS.format()) Serial.println("[FS] formatted");
    else Serial.println("[FS] format FAILED");
    delay(200);
    // Restart - will show blue light in captive portal
    ESP.restart();
  }

  // -------- Button handling --------
  void handleButtonTick(){
    // Disable button during streaming, recording, and playback
    if(playbackActive || isRecording || isStreaming){
      if(btnPhase != BP_IDLE){
        btnPhase = BP_IDLE;
        pressTs = 0;
        releaseTs = 0;
      }
      return;
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
            // Short press: Start 8 second recording (button disabled during recording)
            isRecording = true; // Button disabled during recording
            sessionCounter++; persistSessionCounter();
            char sid[32]; snprintf(sid, sizeof(sid), "%lu", sessionCounter);
            Serial.printf("[BTN] short press -> record 8s sid=%s\n", sid);
            ledState = LS_RECORDING; 
            ledFill(0,0,255); // Blue light during 8 second recording
            lastActivityMs = now;
            recordTimedToFile(sid, RECORD_TIMEOUT_MS);
            // After recording: Stream to MQTT (button still disabled during streaming)
            if(!playbackActive){
              streamSessionFileToMQTT(sid);
              // After streaming: Random RGB light (already set in streamSessionFileToMQTT), then idle animation after 15s
            } else {
              isRecording = false; // Button enabled if playback active
            }
          } else {
            // Medium press: treat as short press
            isRecording = true; // Button disabled during recording
            sessionCounter++; persistSessionCounter();
            char sid[32]; snprintf(sid, sizeof(sid), "%lu", sessionCounter);
            Serial.printf("[BTN] medium press -> record 8s sid=%s\n", sid);
            ledState = LS_RECORDING; 
            ledFill(0,0,255); // Blue light during recording (8 seconds)
            lastActivityMs = now;
            recordTimedToFile(sid, RECORD_TIMEOUT_MS);
            if(!playbackActive){
              streamSessionFileToMQTT(sid);
              // After streaming: Random RGB light (already set in streamSessionFileToMQTT)
            } else {
              isRecording = false; // Button enabled if playback active
            }
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

    // WiFi: open portal with SSID only (no password) - auto connect
    wm.setTimeout(120);
    wm.setConfigPortalTimeout(120);
    wm.setAPStaticIPConfig(IPAddress(192,168,4,1), IPAddress(192,168,4,1), IPAddress(255,255,255,0));
    wm.setAPCallback([](WiFiManager *myWiFiManager) {
      // AP started callback - blue light during provisioning
      ledState = LS_PROVISION;
      ledFill(0,0,255);
    });
    ledState = LS_PROVISION; ledFill(0,0,255); // Blue during provisioning
    if(!wm.autoConnect(AP_SSID)){ // No password - open AP
      delay(2000); ESP.restart();
    }
    WiFi.setSleep(false);
    Serial.printf("[WiFi] connected: %s\n", WiFi.localIP().toString().c_str());
    ledState = LS_CONNECTED; ledFill(0,255,0); // Green when WiFi connected
    lastActivityMs = millis();

    configTime(19800, 0, "pool.ntp.org", "time.google.com");
    delay(600);

    mqttClient.setCallback(mqttCallback);
    connectMQTT();

    Wire.begin();
    initCodec();
    
    g_hpf.init(80.0f, (float)SAMPLE_RATE);
    g_notch.init50Hz((float)SAMPLE_RATE);

    setupI2SRecorder();

    TOP_PLAY_CTRL = String(DEVICE_ID) + "/audio/play/control";

    Serial.println("[READY] Short press -> record 8s (upload). Long press >=10s -> factory reset.");
  }

  unsigned long lastIdleAnim = 0;
  void loop(){
    if(WiFi.isConnected()){
      if(!mqttClient.connected()){
        if(millis() - lastMQTTAttempt > MQTT_RECONNECT_MS){
          lastMQTTAttempt = millis();
          connectMQTT();
        }
      } else mqttClient.loop();
    }

    handleButtonTick();

    unsigned long now = millis();
    
    // Priority 1: WiFi disconnected - Blue light
    if(!WiFi.isConnected()){
      if(ledState != LS_RECORDING && ledState != LS_RESET && !playbackActive && !isStreaming && !isRecording) { 
        ledState = LS_DISCONNECTED; 
        ledFill(0,0,255); // Blue when WiFi disconnected
      }
    } 
    // Priority 2: WiFi connected - Green light
    else if(ledState == LS_DISCONNECTED && !playbackActive && !isRecording && !isStreaming && ledState != LS_RESET){
      ledState = LS_CONNECTED;
      ledFill(0,255,0); // Green when WiFi connected
      lastActivityMs = now;
    }
    
    // Priority 3: After streaming/recording - Random RGB, then idle animation after 15s
    if(ledState == LS_RANDOM_RGB){
      if(now - lastRandomRgbChange > 500){
        ledRandomRgb();
        lastRandomRgbChange = now;
      }
      // After 15s inactivity: Start animation theme (dhere dhere change)
      if((now - lastActivityMs >= IDLE_ANIM_AFTER_MS) && 
        !playbackActive && !isRecording && !isStreaming && WiFi.isConnected()){
        ledState = LS_IDLE_ANIM;
        lastIdleAnim = now;
        Serial.println("[LED] idle animation started (15s inactivity)");
      }
    }
    
    // Priority 4: After 15s idle - Start animation theme (from CONNECTED or RANDOM_RGB)
    if((ledState == LS_CONNECTED || ledState == LS_RANDOM_RGB) && 
      (now - lastActivityMs >= IDLE_ANIM_AFTER_MS) && 
      !playbackActive && !isRecording && !isStreaming && WiFi.isConnected()){
      ledState = LS_IDLE_ANIM;
      lastIdleAnim = now;
      Serial.println("[LED] idle animation started (15s inactivity)");
    }
    
    // LED state machine - render current state
    switch(ledState){
      case LS_CONNECTED: 
        ledFill(0,255,0); // Green - WiFi connected
        break;
      case LS_IDLE_ANIM:
        if(now - lastIdleAnim > IDLE_ANIM_STEP_MS){
          lastIdleAnim = now;
          ledIdleAmbientStep();
        }
        break;
      case LS_RECORDING:
        ledFill(0,0,255); // Blue - Recording (8 seconds)
        break;
      case LS_STREAMING:
        // NO rainbow during streaming - just random RGB
        // Update random RGB periodically
        if(now - lastRandomRgbChange > 500){
          ledRandomRgb();
          lastRandomRgbChange = now;
        }
        break;
      case LS_PLAYING:
        // Rainbow theme ONLY during active playback - STRICT check
        // If playbackActive is false, immediately switch to random RGB (playback ended)
        if(playbackActive == true && ledState == LS_PLAYING){
          // Only show rainbow if playback is actually active
          ledRainbowStepFast();
        } else {
          // Playback ended - immediately stop rainbow and switch to random RGB
          playbackActive = false; // Ensure flag is cleared
          ledState = LS_RANDOM_RGB;
          lastActivityMs = now;
          lastRandomRgbChange = now;
          ledRandomRgb(); // Stop rainbow immediately, show random RGB
          Serial.println("[LED] playback ended in loop - rainbow stopped");
        }
        break;
      case LS_RANDOM_RGB:
        break;
      case LS_DISCONNECTED:
        ledFill(0,0,255); // Blue - WiFi disconnected
        break;
      case LS_RESET:
        ledFill(255,0,0); // Red - Factory reset
        break;
      case LS_BOOT:
      case LS_PROVISION:
        break;
      default: 
        break;
    }

    delay(4);
  }