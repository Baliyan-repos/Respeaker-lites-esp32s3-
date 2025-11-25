
📘 Rannkly Nova AI Speaker — Complete Technical Documentation

🧠 1. Introduction

The Rannkly Nova AI Speaker is a smart IoT audio device designed for hospitality environments such 
as hotels and service apartments.
It allows guests to make service requests, play audio prompts, or interact with a voice AI assistant 
through the AWS IoT Cloud using MQTT communication.
Nova connects over WiFi, retrieves audio URLs from the cloud, and streams them directly using the 
ESP32-S3 audio I2S interface — providing real-time, low-latency audio output.



⚙️ 2. System Overview
Nova combines multiple systems into one:
     
Subsystem
Function
WiFi Manager
Handles WiFi provisioning (auto portal if not configured)
MQTT Client
Communicates with AWS IoT Core securely via TLS
Audio Player
Streams and plays cloud-hosted MP3/WAV files
RGB LED Controller
Indicates system states (WiFi, idle, playing, error)
Button Controller
User input (short press = event, long press = WiFi reset)






🧩 3. Hardware Overview
  
Component
Description
Pin
ESP32-S3 (ReSpeaker Lite)
Main MCU for IoT and audio
—
I2S Audio Out
Connected to MAX98357A amplifier
BCLK=8, WS=7, DATA=43, MCLK=44
Button (User Input)
For actions and WiFi reset
GPIO3
WS2812 RGB LEDs (8x)
For status and animation
GPIO1
Power Supply
5V regulated
—







📈 Hardware Connection Diagram (simplified)

           +-------------------+
             |   ESP32-S3 Board  |
             |                   |
             |  GPIO8 -> BCLK    |
             |  GPIO7 -> WS      |
             |  GPIO43 -> DATA   |
             |  GPIO44 -> MCLK   |
             |  GPIO1 -> WS2812  |
             |  GPIO3 -> BUTTON  |
             |                   |
             +-------------------+
                    |     |
                    |     +---> I2S Amplifier (MAX98357A)
                    |+---> 8x WS2812 RGB LEDs





💡 4. Software & Libraries Used

Library
Purpose
Source
WiFiManager.h
Handles WiFi portal provisioning
Arduino Library Manager
WiFiClientSecure.h
Enables TLS (secure MQTT)
Built-in
PubSubClient.h
MQTT communication
Arduino Library Manager
ArduinoJson.h
Parses JSON payloads from MQTT
Arduino Library Manager
Adafruit_NeoPixel.h
Controls WS2812 RGB LEDs
Adafruit
    Audio.h 
(ESP32-audioI2S)
Streams MP3/WAV over HTTP(S)
       GtHub: earlephilhower
/ESP32-audioI2S



🌐 5. AWS IoT Core Setup

  Follow these steps to connect Nova to AWS IoT securely.

Step 1: Create an IoT Thing -
Go to AWS Console → IoT Core → Manage → Things → Create a Thing
Name: Rannkly_Nova_01
Choose “Single Thing”
Create certificates and download:
AmazonRootCA1.pem
Device-certificate.pem.crt
private.pem.key


Step 2: Create a Policy -
Go to Security → Policies → Create
Name: NovaPolicy
Action: iot:*
Resource: *
Effect: Allow

Step 3: Attach Policy & Certificate -
Attach your NovaPolicy to your certificate.
Attach the certificate to your IoT Thing.

Step 4: Get MQTT EndpointGo to Settings → Endpoint
 Example: a1b2c3d4XXXXXXXXXXXXXX-1.amazonaws.com


🧩 AWS Connection Diagram


[ Nova Speaker ] 
   │
   ▼
WiFi → MQTT (TLS) → AWS IoT Core → MQTT Topic → Lambda/API → Audio URL

    
 🔗 6. MQTT Communication Flow

 Direction
Topic
Payload
Description
  Cloud→
Device
 Esp32_1
/sub
{" url": 
 "<audio_url>"}
Play a specific 
MP3 URL
     Device→
Cloud
 Esp32_1
/pub
 {"playing":1}
Device is 
now playing
      Device→
 Cloud
 Esp32_1
/pub
 {"playing":0}
 Playback stopped
     Device→
Cloud
  {"status”:
  "online"}
 On boot 
  /reconnect
-


🔧 7. Firmware Architecture
         Nova firmware is written in C++ (Arduino IDE) and divided into modules:


Module
Responsibility
WiFi Setup
WiFiManager handles saved credentials and AP portal
MQTT Core
Handles TLS secure connection with AWS IoT
Audio Module
Streams and plays audio using ESP32 I2S
LED Manager
Controls RGB themes based on device state
Button Handler
Detects short and long press actions





🖥️ 8. Firmware Boot Flow

                     	+-------------------------------+
| Boot ESP32-S3                 |
+-------------------------------+
         |
         v
 Check Button at Boot
 (if held -> Clear WiFi)
         |
         v
 WiFiManager.autoConnect()
 (AP Portal if no WiFi)
         |
         v
 Connect to AWS MQTT
         |
         v
 Subscribe to `esp32_1/sub`
         |
         v
 Idle State (LED Blue)
         |
         v
 MQTT Message → Play Audio
 (LED Rainbow Fast)
         |
         v
 Audio Done → Back to Blue
         |
         v
 Idle 15s → Slow LED Animation


🎨 9. LED Behavior & Themes

State
LED Color
Description
WiFi Provisioning
Blue (Solid)
AP mode active
Connecting to WiFi
White Blinking
Attempting to connect
Connected (Idle)
Green (Solid)
Connected successfully
    Idle (no movement >15s)
Slow Color Fade
Ambient theme
Button Press
Orange Blink
Feedback pulse
Audio Streaming
Fast Rainbow Animation
Playback active
Error / MQTT Fail
Red Solid
Connection or audio error


🔘 10. Button Logic -


Action
Duration
Behavior
Short Press
< 1s
Orange Blink → Trigger activity
Long Press
> 10s
Red Light → Erase WiFi → Restart


    

  🔊 11. Audio System

      Library: Audio.h (ESP32-audioI2S)
      Function: Streams audio directly via audio.connecttohost(url)
   Formats: MP3 / WAV / AAC supported
      Pipeline:
                     MQTT → JSON Parse → Extract URL → Stream via HTTP(S)


🌈 12. LED Animation System
        Ambient Mode (Idle):
Triggered after 15s of inactivity.
Smoothly transitions between colors (soft brightness).


        Rainbow Mode (Playback):
Active when streaming.
Cycles hues rapidly for vibrant feedback



🔐 13. Security Overview
MQTT over TLS (Port 8883)
Uses AWS Root CA + device certificate + private key
Certificates stored in certs.h
Example:
#define AWS_IOT_ENDPOINT "a1b2c3d4e5XXXXXXXXXXXXXXXamazonaws.com"
static const char AWS_CERT_CA[] PROGMEM = R"EOF(-----BEGIN 
CERTIFICATE-----)";
Automatic reconnection if WiFi or MQTT drops.

⚙️ 14. Deployment Process
Install Arduino IDE
Add ESP32 Boards URL:
 https://espressif.github.io/arduino-esp32/package_esp32_index.json
Install required libraries:
(WiFiManager, PubSubClient, ArduinoJson, Adafruit NeoPixel, ESP32-audioI2S)

4. Add your certs in certs.h
5. Compile and upload firmware via USB-C.
6. Open Serial Monitor → 115200 baud.

🧠 15. Testing Process
Power on the device.
Hold button 10s to open WiFi portal → connect via ESP32_Speaker_Config.
Enter WiFi credentials.
Once connected → Green LED.

From AWS IoT → Publish message:
{"url":"https://example.com/test.mp3"}
Observe rainbow animation while audio plays.

🧩 16. Troubleshooting
Problem
Cause
Fix
No WiFi Portal
Button not held at boot
Hold for 10 seconds
MQTT not connecting
Port 8883 blocked
Use mobile hotspot
LED stuck on red
Certificate issue
Re-upload valid certs
Audio distortion
Incorrect I2S wiring
Check pins (8,7,43,44)

🚀 17. Future Upgrades
Feature
Description
Voice Recognition (STT)
Add microphone for voice-triggered commands
OTA Updates
Allow remote firmware upgrades
Hotel API Integration
Directly fetch service requests via REST API
Local Storage
Cache frequent audio messages


🧾 18. Appendix — Firmware Summary
Module
Description
main.ino
Full firmware (MQTT + Audio + LED + WiFi)
certs.h
TLS credentials
libraries.txt
Required Arduino dependencies



⚙️ Pinout Summary
       ESP32-S3 (ReSpeaker Lite)
-------------------------
GPIO8   → I2S BCLK
GPIO7   → I2S WS
GPIO43  → I2S DATA
GPIO44  → I2S MCLK
GPIO1   → WS2812 LEDs
GPIO3   → Button
5V      → Power
GND     → Ground


📊 System Block Diagram (Text Version)
               +--------------------+
     |  AWS IoT Core      |
     |  (MQTT Broker)     |
     +--------+-----------+
              |
              | TLS MQTT 8883
              |
     +--------v-----------+
     | Rannkly Nova AI    |
     | ESP32-S3 Firmware  |
     +--------+-----------+
              |
       +------+-------+
       | I2S Audio Out|--> Speaker
       | WS2812 LEDs  |--> RGB Feedback
       | Button Input |--> Control



✅ Summary
The Rannkly Nova AI Speaker combines IoT connectivity, audio streaming, and LED visualization in a single compact unit powered by the ESP32-S3 platform.
Its integration with AWS IoT provides a scalable, secure, and cloud-driven architecture suitable for smart hospitality environments.
