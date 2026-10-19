
import express from "express";
import http from "http";
import WebSocket, { WebSocketServer } from "ws";
import dotenv from "dotenv";

dotenv.config();

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

const PORT = Number(process.env.PORT || 10000);
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

const GEMINI_MODEL =
  "gemini-2.5-flash-native-audio-preview-12-2025";

const GEMINI_URL =
  `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;

if (!GEMINI_API_KEY) {
  console.error("ERROR: GEMINI_API_KEY is missing.");
  process.exit(1);
}

app.get("/", (_req, res) => {
  res.json({
    service: "EbykeShop AI Voice Agent",
    status: "running",
    websocket: "/media",
    mode: "temporary tokenless testing"
  });
});

app.get("/health", (_req, res) => {
  res.status(200).json({ status: "ok" });
});

/*
 * TEMPORARY TEST MODE:
 * WebSocket token authentication is intentionally disabled.
 * Use only for your own test calls.
 */
server.on("upgrade", (req, socket, head) => {
  let pathname;

  try {
    pathname = new URL(
      req.url,
      `http://${req.headers.host}`
    ).pathname;
  } catch {
    socket.destroy();
    return;
  }

  console.log("[INFO] WebSocket request path:", pathname);

  if (pathname !== "/media") {
    socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
    socket.destroy();
    return;
  }

  console.warn(
    "[WARNING] TEST MODE: accepting WebSocket without token"
  );

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});

/*
 * Convert mono PCM16 audio between sample rates.
 * This is a simple linear interpolation resampler.
 */
function resamplePcm16(inputBuffer, inputRate, outputRate) {
  if (!inputBuffer || inputBuffer.length < 2) {
    return Buffer.alloc(0);
  }

  const usableLength = inputBuffer.length - (inputBuffer.length % 2);
  const inputSamples = usableLength / 2;
  const outputSamples = Math.max(
    1,
    Math.floor(inputSamples * outputRate / inputRate)
  );

  const outputBuffer = Buffer.alloc(outputSamples * 2);

  for (let i = 0; i < outputSamples; i++) {
    const sourcePosition = i * inputRate / outputRate;
    const leftIndex = Math.min(
      Math.floor(sourcePosition),
      inputSamples - 1
    );
    const rightIndex = Math.min(
      leftIndex + 1,
      inputSamples - 1
    );
    const fraction = sourcePosition - leftIndex;

    const left = inputBuffer.readInt16LE(leftIndex * 2);
    const right = inputBuffer.readInt16LE(rightIndex * 2);

    const sample = Math.round(
      left + (right - left) * fraction
    );

    outputBuffer.writeInt16LE(
      Math.max(-32768, Math.min(32767, sample)),
      i * 2
    );
  }

  return outputBuffer;
}

function sendExotelAudio(exotelWs, streamSid, audioBase64) {
  if (exotelWs.readyState !== WebSocket.OPEN) {
    return;
  }

  const message = {
    event: "media",
    media: {
      payload: audioBase64
    }
  };

  if (streamSid) {
    message.streamSid = streamSid;
  }

  exotelWs.send(JSON.stringify(message));
}

wss.on("connection", (exotelWs) => {
  console.log("[INFO] Exotel WebSocket connected");

  let geminiWs;
  let streamSid = null;
  let geminiReady = false;
  let greetingSent = false;
  let exotelAudioLogged = false;
  let geminiAudioLogged = false;
  let closed = false;

  function cleanup() {
    if (closed) return;
    closed = true;

    console.log("[INFO] Cleaning up call connections");

    if (
      geminiWs &&
      geminiWs.readyState !== WebSocket.CLOSED
    ) {
      geminiWs.close();
    }
  }

  exotelWs.on("message", (rawMessage) => {
    let message;

    try {
      message = JSON.parse(rawMessage.toString());
    } catch {
      console.warn("[WARN] Could not parse Exotel message");
      return;
    }

    if (message.event === "connected") {
      console.log("[INFO] Exotel connected event received");
      return;
    }

    if (message.event === "start") {
      streamSid =
        message.start?.streamSid ||
        message.streamSid ||
        null;

      console.log("[INFO] Exotel stream started", {
        streamSidReceived: Boolean(streamSid)
      });

      return;
    }

    if (message.event === "stop") {
      console.log("[INFO] Exotel stream stopped");
      cleanup();
      return;
    }

    if (message.event === "media") {
      const payload = message.media?.payload;

      if (!payload || !geminiReady || !geminiWs) {
        return;
      }

      if (geminiWs.readyState !== WebSocket.OPEN) {
        return;
      }

      if (!exotelAudioLogged) {
        console.log("[INFO] First Exotel audio event received");
        exotelAudioLogged = true;
      }

      try {
        // Exotel input is assumed to be mono PCM16 at 8 kHz.
        const inputPcm = Buffer.from(payload, "base64");
        const pcm16k = resamplePcm16(inputPcm, 8000, 16000);

        if (pcm16k.length === 0) return;

        geminiWs.send(JSON.stringify({
          realtimeInput: {
            audio: {
              mimeType: "audio/pcm;rate=16000",
              data: pcm16k.toString("base64")
            }
          }
        }));
      } catch (error) {
        console.error(
          "[ERROR] Forwarding Exotel audio:",
          error.message
        );
      }
    }
  });

  geminiWs = new WebSocket(GEMINI_URL);

  geminiWs.on("open", () => {
    console.log("[INFO] Gemini WebSocket connected");

    const setupMessage = {
      setup: {
        model: `models/${GEMINI_MODEL}`,

        generationConfig: {
          responseModalities: ["AUDIO"],
          temperature: 0.6,
          speechConfig: {
            voiceConfig: {
              prebuiltVoiceConfig: {
                voiceName: "Aoede"
              }
            }
          }
        },

        systemInstruction: {
          parts: [{
            text: `
You are the friendly AI calling assistant for EbykeShop,
an electric bicycle store in Bangalore, India.

SPEAKING STYLE:
- Speak in natural, conversational Indian English.
- Use a warm, polite, professional Indian communication style.
- Avoid American slang, American idioms, and overly American expressions.
- Do not exaggerate slang or imitate a stereotype.
- Speak clearly and at a natural pace.
- Keep responses concise, usually one or two sentences.
- Ask one question at a time.
- Let the customer finish speaking before replying.
- Avoid unnecessary pauses, repeated greetings, and long explanations.
- Sound like a helpful Indian sales representative.

SALES CONVERSATION:
- Understand the customer's electric bicycle requirements.
- Ask about preferred bike type, budget, and intended use.
- Help the customer explore a test ride when relevant.
- Never invent prices, specifications, discounts, or stock availability.
- If you do not know an answer, politely say you will confirm it.
- Be helpful, natural, and never pushy.

RESPONSE SPEED:
- Prefer short, direct answers.
- Do not repeat the customer's entire question.
- Do not add unnecessary introductions.
`
          }]
        },

        realtimeInputConfig: {
          automaticActivityDetection: {
            disabled: false,
            endOfSpeechSensitivity: "END_SENSITIVITY_HIGH",
            silenceDurationMs: 300,
            prefixPaddingMs: 20
          },
          activityHandling: "START_OF_ACTIVITY_INTERRUPTS"
        }
      }
    };

    geminiWs.send(JSON.stringify(setupMessage));
    console.log("[INFO] Gemini setup message sent");
  });

  geminiWs.on("message", (rawMessage) => {
    let message;

    try {
      message = JSON.parse(rawMessage.toString());
    } catch {
      console.warn("[WARN] Could not parse Gemini message");
      return;
    }

    if (message.setupComplete) {
      console.log("[INFO] Gemini session setup complete");
      geminiReady = true;

      if (!greetingSent) {
        greetingSent = true;

        console.log(
          "[INFO] Sending initial greeting request to Gemini"
        );

        geminiWs.send(JSON.stringify({
          realtimeInput: {
            text:
              "Greet the customer briefly. Say you are the AI assistant calling from EbykeShop in Bangalore, and ask how you can help with their electric bicycle requirements."
          }
        }));
      }

      return;
    }

    if (message.error) {
      console.error(
        "[ERROR] Gemini API error:",
        JSON.stringify(message.error)
      );
      return;
    }

    if (message.serverContent?.interrupted) {
      console.log("[INFO] Gemini reported an interruption");
      return;
    }

    const parts =
      message.serverContent?.modelTurn?.parts || [];

    for (const part of parts) {
      if (part.inlineData?.data) {
        if (!geminiAudioLogged) {
          console.log("[INFO] First Gemini audio chunk received");
          geminiAudioLogged = true;
        }

        try {
          // Gemini output is assumed to be mono PCM16 at 24 kHz.
          const pcm24k = Buffer.from(
            part.inlineData.data,
            "base64"
          );

          const pcm8k = resamplePcm16(
            pcm24k,
            24000,
            8000
          );

          if (pcm8k.length > 0) {
            sendExotelAudio(
              exotelWs,
              streamSid,
              pcm8k.toString("base64")
            );
          }
        } catch (error) {
          console.error(
            "[ERROR] Processing Gemini audio:",
            error.message
          );
        }
      }

      if (part.text) {
        console.log("[GEMINI TEXT]", part.text);
      }
    }

    if (message.serverContent?.turnComplete) {
      console.log("[INFO] Gemini turn completed");
    }
  });

  geminiWs.on("error", (error) => {
    console.error("[ERROR] Gemini WebSocket:", error.message);
  });

  geminiWs.on("close", (code, reason) => {
    geminiReady = false;
    console.log(
      "[INFO] Gemini WebSocket closed:",
      code,
      reason.toString()
    );

    if (
      exotelWs.readyState === WebSocket.OPEN
    ) {
      exotelWs.close();
    }
  });

  exotelWs.on("error", (error) => {
    console.error("[ERROR] Exotel WebSocket:", error.message);
  });

  exotelWs.on("close", () => {
    console.log("[INFO] Exotel WebSocket closed");
    cleanup();
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(
    `[INFO] EbykeShop AI Voice Agent listening on port ${PORT}`
  );
});
