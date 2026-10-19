
import "dotenv/config";
import express from "express";
import WebSocket, { WebSocketServer } from "ws";

const app = express();
const PORT = Number(process.env.PORT || 3000);
const API_KEY = process.env.GEMINI_API_KEY;
const STREAM_TOKEN = process.env.EXOTEL_STREAM_TOKEN;
const MODEL =
  process.env.GEMINI_MODEL ||
  "gemini-2.5-flash-native-audio-preview-12-2025";

app.get("/", (_req, res) => {
  res.json({
    service: "EbykeShop AI Voice Agent",
    status: "running",
    websocket: "/media",
  });
});

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    apiKeyConfigured: Boolean(API_KEY),
    streamTokenConfigured: Boolean(STREAM_TOKEN),
  });
});

const server = app.listen(PORT, "0.0.0.0", () => {
  console.log(`Voice agent listening on port ${PORT}`);
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  let pathname;
  let token;

  try {
    const url = new URL(req.url, `http://${req.headers.host}`);
    pathname = url.pathname;
    token = url.searchParams.get("token");
  } catch {
    socket.destroy();
    return;
  }

  if (
    pathname !== "/media" ||
    !STREAM_TOKEN ||
    token !== STREAM_TOKEN
  ) {
    socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});

// Exotel audio is assumed to be raw 8 kHz, signed 16-bit,
// mono PCM, little-endian. Confirm this in the Exotel configuration.
function upsample8kTo16k(base64) {
  const input = Buffer.from(base64, "base64");
  const usableLength = input.length - (input.length % 2);
  const count = usableLength / 2;

  if (count === 0) return "";

  const output = Buffer.alloc(count * 4);

  for (let i = 0; i < count; i++) {
    const current = input.readInt16LE(i * 2);
    const next =
      i + 1 < count ? input.readInt16LE((i + 1) * 2) : current;

    output.writeInt16LE(current, i * 4);
    output.writeInt16LE(
      Math.round((current + next) / 2),
      i * 4 + 2
    );
  }

  return output.toString("base64");
}

// Approximate 24 kHz -> 8 kHz decimation.
// Keep sample groups across Gemini audio chunks.
function createDownsampler() {
  let pending = [];

  return function downsample(base64) {
    const input = Buffer.from(base64, "base64");
    const usableLength = input.length - (input.length % 2);

    for (let i = 0; i < usableLength; i += 2) {
      pending.push(input.readInt16LE(i));
    }

    const outputSamples = [];
    let index = 0;

    while (index + 2 < pending.length) {
      const average = Math.round(
        (pending[index] +
          pending[index + 1] +
          pending[index + 2]) /
          3
      );

      outputSamples.push(
        Math.max(-32768, Math.min(32767, average))
      );
      index += 3;
    }

    pending = pending.slice(index);

    const output = Buffer.alloc(outputSamples.length * 2);

    outputSamples.forEach((sample, i) => {
      output.writeInt16LE(sample, i * 2);
    });

    return output.toString("base64");
  };
}

wss.on("connection", (exotelWs) => {
  console.log("Exotel WebSocket connected");

  if (!API_KEY) {
    console.error("GEMINI_API_KEY is missing");
    exotelWs.close(1011, "Server configuration error");
    return;
  }

  let streamSid = null;
  let geminiReady = false;
  let greetingSent = false;
  let closed = false;

  const pendingAudio = [];
  const downsampleAudio = createDownsampler();

  const geminiUrl =
    "wss://generativelanguage.googleapis.com/ws/" +
    "google.ai.generativelanguage.v1beta.GenerativeService." +
    "BidiGenerateContent?key=" +
    encodeURIComponent(API_KEY);

  const geminiWs = new WebSocket(geminiUrl);

  function sendGemini(message) {
    if (geminiWs.readyState === WebSocket.OPEN) {
      geminiWs.send(JSON.stringify(message));
    }
  }

  function sendCallerAudio(base64Audio) {
    if (
      exotelWs.readyState !== WebSocket.OPEN ||
      !streamSid ||
      !base64Audio
    ) {
      return;
    }

    exotelWs.send(
      JSON.stringify({
        event: "media",
        stream_sid: streamSid,
        media: { payload: base64Audio },
      })
    );
  }

  function sendGreeting() {
    if (greetingSent || !geminiReady) return;
    greetingSent = true;

    sendGemini({
      realtimeInput: {
        text:
          "Start the call now. Say: Hi, I'm an AI assistant calling " +
          "on behalf of EbykeShop, an electric bicycle store in " +
          "Bangalore. Is this a good time for a quick conversation? " +
          "Keep it friendly, natural, and brief.",
      },
    });
  }

  geminiWs.on("open", () => {
    console.log("Gemini WebSocket connected");

    sendGemini({
      setup: {
        model: `models/${MODEL}`,
        generationConfig: {
          responseModalities: ["AUDIO"],
        },
        systemInstruction: {
          parts: [
            {
              text:
                "You are a polite, friendly AI sales assistant calling " +
                "on behalf of E byke Shop, an electric bicycle store in " +
                "Bangalore, India. Identify yourself as an AI assistant. " +
                "Speak naturally in conversational English, without " +
                "forced slang. Ask permission before continuing. If the " +
                "caller agrees, ask one question at a time about their " +
                "interest in an electric bicycle, who will use it, its " +
                "main purpose, expected purchase timeline, and approximate " +
                "budget in Indian rupees. Listen before asking the next " +
                "question. Respect refusals and end politely. Never claim " +
                "an order or purchase is confirmed. Do not request " +
                "sensitive personal information. This is a test call.",
            },
          ],
        },
        realtimeInputConfig: {
          automaticActivityDetection: {
            disabled: false,
          },
        },
      },
    });
  });

  geminiWs.on("message", (raw) => {
    let message;

    try {
      message = JSON.parse(raw.toString());
    } catch {
      console.error("Invalid Gemini message received");
      return;
    }

    if (message.setupComplete) {
      geminiReady = true;
      console.log("Gemini session setup complete");

      while (pendingAudio.length > 0) {
        sendCallerAudio(pendingAudio.shift());
      }

      sendGreeting();
    }

    const parts = message.serverContent?.modelTurn?.parts || [];

    for (const part of parts) {
      if (!part.inlineData?.data) continue;

      const mimeType = part.inlineData.mimeType || "";

      if (!mimeType.includes("audio/pcm")) continue;

      const exotelAudio = downsampleAudio(part.inlineData.data);
      sendCallerAudio(exotelAudio);
    }

    if (message.serverContent?.interrupted) {
      // Exotel clear-message support must be confirmed for your
      // Voicebot Applet before enabling interruption handling.
      console.log("Gemini reported an interruption");
    }

    if (message.error) {
      console.error(
        "Gemini API error:",
        JSON.stringify(message.error)
      );
    }

    if (message.goAway) {
      console.warn("Gemini session is preparing to close");
    }
  });

  geminiWs.on("error", (error) => {
    console.error("Gemini WebSocket error:", error.message);
  });

  geminiWs.on("close", (code) => {
    console.log("Gemini WebSocket closed:", code);

    if (!closed && exotelWs.readyState === WebSocket.OPEN) {
      exotelWs.close(1011, "AI session ended");
    }
  });

  exotelWs.on("message", (raw) => {
    let event;

    try {
      event = JSON.parse(raw.toString());
    } catch {
      console.warn("Ignoring invalid Exotel message");
      return;
    }

    if (event.event === "start") {
      streamSid =
        event.start?.stream_sid ||
        event.start?.streamSid ||
        event.stream_sid ||
        null;

      console.log("Exotel stream started");
    }

    if (event.event === "media" && event.media?.payload) {
      const audio16k = upsample8kTo16k(event.media.payload);

      if (geminiReady) {
        sendGemini({
          realtimeInput: {
            audio: {
              data: audio16k,
              mimeType: "audio/pcm;rate=16000",
            },
          },
        });
      } else if (pendingAudio.length < 30) {
        pendingAudio.push(audio16k);
      }
    }

    if (event.event === "stop") {
      console.log("Exotel stream stopped");

      if (geminiWs.readyState === WebSocket.OPEN) {
        geminiWs.close();
      }
    }
  });

  exotelWs.on("close", () => {
    closed = true;
    console.log("Exotel WebSocket closed");

    if (
      geminiWs.readyState === WebSocket.OPEN ||
      geminiWs.readyState === WebSocket.CONNECTING
    ) {
      geminiWs.close();
    }
  });

  exotelWs.on("error", (error) => {
    console.error("Exotel WebSocket error:", error.message);
  });
});
