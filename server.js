
import "dotenv/config";
import express from "express";
import WebSocket, { WebSocketServer } from "ws";

const app = express();
const PORT = process.env.PORT || 3000;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL =
  process.env.GEMINI_MODEL || "gemini-2.5-flash-native-audio-preview-12-2025";

app.get("/", (_req, res) => {
  res.json({
    service: "Exotel Gemini Voice Agent",
    status: "running",
    websocket: "/media",
  });
});

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

const server = app.listen(PORT, "0.0.0.0", () => {
  console.log(`Voice agent listening on port ${PORT}`);
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  const pathname = new URL(
    request.url,
    `http://${request.headers.host}`
  ).pathname;

  if (pathname !== "/media") {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit("connection", ws, request);
  });
});

function pcm24kTo8k(base64Audio) {
  const input = Buffer.from(base64Audio, "base64");
  const output = Buffer.alloc(Math.floor(input.length / 6) * 2);

  // Basic 3:1 downsampling for initial testing.
  // Production audio should use a proper low-pass resampler.
  let outOffset = 0;

  for (let i = 0; i + 5 < input.length; i += 6) {
    output.writeInt16LE(input.readInt16LE(i), outOffset);
    outOffset += 2;
  }

  return output.subarray(0, outOffset).toString("base64");
}

wss.on("connection", (exotelWs) => {
  console.log("Exotel WebSocket connected");

  if (!GEMINI_API_KEY) {
    console.error("GEMINI_API_KEY is missing");
    exotelWs.close(1011, "Server configuration error");
    return;
  }

  let streamSid = null;
  let geminiReady = false;
  let greetingSent = false;
  const pendingAudio = [];

  const geminiUrl =
    "wss://generativelanguage.googleapis.com/ws/" +
    "google.ai.generativelanguage.v1beta.GenerativeService." +
    "BidiGenerateContent?key=" +
    encodeURIComponent(GEMINI_API_KEY);

  const geminiWs = new WebSocket(geminiUrl);

  function sendGemini(message) {
    if (geminiWs.readyState === WebSocket.OPEN) {
      geminiWs.send(JSON.stringify(message));
    }
  }

  function forwardAudio(base64Audio) {
    sendGemini({
      realtimeInput: {
        audio: {
          data: base64Audio,
          mimeType: "audio/pcm;rate=8000",
        },
      },
    });
  }

  function sendGreetingRequest() {
    if (greetingSent || !geminiReady) return;

    greetingSent = true;

    sendGemini({
      clientContent: {
        turns: [
          {
            role: "user",
            parts: [
              {
                text:
                  "Start the phone conversation now. Briefly introduce " +
                  "yourself as an AI assistant calling on behalf of an " +
                  "E bike shop. Ask permission to continue. " +
                  "Speak naturally and keep the opening short.",
              },
            ],
          },
        ],
        turnComplete: true,
      },
    });
  }

  geminiWs.on("open", () => {
    console.log("Connected to Gemini Live API");

    sendGemini({
      setup: {
        model: `models/${GEMINI_MODEL}`,
        generationConfig: {
          responseModalities: ["AUDIO"],
        },
        systemInstruction: {
          parts: [
            {
              text:
                "You are a polite, concise sales qualification assistant " +
                "for an electric bicycle shop in Bangalore, India. " +
                "Speak clearly in natural English in slang. Ask one question at a " +
                "time and wait for the caller's answer. Never claim a " +
                "purchase is confirmed. This is a test call. First ask " +
                "permission to continue. Then ask about interest in an " +
                "electric bicycle, intended user, main purpose, expected " +
                "purchase timeline, and approximate budget in INR. " +
                "Respect a refusal and end politely. Do not request " +
                "sensitive personal information.",
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
      console.error("Received an invalid Gemini message");
      return;
    }

    if (message.setupComplete) {
      geminiReady = true;
      console.log("Gemini session ready");

      while (pendingAudio.length) {
        forwardAudio(pendingAudio.shift());
      }

      sendGreetingRequest();
    }

    if (message.serverContent?.modelTurn?.parts) {
      for (const part of message.serverContent.modelTurn.parts) {
        const audio = part.inlineData;

        if (
          audio?.data &&
          (!audio.mimeType || audio.mimeType.includes("audio/pcm"))
        ) {
          if (exotelWs.readyState === WebSocket.OPEN && streamSid) {
            exotelWs.send(
              JSON.stringify({
                event: "media",
                stream_sid: streamSid,
                media: {
                  payload: pcm24kTo8k(audio.data),
                },
              })
            );
          }
        }
      }
    }

    if (message.serverContent?.interrupted && streamSid) {
      if (exotelWs.readyState === WebSocket.OPEN) {
        exotelWs.send(JSON.stringify({
          event: "clear",
          stream_sid: streamSid,
        }));
      }
    }

    if (message.error) {
      console.error("Gemini API error:", JSON.stringify(message.error));
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
    if (exotelWs.readyState === WebSocket.OPEN) {
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
        event.start?.stream_sid || event.stream_sid || null;
      console.log("Exotel call stream started");
    }

    if (event.event === "media" && event.media?.payload) {
      if (geminiReady) {
        forwardAudio(event.media.payload);
      } else if (pendingAudio.length < 30) {
        pendingAudio.push(event.media.payload);
      }
    }

    if (event.event === "stop") {
      console.log("Exotel call stream stopped");
      geminiWs.close();
    }
  });

  exotelWs.on("close", () => {
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
