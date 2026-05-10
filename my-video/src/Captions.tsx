import React from "react";
import { useCurrentFrame, useVideoConfig, interpolate, Easing } from "remotion";

type Word = { word: string; start: number; end: number };

// Group consecutive words into short phrases (2-3 words each).
// Breaks earlier on punctuation so phrasing reads naturally.
function buildPhrases(words: Word[]): { text: string; start: number; end: number }[] {
  const phrases: { text: string; start: number; end: number }[] = [];
  const MAX_WORDS = 3;
  let current: Word[] = [];

  const flush = () => {
    if (current.length === 0) return;
    phrases.push({
      text: current.map((w) => w.word).join(" "),
      start: current[0].start,
      end: current[current.length - 1].end,
    });
    current = [];
  };

  for (const w of words) {
    current.push(w);
    const endsWithBreak = /[.,?!]$/.test(w.word);
    if (current.length >= MAX_WORDS || endsWithBreak) flush();
  }
  flush();

  return phrases;
}

export const Captions: React.FC<{ words: Word[] }> = ({ words }) => {
  const frame = useCurrentFrame();
  const { fps, height } = useVideoConfig();
  const t = frame / fps;

  const phrases = React.useMemo(() => buildPhrases(words), [words]);

  // Find the active phrase based on current time
  const active = phrases.find((p) => t >= p.start && t <= p.end + 0.1);
  if (!active) return null;

  // Pop animation: scale + fade in over the first ~120ms of the phrase
  const popProgress = interpolate(t - active.start, [0, 0.12], [0, 1], {
    extrapolateLeft: "clamp",
    extrapolateRight: "clamp",
    easing: Easing.out(Easing.back(1.5)),
  });

  return (
    <div
      style={{
        position: "absolute",
        left: 0,
        right: 0,
        bottom: Math.round(height * 0.22),
        display: "flex",
        justifyContent: "center",
        pointerEvents: "none",
      }}
    >
      <div
        style={{
          transform: `scale(${popProgress})`,
          opacity: popProgress,
          maxWidth: "85%",
          textAlign: "center",
          fontFamily:
            "Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif",
          fontWeight: 900,
          fontSize: Math.round(height * 0.045),
          lineHeight: 1.15,
          letterSpacing: "-0.02em",
          color: "#ffffff",
          textTransform: "uppercase",
          textShadow: [
            "0 0 3px rgba(0,0,0,1)",
            "0 0 3px rgba(0,0,0,1)",
            "0 0 6px rgba(0,0,0,0.9)",
            "0 4px 12px rgba(0,0,0,0.85)",
          ].join(", "),
        }}
      >
        {active.text}
      </div>
    </div>
  );
};
