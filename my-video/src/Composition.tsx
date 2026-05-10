import React from "react";
import { AbsoluteFill, OffthreadVideo, staticFile } from "remotion";
import { Captions } from "./Captions";
import pre115Captions from "./captions.json";
import patriarchCaptions from "./captions-patriarch.json";

type Word = { word: string; start: number; end: number };

export const Pre115: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: "#000000" }}>
      <OffthreadVideo src={staticFile("pre-115-trimmed.mp4")} />
      <Captions words={pre115Captions.words as Word[]} />
    </AbsoluteFill>
  );
};

export const Patriarch: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: "#000000" }}>
      <OffthreadVideo src={staticFile("patriarch-trimmed.mp4")} />
      <Captions words={patriarchCaptions.words as Word[]} />
    </AbsoluteFill>
  );
};

export const Pre115_4x5: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: "#000000" }}>
      <OffthreadVideo src={staticFile("pre-115-trimmed-4x5.mp4")} />
      <Captions words={pre115Captions.words as Word[]} />
    </AbsoluteFill>
  );
};

export const Patriarch_4x5: React.FC = () => {
  return (
    <AbsoluteFill style={{ backgroundColor: "#000000" }}>
      <OffthreadVideo src={staticFile("patriarch-trimmed-4x5.mp4")} />
      <Captions words={patriarchCaptions.words as Word[]} />
    </AbsoluteFill>
  );
};
