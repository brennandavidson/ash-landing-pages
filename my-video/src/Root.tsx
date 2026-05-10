import "./index.css";
import { Composition } from "remotion";
import { Pre115, Patriarch, Pre115_4x5, Patriarch_4x5 } from "./Composition";

const FPS = 30;

const PRE115_DURATION_FRAMES = Math.round(29.2 * FPS); // 876
const PATRIARCH_DURATION_FRAMES = Math.round(18.15 * FPS); // 545

export const RemotionRoot: React.FC = () => {
  return (
    <>
      {/* 9:16 — Reels, Stories */}
      <Composition
        id="Pre115"
        component={Pre115}
        durationInFrames={PRE115_DURATION_FRAMES}
        fps={FPS}
        width={720}
        height={1280}
      />
      <Composition
        id="Patriarch"
        component={Patriarch}
        durationInFrames={PATRIARCH_DURATION_FRAMES}
        fps={FPS}
        width={540}
        height={960}
      />
      {/* 4:5 — Mobile/IG Feed, Explore, Profile */}
      <Composition
        id="Pre115-4x5"
        component={Pre115_4x5}
        durationInFrames={PRE115_DURATION_FRAMES}
        fps={FPS}
        width={720}
        height={900}
      />
      <Composition
        id="Patriarch-4x5"
        component={Patriarch_4x5}
        durationInFrames={PATRIARCH_DURATION_FRAMES}
        fps={FPS}
        width={540}
        height={675}
      />
    </>
  );
};
