import { useEffect, useRef } from "react";

// Authentic "digital rain" glyph set: half-width katakana (as in the film's
// mirrored source), western digits, and a few punctuation marks.
const GLYPHS =
  "ｱｲｳｴｵｶｷｸｹｺｻｼｽｾｿﾀﾁﾂﾃﾄﾅﾆﾇﾈﾉﾊﾋﾌﾍﾎﾏﾐﾑﾒﾓﾔﾕﾖﾗﾘﾙﾚﾛﾜﾝ0123456789:.=*+-<>¦｜";

// Matches the mono stack in theme.css.
const FONT_STACK = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace";
const FONT_SIZE = 16;
// Advance one row roughly every FRAME_MS; lower is faster.
const FRAME_MS = 48;

type RGB = [number, number, number];

function glyph(): string {
  return GLYPHS[Math.floor(Math.random() * GLYPHS.length)];
}

// resolveRGB turns any CSS color (hex, rgb(), a var() value) into [r,g,b] by
// letting the canvas normalize it. Falls back to the supplied default.
function resolveRGB(
  ctx: CanvasRenderingContext2D,
  value: string,
  fallback: RGB,
): RGB {
  const v = value.trim();
  if (!v) return fallback;

  ctx.fillStyle = "#000";
  ctx.fillStyle = v;
  const s = ctx.fillStyle;

  if (s.startsWith("#")) {
    const hex =
      s.length === 4
        ? s
            .slice(1)
            .split("")
            .map((c) => c + c)
            .join("")
        : s.slice(1);
    const n = parseInt(hex, 16);
    return [(n >> 16) & 255, (n >> 8) & 255, n & 255];
  }

  const m = s.match(/(\d+)[,\s]+(\d+)[,\s]+(\d+)/);
  return m ? [Number(m[1]), Number(m[2]), Number(m[3])] : fallback;
}

// MatrixRain paints a canvas-based Matrix digital-rain animation sized to its
// parent, using the dashboard theme's own tokens (accent for the trail, the
// strong foreground for the leading glyph, the base background for the fade
// wash). It respects prefers-reduced-motion (renders a single static frame) and
// is purely decorative (aria-hidden).
export default function MatrixRain() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const parent = canvas?.parentElement;
    if (!canvas || !parent) return;

    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const reduceMotion = window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches;

    let width = 0;
    let height = 0;
    let columns = 0;
    let drops: number[] = [];
    let speeds: number[] = [];

    // Theme colors, read from the dashboard's CSS tokens.
    let accent: RGB = [200, 163, 92];
    let head: RGB = [246, 242, 232];
    let base: RGB = [26, 26, 25];

    const readTheme = () => {
      const s = getComputedStyle(canvas);
      accent = resolveRGB(ctx, s.getPropertyValue("--accent"), accent);
      head = resolveRGB(ctx, s.getPropertyValue("--fg-strong"), head);
      base = resolveRGB(ctx, s.getPropertyValue("--bg"), base);
    };

    const setup = () => {
      const dpr = Math.min(window.devicePixelRatio || 1, 2);
      width = parent.clientWidth;
      height = parent.clientHeight;
      canvas.width = Math.max(1, Math.floor(width * dpr));
      canvas.height = Math.max(1, Math.floor(height * dpr));
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.font = `${FONT_SIZE}px ${FONT_STACK}`;
      ctx.textBaseline = "top";

      readTheme();

      columns = Math.max(1, Math.ceil(width / FONT_SIZE));
      // Start columns at staggered negative offsets so the field fills in.
      drops = Array.from({ length: columns }, () => Math.random() * -40);
      speeds = Array.from({ length: columns }, () => 0.55 + Math.random() * 0.9);

      ctx.fillStyle = `rgb(${base[0]}, ${base[1]}, ${base[2]})`;
      ctx.fillRect(0, 0, width, height);
    };

    const step = () => {
      // Translucent base-colored wash leaves a fading trail behind each head.
      ctx.fillStyle = `rgba(${base[0]}, ${base[1]}, ${base[2]}, 0.1)`;
      ctx.fillRect(0, 0, width, height);

      for (let i = 0; i < columns; i++) {
        const x = i * FONT_SIZE;
        const y = drops[i] * FONT_SIZE;

        // Accent-colored body just above the head.
        ctx.shadowBlur = 0;
        ctx.fillStyle = `rgba(${accent[0]}, ${accent[1]}, ${accent[2]}, 0.82)`;
        ctx.fillText(glyph(), x, y - FONT_SIZE);

        // High-contrast leading glyph with an accent glow.
        ctx.fillStyle = `rgb(${head[0]}, ${head[1]}, ${head[2]})`;
        ctx.shadowColor = `rgb(${accent[0]}, ${accent[1]}, ${accent[2]})`;
        ctx.shadowBlur = 8;
        ctx.fillText(glyph(), x, y);

        if (y > height && Math.random() > 0.975) {
          drops[i] = Math.random() * -16;
          speeds[i] = 0.55 + Math.random() * 0.9;
        } else {
          drops[i] += speeds[i];
        }
      }

      ctx.shadowBlur = 0;
    };

    setup();

    const ro = new ResizeObserver(() => setup());
    ro.observe(parent);

    if (reduceMotion) {
      for (let pass = 0; pass < 24; pass++) step();

      return () => ro.disconnect();
    }

    let raf = 0;
    let last = 0;
    const loop = (t: number) => {
      raf = requestAnimationFrame(loop);
      if (t - last < FRAME_MS) return;
      last = t;
      step();
    };
    raf = requestAnimationFrame(loop);

    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
    };
  }, []);

  return <canvas ref={canvasRef} className="matrix-canvas" aria-hidden="true" />;
}
