import type { SVGProps } from "react";

// The three sheared bars of the go-faster mark, lifted verbatim from the logo
// generator (go-faster/fs docs/logo/generate.py). The paths are authored around
// an origin of (-130.31, -190), so the group re-centers them on the viewBox.
const BARS = [
  "M594.91,369.52q-1.77,17-1.91,34.45a3.47,3.47,0,0,1-3.43,3.46l-217,2.36a3.49,3.49,0,0,1-3.24-4.86l15.53-36.17a5.23,5.23,0,0,1,3.94-2.68l202.64-.4A3.48,3.48,0,0,1,594.91,369.52Z",
  "M617.07,282.08c-4.08,10.65-8.31,21.44-11.28,32.36-.41,1.51-2.41,2.56-4,2.56h-468a3.54,3.54,0,0,1-2.76-5.72l21.08-31.34a5.41,5.41,0,0,1,3.75-1.76l456.6-.84C614.94,277.33,618,279.79,617.07,282.08Z",
  "M669.69,191.7a.9.9,0,0,1-.2.57,401.62,401.62,0,0,0-23.81,34.09,3.44,3.44,0,0,1-2.93,1.64H282.9c-3,0-4.64-3.92-2.83-6.61l19.44-29a8.55,8.55,0,0,1,4.62-2.37H667.65A2.13,2.13,0,0,1,669.69,191.7Z",
];

/**
 * go-faster logo mark, painted with the logo's own teal → cyan gradient. Shaped
 * as an `Icon` data component so it can be handed to Gravity UI's `logo.icon`.
 */
export function GoFasterMark(props: SVGProps<SVGSVGElement>) {
  return (
    <svg
      viewBox="0 0 539.38 219.79"
      xmlns="http://www.w3.org/2000/svg"
      role="presentation"
      aria-hidden="true"
      {...props}
    >
      <defs>
        <linearGradient id="gofaster-mark" x1="0" y1="1" x2="1" y2="0">
          <stop offset="0" stopColor="#00808f" />
          <stop offset="0.35" stopColor="#01add8" />
          <stop offset="0.7" stopColor="#33c4e6" />
          <stop offset="1" stopColor="#74d9f2" />
        </linearGradient>
      </defs>
      <g fill="url(#gofaster-mark)" transform="translate(-130.31 -190)">
        {BARS.map((d) => (
          <path key={d} d={d} />
        ))}
      </g>
    </svg>
  );
}
