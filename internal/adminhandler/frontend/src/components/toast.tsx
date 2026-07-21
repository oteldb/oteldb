import { createContext, useCallback, useContext, useRef, useState, type ReactNode } from "react";

type ToastFn = (message: string, isErr?: boolean) => void;
const ToastCtx = createContext<ToastFn>(() => {});

export function useToast(): ToastFn {
  return useContext(ToastCtx);
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [msg, setMsg] = useState("");
  const [err, setErr] = useState(false);
  const [show, setShow] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const toast = useCallback<ToastFn>((message, isErr = false) => {
    setMsg(message);
    setErr(isErr);
    setShow(true);
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => setShow(false), 2800);
  }, []);

  return (
    <ToastCtx.Provider value={toast}>
      {children}
      <div id="toast" className={[show ? "show" : "", err ? "err" : ""].join(" ").trim()}>
        {msg}
      </div>
    </ToastCtx.Provider>
  );
}
