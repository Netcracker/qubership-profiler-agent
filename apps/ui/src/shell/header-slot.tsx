import { createContext, useContext } from 'react';
import type { ReactNode } from 'react';
import { createPortal } from 'react-dom';

/**
 * The header element that takes a screen's controls. `undefined` means no
 * shell is mounted; `null` means the shell has not attached the element yet.
 */
export const HeaderSlotContext = createContext<HTMLElement | null | undefined>(undefined);

/**
 * Renders a screen's controls in the shell header. The screen keeps the state
 * and the handlers; only the markup moves. Without a shell, as in a page test,
 * the controls render in place.
 */
export function HeaderSlot({ children }: { children: ReactNode }) {
  const target = useContext(HeaderSlotContext);
  if (target === undefined) return <>{children}</>;
  return target === null ? null : createPortal(children, target);
}
