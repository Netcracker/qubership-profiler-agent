import { cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { usePermalinkHotkey } from './use-permalink-hotkey';

afterEach(cleanup);

function Harness({ enabled, onFreeze }: { enabled: boolean; onFreeze: () => void }) {
  usePermalinkHotkey(enabled, onFreeze);
  return <input aria-label="field" />;
}

describe('usePermalinkHotkey', () => {
  it('fires on a bare "y"', () => {
    const onFreeze = vi.fn();
    render(<Harness enabled onFreeze={onFreeze} />);
    fireEvent.keyDown(document.body, { key: 'y' });
    expect(onFreeze).toHaveBeenCalledTimes(1);
  });

  it('ignores "y" typed into a field', () => {
    const onFreeze = vi.fn();
    render(<Harness enabled onFreeze={onFreeze} />);
    fireEvent.keyDown(screen.getByLabelText('field'), { key: 'y' });
    expect(onFreeze).not.toHaveBeenCalled();
  });

  it('ignores "y" with a modifier', () => {
    const onFreeze = vi.fn();
    render(<Harness enabled onFreeze={onFreeze} />);
    fireEvent.keyDown(document.body, { key: 'y', metaKey: true });
    expect(onFreeze).not.toHaveBeenCalled();
  });

  it('does nothing when disabled', () => {
    const onFreeze = vi.fn();
    render(<Harness enabled={false} onFreeze={onFreeze} />);
    fireEvent.keyDown(document.body, { key: 'y' });
    expect(onFreeze).not.toHaveBeenCalled();
  });
});
