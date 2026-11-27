// d3-flame-graph ships no types. This declares the part of its API the flame
// graph tab uses (src/tree/flame-view.tsx), as of version 5.0.0.

declare module 'd3-flame-graph' {
  import type { Selection } from 'd3-selection';

  /** A d3-hierarchy node as the library hands it to callbacks, after its partition layout. */
  export interface FlameGraphNode<Datum> {
    /** The input datum. The library adds `fade` (an ancestor of the zoomed frame) and `hide` (off the zoom path). */
    data: Datum & { fade?: boolean; hide?: boolean };
    value: number;
    depth: number;
    x0: number;
    x1: number;
    parent: FlameGraphNode<Datum> | null;
    children?: FlameGraphNode<Datum>[];
    descendants(): FlameGraphNode<Datum>[];
  }

  export interface FlameGraph<Datum> {
    /** Draws into each element of the selection, whose datum is the input tree. */
    (selection: Selection<any, any, any, any>): void;
    width(px: number): this;
    cellHeight(px: number): this;
    minHeight(px: number): this;
    minFrameSize(px: number): this;
    transitionDuration(ms: number): this;
    inverted(on: boolean): this;
    sort(on: boolean): this;
    selfValue(on: boolean): this;
    resetHeightOnZoom(on: boolean): this;
    getName(fn: (d: FlameGraphNode<Datum>) => string): this;
    setLabelHandler(fn: (d: FlameGraphNode<Datum>) => string): this;
    setColorMapper(fn: (d: FlameGraphNode<Datum>, originalColor: string) => string): this;
    /** Receives the label of the hovered frame, and null when the pointer leaves it. */
    setDetailsHandler(fn: (text: string | null) => void): this;
    /** Called after the library zooms on a clicked frame, also from zoomTo and resetZoom. */
    onClick(fn: (d: FlameGraphNode<Datum>) => void): this;
    update(data?: Datum): this;
    zoomTo(d: FlameGraphNode<Datum>): void;
    resetZoom(): void;
    destroy(): this;
  }

  export default function flamegraph<Datum = unknown>(): FlameGraph<Datum>;
}
