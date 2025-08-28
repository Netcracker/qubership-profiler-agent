// Streaming FNV-1a 128-bit over UTF-8, dependency-free.

export class Fnv1a128 {
    // See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function#FNV_hash_parameters

    static OFFSET_BASIS = 0x6c62272e07bb014262b821756295c58dn;
    static PRIME        = 0x0000000001000000000000000000013bn;
    static MASK         = (1n << 128n) - 1n; // modulo 2^128

    constructor() {
        this._h = Fnv1a128.OFFSET_BASIS;
        this._enc = typeof TextEncoder !== 'undefined' ? new TextEncoder() : null;
    }

    /** Reset to initial state (chainable). */
    reset() {
        this._h = Fnv1a128.OFFSET_BASIS;
        return this;
    }

    /**
     * Update the hash with next chunk.
     * Accepts a string (UTF-8 encoded) or a Uint8Array / ArrayBuffer / typed array.
     */
    update(input) {
        let bytes;

        if (typeof input === 'string') {
            if (!this._enc) {
                throw new Error(
                    'TextEncoder is not available. Pass Uint8Array/ArrayBuffer instead, ' +
                    'or polyfill TextEncoder (Node >=18 has it globally).'
                );
            }
            bytes = this._enc.encode(input);
        } else if (input instanceof Uint8Array) {
            bytes = input;
        } else if (ArrayBuffer.isView(input) && input.buffer) { // typed arrays
            bytes = new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
        } else if (input instanceof ArrayBuffer) {
            bytes = new Uint8Array(input);
        } else {
            throw new TypeError('update() expects string, Uint8Array, ArrayBuffer, or a typed array.');
        }

        let h = this._h;
        const prime = Fnv1a128.PRIME;
        const mask  = Fnv1a128.MASK;

        for (let i = 0; i < bytes.length; i++) {
            h ^= BigInt(bytes[i]);
            h = (h * prime) & mask; // keep to 128 bits
        }
        this._h = h;
        return this; // allow chaining
    }

    /**
     * Finalize and return a 32-char lowercase hex string (zero-padded).
     * (Does not reset the state; call .reset() if you want to reuse.)
     */
    finalize() {
        return this._h.toString(16).padStart(32, '0');
    }

    /** Get the internal BigInt value (unsigned 128-bit). */
    value() {
        return this._h;
    }
}

/** One-shot helper: returns hex string for a full string input. */
export function fnv1a128Hex(str) {
    return new Fnv1a128().update(str).finalize();
}

export default Fnv1a128;
