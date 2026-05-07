/**
 * Generate an RFC 9562 UUIDv7 string.
 *
 * The Iceberg REST spec defines `Idempotency-Key` as a UUID format,
 * with the example payloads showing UUIDv7 (timestamp-prefixed). UUIDv7
 * is preferred because keys are roughly time-orderable, which makes
 * server-side dedup windows simpler to reason about.
 *
 * Layout (RFC 9562 §5.7):
 *   48-bit unix-ms timestamp | 4-bit version (0111) | 12-bit rand_a
 *   2-bit variant (10) | 62-bit rand_b
 */
export function generateIdempotencyKey(): string {
  const ms = BigInt(Date.now())
  const bytes = new Uint8Array(16)

  // 48 bits of unix-ms timestamp (big-endian)
  bytes[0] = Number((ms >> 40n) & 0xffn)
  bytes[1] = Number((ms >> 32n) & 0xffn)
  bytes[2] = Number((ms >> 24n) & 0xffn)
  bytes[3] = Number((ms >> 16n) & 0xffn)
  bytes[4] = Number((ms >> 8n) & 0xffn)
  bytes[5] = Number(ms & 0xffn)

  // 10 bytes of randomness
  const rand = new Uint8Array(10)
  cryptoRandom(rand)
  bytes.set(rand, 6)

  // Version (0111) in the high nibble of byte 6
  bytes[6] = (bytes[6] & 0x0f) | 0x70
  // Variant (10) in the high two bits of byte 8
  bytes[8] = (bytes[8] & 0x3f) | 0x80

  return formatUUID(bytes)
}

function formatUUID(b: Uint8Array): string {
  const hex: string[] = []
  for (let i = 0; i < 16; i++) {
    hex.push(b[i].toString(16).padStart(2, '0'))
  }
  return `${hex.slice(0, 4).join('')}-${hex.slice(4, 6).join('')}-${hex.slice(6, 8).join('')}-${hex.slice(8, 10).join('')}-${hex.slice(10, 16).join('')}`
}

function cryptoRandom(buf: Uint8Array): void {
  const c =
    typeof globalThis.crypto !== 'undefined' &&
    typeof globalThis.crypto.getRandomValues === 'function'
      ? globalThis.crypto
      : undefined
  if (c) {
    c.getRandomValues(buf)
    return
  }
  // Fallback: should not happen in supported environments (Node 20+, modern browsers).
  for (let i = 0; i < buf.length; i++) {
    buf[i] = Math.floor(Math.random() * 256)
  }
}
