/**
 * An order-preserving view of a top-level JSON object, for the hash read-modify-write fallback.
 *
 * `JSON.parse` would reorder integer-like keys ("1", "2" sort before others in a JS object) and
 * round big integers to doubles, so the fallback would rewrite documents differently from the
 * JSON1 path. This keeps every field's exact JSON text and the original key order instead.
 */

export type RootType = "object" | "array" | "text" | "true" | "false" | "null" | "number";

export class JsonObject {
  readonly fields: [string, string][] = [];

  private index(key: string): number {
    return this.fields.findIndex(([k]) => k === key);
  }

  has(key: string): boolean {
    return this.index(key) >= 0;
  }

  get(key: string): string | undefined {
    const i = this.index(key);
    return i >= 0 ? this.fields[i]![1] : undefined;
  }

  set(key: string, rawJson: string): void {
    const i = this.index(key);
    if (i >= 0) this.fields[i]![1] = rawJson;
    else this.fields.push([key, rawJson]);
  }

  delete(key: string): boolean {
    const i = this.index(key);
    if (i < 0) return false;
    this.fields.splice(i, 1);
    return true;
  }

  toJSON(): string {
    return `{${this.fields.map(([k, v]) => `${JSON.stringify(k)}:${compactJson(v)}`).join(",")}}`;
  }
}

/** Re-emits JSON text without insignificant whitespace, leaving strings untouched. */
function compactJson(text: string): string {
  let out = "";
  let inString = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i]!;
    if (inString) {
      out += ch;
      if (ch === "\\") out += text[++i] ?? "";
      else if (ch === '"') inString = false;
    } else if (ch === '"') {
      inString = true;
      out += ch;
    } else if (!/\s/.test(ch)) {
      out += ch;
    }
  }
  return out;
}

function rootTypeOf(trimmed: string): RootType {
  switch (trimmed[0]) {
    case "{":
      return "object";
    case "[":
      return "array";
    case '"':
      return "text";
    case "t":
      return "true";
    case "f":
      return "false";
    case "n":
      return "null";
    default:
      return "number";
  }
}

/**
 * Parses JSON text. Returns `{ object }` for an object root, `{ root }` for any other valid root,
 * and throws for malformed JSON.
 */
export function parseJsonObject(text: string): { object?: JsonObject; root: RootType } {
  JSON.parse(text); // validates; throws SyntaxError on malformed input
  const trimmed = text.trim();
  const root = rootTypeOf(trimmed);
  if (root !== "object") return { root };

  const obj = new JsonObject();
  let i = 1;
  const skipWs = (): void => {
    while (i < trimmed.length && /\s/.test(trimmed[i]!)) i++;
  };
  const readString = (): string => {
    const start = i;
    i++; // opening quote
    while (trimmed[i] !== '"') {
      if (trimmed[i] === "\\") i++;
      i++;
    }
    i++; // closing quote
    return trimmed.slice(start, i);
  };
  const readValue = (): string => {
    const start = i;
    let depth = 0;
    while (i < trimmed.length) {
      const ch = trimmed[i]!;
      if (ch === '"') {
        readString();
        if (depth === 0) break;
        continue;
      }
      if (ch === "{" || ch === "[") depth++;
      else if (ch === "}" || ch === "]") {
        if (depth === 0) break;
        depth--;
        if (depth === 0) {
          i++;
          break;
        }
      } else if (ch === "," && depth === 0) break;
      i++;
    }
    return trimmed.slice(start, i).trim();
  };

  skipWs();
  while (trimmed[i] !== "}") {
    const key = JSON.parse(readString()) as string;
    skipWs();
    i++; // colon
    skipWs();
    obj.set(key, readValue());
    skipWs();
    if (trimmed[i] === ",") {
      i++;
      skipWs();
    }
  }
  return { object: obj, root };
}
