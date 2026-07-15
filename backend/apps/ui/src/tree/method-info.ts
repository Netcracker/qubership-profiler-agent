// Parses the agent's dictionary word into display parts. The wire format is
//   <returnType> <package.Class.method>(<args>) (<File>.java:<line>) [<jarPath>/<jarName>]
// and needs no dedicated fields (08 §7) — this is a port of
// backend/libs/parser/dictionary/line_parser.go, minus its debug prints.

export interface MethodInfo {
  original: string;
  /** Full `package.Class` (CGLib noise stripped). */
  className: string;
  /** Abbreviated class, e.g. `c.n.c.s.StreamFacadeCassandra`. */
  shortClassName: string;
  /** Abbreviated signature, e.g. `void c.n.Class.setBeanFactory(o.s.b.f.BeanFactory)`. */
  signature: string;
  /** `<Class>.java`, or `<generated>` for synthesised classes. */
  fileName: string;
  lineNumber: number;
  isGenerated: boolean;
  jarName: string;
  jarPath: string;
}

const CGLIB_ID = /\$\$[a-z0-9]{3,8}/g;
const SIGNATURE = /^(([^(]+)\.[^(.]+)\(([^(]*)\)$/;

function shorten(s: string, keepRight: number): string {
  const cleaned = s.replaceAll('java.lang.', '').replaceAll('java.util.', '');
  const parts = cleaned.split('.');
  for (let i = 0; i < parts.length - keepRight; i++) {
    parts[i] = parts[i]!.slice(0, 1);
  }
  return parts.join('.');
}

const shortClass = (s: string): string => shorten(s, 1);

function parseJar(s: string): { jarPath: string; jarName: string } | null {
  if (!s.startsWith('[') || !s.endsWith(']')) return null;
  const body = s.slice(1, -1);
  if (body.includes('.jar!/')) {
    // Spring boot fat jar: 'escui.jar!/BOOT-INF/classes'.
    const [jar, path] = body.split('!');
    return { jarPath: path ?? '', jarName: jar ?? '' };
  }
  const parts = body.split('/');
  const last = parts[parts.length - 1]!;
  if (last.includes('jar')) {
    return { jarPath: parts.slice(0, -1).join('/'), jarName: last };
  }
  return { jarPath: body, jarName: '' };
}

function parseLine(s: string): { isGenerated: boolean; fileName: string; lineNumber: number } | null {
  if (s.includes('<generated>')) return { isGenerated: true, fileName: '<generated>', lineNumber: 0 };
  if (s.startsWith('(') && s.endsWith(')')) {
    const parts = s.slice(1, -1).split(':');
    if (parts.length === 2) {
      const line = Number(parts[1]);
      if (Number.isInteger(line)) return { isGenerated: false, fileName: parts[0]!, lineNumber: line };
    }
  }
  return null;
}

/**
 * Best-effort parse: a word that does not follow the method format comes back
 * with only `original` set and everything else empty, never an exception.
 */
export function parseMethod(original: string): MethodInfo {
  const info: MethodInfo = {
    original,
    className: '',
    shortClassName: '',
    signature: original,
    fileName: '',
    lineNumber: 0,
    isGenerated: false,
    jarName: '',
    jarPath: '',
  };
  let parts = original.split(' ');
  if (parts.length <= 1) return info;

  const jar = parseJar(parts[parts.length - 1]!);
  if (jar !== null) {
    info.jarPath = jar.jarPath;
    info.jarName = jar.jarName;
    parts = parts.slice(0, -1);
  }
  const line = parseLine(parts[parts.length - 1]!);
  if (line !== null) {
    info.isGenerated = line.isGenerated;
    info.fileName = line.fileName;
    info.lineNumber = line.lineNumber;
    parts = parts.slice(0, -1);
  }
  if (parts.length !== 2) return info;

  const returnType = shortClass(parts[0]!);
  const methodRaw = parts[1]!
    .replaceAll('$$EnhancerBySpringCGLIB', '')
    .replaceAll('$$FastClassBySpringCGLIB', '')
    .replaceAll('$STATICHOOK', '')
    .replace(CGLIB_ID, '');

  const match = SIGNATURE.exec(methodRaw);
  if (match === null) return info;
  const [, qualifiedMethod, className, argsRaw] = match;

  info.className = className!;
  info.shortClassName = shortClass(className!);
  const args = argsRaw!
    .split(',')
    .map((a) => (a === '' ? a : shortClass(a)))
    .join(',');
  info.signature = `${returnType} ${shorten(qualifiedMethod!, 2)}(${args})`;
  return info;
}
