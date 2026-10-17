const SECONDS_PER_DAY = 86_400n;
const MAX_DURATION_DAYS = 36_500n;

export function durationSecondsFromDays(value: string): bigint {
  const match = /^(\d+)(?:\.(\d+))?$/.exec(value);
  if (!match) {
    throw new RangeError("days must be a positive decimal number no greater than 36,500");
  }

  const whole = BigInt(match[1]!);
  const fraction = (match[2] ?? "").replace(/0+$/, "");
  if (whole > MAX_DURATION_DAYS || (whole === MAX_DURATION_DAYS && fraction.length > 0)) {
    throw new RangeError("days must be a positive decimal number no greater than 36,500");
  }
  if (fraction.length > 18) {
    throw new RangeError("days may contain at most 18 significant decimal places");
  }

  const denominator = 10n ** BigInt(fraction.length);
  const numerator = whole * denominator + BigInt(fraction || "0");
  if (numerator === 0n) {
    throw new RangeError("days must be a positive decimal number no greater than 36,500");
  }

  return (numerator * SECONDS_PER_DAY + denominator - 1n) / denominator;
}
