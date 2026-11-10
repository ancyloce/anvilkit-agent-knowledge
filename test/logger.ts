// The logger of the unit tests: every line discarded.
import type { Logger } from "../src/log.js";

export const silentLogger: Logger = { info: () => {}, warn: () => {}, error: () => {} };
