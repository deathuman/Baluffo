// The development entry: repo pages boot through the runtime dispatcher, which
// serves both plain browser mode and `?desktop=1`. The shipped builds compose the
// page body (`page-entry.js`) with their own runtime's client instead, so neither
// packaged page carries the other runtime's code.
import "../shared/local-data/app-client.js";
import "./page-entry.js";
