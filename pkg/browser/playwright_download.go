package browser

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bogdanovich/mintclaw/pkg/config"
)

const (
	playwrightDownloadMarker        = "MINTCLAW_DL_V1"
	playwrightDownloadChunkBytes    = 128 * 1024
	playwrightDownloadEnvelopeBytes = 2 * 1024 * 1024
	playwrightDownloadConfigName    = ".mintclaw-download-boundary.json"
)

func playwrightCaptureDownloadCode(target string, maximumBytes int64) string {
	// This fixed template is sent only through the worker's private MCP client.
	// The unsafe driver tool is never registered in an agent-facing registry;
	// the interpolated ref and byte limit have already passed typed validation.
	return fmt.Sprintf(`async (page) => {
  const maximumBytes = %d;
  const cdp = await page.context().newCDPSession(page);
  const locator = page.locator("aria-ref=" + %q);
  if (await locator.count() !== 1) {
    await cdp.detach();
    return "MINTCLAW_DL_V1|error|stale_target";
  }
  const expectedURL = await locator.evaluate(element => element instanceof HTMLAnchorElement ? element.href : "");
  if (!/^https?:\/\//i.test(expectedURL)) {
    await cdp.detach();
    return "MINTCLAW_DL_V1|error|unsupported_target";
  }
  const frameTree = await cdp.send("Page.getFrameTree");
  const mainFrameID = String(frameTree.frameTree && frameTree.frameTree.frame && frameTree.frameTree.frame.id || "");
  if (!mainFrameID) {
    await cdp.detach();
    return "MINTCLAW_DL_V1|error|capture_failed";
  }
  const state = {
    cdp, expectedURL, mainFrameID, boundRequestID: "", attachmentCount: 0, clickStarted: false,
    status: "pending", stream: "", disposition: "", contentType: "", multiple: false
  };
  cdp.on("Network.requestWillBeSent", event => {
    const request = event.request || {};
    const directClickNavigation = state.clickStarted &&
      event.frameId === state.mainFrameID && event.type === "Document" &&
      request.url === state.expectedURL && request.method === "GET" && !event.redirectResponse &&
      event.initiator && event.initiator.type === "other";
    if (!directClickNavigation) return;
    if (state.boundRequestID && state.boundRequestID !== event.requestId) {
      state.multiple = true;
      return;
    }
    state.boundRequestID = event.requestId;
  });
  cdp.on("Fetch.requestPaused", async event => {
    try {
      if (!event.responseStatusCode) {
        await cdp.send("Fetch.continueRequest", { requestId: event.requestId });
        return;
      }
      let disposition = "";
      let contentType = "";
      for (const header of event.responseHeaders || []) {
        const name = String(header.name || "").toLowerCase();
        if (name === "content-disposition") disposition = String(header.value || "");
        if (name === "content-type") contentType = String(header.value || "");
      }
      const attachment = /^\s*attachment(?:\s*;|$)/i.test(disposition);
      if (!attachment) {
        await cdp.send("Fetch.continueResponse", { requestId: event.requestId });
        return;
      }
      state.attachmentCount++;
      const directDocument = state.boundRequestID !== "" &&
        event.networkId === state.boundRequestID && event.frameId === state.mainFrameID &&
        event.request.url === state.expectedURL &&
        event.request.method === "GET" && event.resourceType === "Document" &&
        event.responseStatusCode >= 200 && event.responseStatusCode < 300 &&
        !event.redirectedRequestId;
      if (!directDocument || state.attachmentCount !== 1) {
        state.multiple = true;
        await cdp.send("Fetch.continueResponse", { requestId: event.requestId });
        return;
      }
      if (state.status !== "pending") {
        state.multiple = true;
        await cdp.send("Fetch.continueResponse", { requestId: event.requestId });
        return;
      }
      state.status = "claiming";
      const body = await cdp.send("Fetch.takeResponseBodyAsStream", { requestId: event.requestId });
      state.stream = body.stream;
      state.disposition = disposition;
      state.contentType = contentType;
      state.status = "ready";
    } catch (_) {
      state.status = "error";
    }
  });
  await cdp.send("Network.enable");
  await cdp.send("Fetch.enable", { patterns: [{ urlPattern: "*", requestStage: "Response" }] });
  let clickFinished = false;
  let clickFinishedAt = 0;
  let clickFailed = false;
  state.clickStarted = true;
  const click = locator.click().then(
    () => { clickFinished = true; clickFinishedAt = Date.now(); },
    () => { clickFinished = true; clickFinishedAt = Date.now(); clickFailed = true; }
  );
  for (let attempt = 0; attempt < 400 &&
    (state.status === "pending" || state.status === "claiming"); attempt++) {
    if (state.multiple) break;
    if (clickFinished && state.status === "pending" && Date.now() - clickFinishedAt >= 250) break;
    await page.waitForTimeout(25);
  }
  const finish = async () => {
    try { if (state.stream) await cdp.send("IO.close", { handle: state.stream }); } catch (_) {}
    try { await cdp.send("Fetch.disable"); } catch (_) {}
    try { await page.close({ runBeforeUnload: false }); } catch (_) {}
    try { await cdp.detach(); } catch (_) {}
  };
  if (state.status !== "ready") {
    await finish();
    return "MINTCLAW_DL_V1|error|" + (state.multiple ? "multiple" : state.status === "error" ? "capture_failed" :
      (clickFailed ? "click_failed" : "no_attachment"));
  }
  const parts = [];
  let total = 0;
  const encodeUTF8Base64 = value => {
    const bytes = [];
    for (const character of value) {
      const point = character.codePointAt(0);
      if (point <= 0x7f) bytes.push(point);
      else if (point <= 0x7ff) bytes.push(0xc0 | point >> 6, 0x80 | point & 0x3f);
      else if (point <= 0xffff) bytes.push(
        0xe0 | point >> 12, 0x80 | point >> 6 & 0x3f, 0x80 | point & 0x3f
      );
      else bytes.push(
        0xf0 | point >> 18, 0x80 | point >> 12 & 0x3f,
        0x80 | point >> 6 & 0x3f, 0x80 | point & 0x3f
      );
    }
    const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let encoded = "";
    for (let index = 0; index < bytes.length; index += 3) {
      const first = bytes[index];
      const second = index + 1 < bytes.length ? bytes[index + 1] : 0;
      const third = index + 2 < bytes.length ? bytes[index + 2] : 0;
      encoded += alphabet[first >> 2] + alphabet[(first & 3) << 4 | second >> 4] +
        (index + 1 < bytes.length ? alphabet[(second & 15) << 2 | third >> 6] : "=") +
        (index + 2 < bytes.length ? alphabet[third & 63] : "=");
    }
    return { bytes: bytes.length, encoded };
  };
  try {
    for (;;) {
      const part = await cdp.send("IO.read", { handle: state.stream, size: %d });
      const data = String(part.data || "");
      let bytes = 0;
      let encoded = "";
      if (part.base64Encoded) {
        bytes = Math.floor(data.length * 3 / 4);
        if (data.endsWith("==")) bytes -= 2;
        else if (data.endsWith("=")) bytes -= 1;
        encoded = data;
      } else {
        const converted = encodeUTF8Base64(data);
        bytes = converted.bytes;
        encoded = converted.encoded;
      }
      if (total + bytes > maximumBytes) {
        await finish();
        return "MINTCLAW_DL_V1|error|oversize";
      }
      total += bytes;
      if (encoded) parts.push(encoded);
      if (part.eof) break;
    }
  } catch (_) {
    await finish();
    return "MINTCLAW_DL_V1|error|read_failed";
  }
  await finish();
  if (!clickFinished || clickFailed) return "MINTCLAW_DL_V1|error|click_failed";
  if (state.multiple) return "MINTCLAW_DL_V1|error|multiple";
  return "MINTCLAW_DL_V1|complete|" + encodeURIComponent(state.disposition) + "|" +
    encodeURIComponent(state.contentType) + "|" + total + "|" + parts.join(",");
}`, maximumBytes, target, playwrightDownloadChunkBytes)
}

// PlaywrightDownloadAvailable reports whether the configured private driver
// can deny native disk downloads and expose the scoped Chromium stream boundary.
func PlaywrightDownloadAvailable(root *config.Config) bool {
	if root == nil || !playwrightDownloadBoundaryAvailable() || !root.Tools.Browser.Enabled {
		return false
	}
	target, ok := root.Tools.Browser.Targets[config.BrowserDefaultTarget]
	if !ok || !target.Enabled || target.Driver != config.BrowserDriverPlaywrightMCP {
		return false
	}
	server, ok := root.Tools.MCP.Servers[target.DriverServer]
	if !ok {
		return false
	}
	browserName := "chromium"
	for index := 0; index < len(server.Args); index++ {
		argument := server.Args[index]
		if argument == "--browser" && index+1 < len(server.Args) {
			browserName = strings.ToLower(strings.TrimSpace(server.Args[index+1]))
			index++
			continue
		}
		if strings.HasPrefix(argument, "--browser=") {
			browserName = strings.ToLower(strings.TrimSpace(strings.TrimPrefix(argument, "--browser=")))
		}
	}
	switch browserName {
	case "chromium", "chrome", "msedge":
		return true
	default:
		return false
	}
}

func configurePlaywrightDownloadBoundary(
	server config.MCPServerConfig,
	outputDir string,
) (config.MCPServerConfig, error) {
	path := filepath.Join(outputDir, playwrightDownloadConfigName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return config.MCPServerConfig{}, err
	}
	content := []byte("{\"browser\":{\"contextOptions\":{\"acceptDownloads\":false}}}\n")
	if _, err = file.Write(content); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return config.MCPServerConfig{}, err
	}
	server.Args = append(server.Args, "--config", path)
	return server, nil
}

func (worker *playwrightWorker) captureDownload(
	ctx context.Context,
	action DriverAction,
	maximumBytes int64,
) (DriverDownload, error) {
	file, err := os.CreateTemp(worker.outputDir, "captured-download-*.bin")
	if err != nil {
		return DriverDownload{}, ErrWorkerUnavailable
	}
	path := file.Name()
	keep := false
	defer func() {
		_ = file.Close()
		if !keep {
			_ = os.Remove(path)
		}
	}()

	denialsBefore := uint64(0)
	if worker.networkProxy != nil {
		denialsBefore = worker.networkProxy.Denials()
	}
	fields, err := worker.downloadControl(
		ctx,
		playwrightCaptureDownloadCode(action.Target, maximumBytes),
		maximumBytes,
	)
	if err != nil {
		return DriverDownload{}, err
	}
	if worker.networkProxy != nil && worker.networkProxy.Denials() > denialsBefore {
		return DriverDownload{}, ErrDenied
	}
	if len(fields) != 6 || fields[1] != "complete" {
		return DriverDownload{}, ErrDriverIncompatible
	}
	disposition, dispositionErr := url.QueryUnescape(fields[2])
	contentType, contentTypeErr := url.QueryUnescape(fields[3])
	declaredBytes, sizeErr := strconv.ParseInt(fields[4], 10, 64)
	if dispositionErr != nil || contentTypeErr != nil || sizeErr != nil ||
		declaredBytes < 1 || declaredBytes > maximumBytes {
		return DriverDownload{}, ErrDriverIncompatible
	}

	hasher := sha256.New()
	written := int64(0)
	for _, encoded := range strings.Split(fields[5], ",") {
		if encoded == "" {
			continue
		}
		chunk, decodeErr := base64.StdEncoding.DecodeString(encoded)
		if decodeErr != nil || written+int64(len(chunk)) > maximumBytes {
			return DriverDownload{}, ErrDriverIncompatible
		}
		count, writeErr := io.MultiWriter(file, hasher).Write(chunk)
		if writeErr != nil || count != len(chunk) {
			return DriverDownload{}, ErrWorkerUnavailable
		}
		written += int64(count)
	}

	if written != declaredBytes || file.Sync() != nil || file.Close() != nil {
		return DriverDownload{}, ErrDriverIncompatible
	}
	keep = true
	return DriverDownload{
		Path: path, Filename: playwrightDownloadFilename(disposition),
		ContentType: playwrightDownloadContentType(contentType),
		SHA256:      hex.EncodeToString(hasher.Sum(nil)), Size: written,
	}, nil
}

func (worker *playwrightWorker) downloadControl(
	ctx context.Context,
	code string,
	maximumBytes int64,
) ([]string, error) {
	// The private result may contain bounded encoded bytes, but it never enters
	// model context or the generic MCP tool surface.
	result, err := worker.client.CallTool(ctx, "browser_run_code_unsafe", map[string]any{"code": code})
	if err != nil || result == nil {
		worker.lost = true
		return nil, ErrWorkerUnavailable
	}
	responseLimit := int(((maximumBytes+2)/3)*4) + playwrightDownloadEnvelopeBytes
	text, err := boundedPlaywrightText(result, responseLimit)
	if err != nil {
		return nil, fmt.Errorf("download control response: %w", ErrDriverIncompatible)
	}
	if result.IsError {
		return nil, fmt.Errorf("download control rejected: %w", ErrDriverIncompatible)
	}
	index := strings.Index(text, playwrightDownloadMarker+"|")
	if index < 0 {
		return nil, fmt.Errorf("download control marker: %w", ErrDriverIncompatible)
	}
	line := text[index:]
	if end := strings.IndexByte(line, '\n'); end >= 0 {
		line = line[:end]
	}
	line = strings.TrimRight(line, "\r\"' ")
	fields := strings.SplitN(line, "|", 6)
	if len(fields) < 2 || fields[0] != playwrightDownloadMarker {
		return nil, fmt.Errorf("download control fields: %w", ErrDriverIncompatible)
	}
	return fields, nil
}

func playwrightDownloadFilename(disposition string) string {
	_, parameters, err := mime.ParseMediaType(disposition)
	if err == nil {
		name := strings.TrimSpace(filepath.Base(parameters["filename"]))
		if name != "" && name != "." && name != string(filepath.Separator) {
			return name
		}
	}
	return "download.bin"
}

func playwrightDownloadContentType(value string) string {
	mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(value))
	if err == nil && mediaType != "" {
		return mediaType
	}
	return "application/octet-stream"
}
