// Auto-refresh: fetches the current page in the background and swaps in
// just #refresh-region, instead of reloading the whole page -- a full
// reload was interrupting whatever the operator was doing (scrolling,
// about to click, mid-selection). The server still computes every value;
// this only replaces the one region's markup with what it just rendered.
//
// Two modes:
//   - fixed (body data-refresh-seconds): the dashboard and stats views.
//   - adaptive (body data-refresh-adaptive): the Events tab. Same scheme
//     as the library's own subscribers -- after each poll that brought
//     nothing new (#refresh-region's data-newest-seq unchanged) the wait
//     doubles, and it snaps back to the minimum as soon as events flow.
//     Bounds are 0.5 s .. 5 s rather than the library's 50 ms .. 1 s,
//     since each poll here renders a whole page region.
//
// Pausing (the #auto-refresh checkbox or the #pause-refresh button) is
// remembered in localStorage when available.
(function () {
  "use strict";

  var body = document.body;
  var fixedSeconds = parseInt(body.getAttribute("data-refresh-seconds") || "0", 10);
  var adaptive = body.getAttribute("data-refresh-adaptive") === "1";
  if (!fixedSeconds && !adaptive) return;

  var MIN_MS = 500, MAX_MS = 5000;
  var storageKey = "lytecache-ui-autorefresh-paused";
  var regionId = "refresh-region";
  var toggle = document.getElementById("auto-refresh");
  var pauseButton = document.getElementById("pause-refresh");
  var status = document.getElementById("refresh-status");
  var pausedFallback = false;
  var delay = adaptive ? MIN_MS : fixedSeconds * 1000;
  var timer = null;

  function isPaused() {
    try {
      return window.localStorage.getItem(storageKey) === "1";
    } catch (e) {
      return pausedFallback;
    }
  }

  function setPaused(paused) {
    pausedFallback = paused;
    try {
      window.localStorage.setItem(storageKey, paused ? "1" : "0");
    } catch (e) {
      // localStorage unavailable -- the in-memory fallback still works for this page
    }
    render();
    if (!paused) schedule(adaptive ? MIN_MS : delay);
  }

  function render() {
    var paused = isPaused();
    if (toggle) toggle.checked = !paused;
    if (pauseButton) pauseButton.textContent = paused ? "resume" : "pause";
    if (status) {
      status.textContent = paused
        ? "paused"
        : "live, next refresh in " + (delay / 1000).toFixed(1) + "s";
    }
  }

  if (toggle) toggle.addEventListener("change", function () { setPaused(!toggle.checked); });
  if (pauseButton) pauseButton.addEventListener("click", function () { setPaused(!isPaused()); });

  function newestSeq(region) {
    return region ? region.getAttribute("data-newest-seq") : null;
  }

  function schedule(ms) {
    if (timer) clearTimeout(timer);
    delay = ms;
    render();
    timer = setTimeout(refresh, ms);
  }

  function refresh() {
    timer = null;
    if (isPaused()) return;
    var currentRegion = document.getElementById(regionId);
    if (!currentRegion) return;

    fetch(window.location.href, { credentials: "same-origin" })
      .then(function (res) { return res.text(); })
      .then(function (html) {
        var next = new DOMParser().parseFromString(html, "text/html");
        var nextRegion = next.getElementById(regionId);
        // A missing region most likely means the session expired and this
        // fetch landed on the login page -- leave the visible page alone;
        // the operator's next real click hits the login redirect normally.
        if (!nextRegion) return;
        var changed = newestSeq(nextRegion) !== newestSeq(currentRegion);
        currentRegion.replaceWith(nextRegion);
        if (adaptive) {
          schedule(changed ? MIN_MS : Math.min(delay * 2, MAX_MS));
        } else {
          schedule(delay);
        }
      })
      .catch(function () {
        schedule(adaptive ? Math.min(delay * 2, MAX_MS) : delay); // network blip -- try again
      });
  }

  render();
  if (!isPaused()) schedule(delay);
})();
