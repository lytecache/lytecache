// Events tab: payload previews stay hidden until the operator reveals
// them (the same reveal-behind-a-click treatment every value gets), and a
// click anywhere on a tail row opens that event in the viewer. Both work
// across poll.js's region swaps, since the state lives on <body> and the
// click handler is delegated.
(function () {
  "use strict";

  var button = document.getElementById("toggle-previews");
  if (button) {
    button.addEventListener("click", function () {
      var on = document.body.classList.toggle("show-previews");
      button.textContent = on ? "hide payload previews" : "show payload previews";
    });
  }

  document.addEventListener("click", function (e) {
    var row = e.target.closest && e.target.closest("tr[data-href]");
    if (!row || e.target.closest("a, button, input, form")) return;
    window.location = row.getAttribute("data-href");
  });
})();
