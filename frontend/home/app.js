import { bindUi } from "../shared/ui/index.js";

const descriptions = [
  "This page was built to demonstrate a simple interactive web component.",
  "Did you know? The first website ever created is still online at info.cern.ch.",
  "Web applications can range from a single static page to complex real-time systems.",
  "JavaScript was created in just 10 days by Brendan Eich in 1995.",
  "HTML stands for HyperText Markup Language and is the backbone of the web.",
  "CSS allows developers to separate content from visual presentation.",
  "The average web page makes over 70 HTTP requests to fully load.",
  "Responsive design ensures websites look good on screens of all sizes.",
  "Progressive Web Apps can work offline and feel like native applications.",
  "The internet and the World Wide Web are not the same thing."
];

function getRandomDescription() {
  const index = Math.floor(Math.random() * descriptions.length);
  return descriptions[index];
}

export function boot() {
  const learnMoreBtn = document.getElementById("learn-more-btn");
  const popupOverlay = document.getElementById("popup-overlay");
  const popupDescription = document.getElementById("popup-description");
  const popupCloseBtn = document.getElementById("popup-close-btn");
  const gameDevJobsBtn = document.getElementById("game-dev-jobs-btn");

  bindUi(learnMoreBtn, "click", () => {
    if (!popupDescription || !popupOverlay) return;
    popupDescription.textContent = getRandomDescription();
    popupOverlay.classList.remove("hidden");
  });

  bindUi(popupCloseBtn, "click", () => {
    popupOverlay?.classList.add("hidden");
  });

  bindUi(popupOverlay, "click", event => {
    if (event.target === popupOverlay) {
      popupOverlay.classList.add("hidden");
    }
  });

  bindUi(gameDevJobsBtn, "click", () => {
    window.location.href = "jobs.html";
  });
}
