import { build, files, prerendered, version } from "$service-worker";

const CACHE_NAME = `irohsion-remote-${version}`;
const APP_SHELL = [
  ...new Set([...build, ...files, ...prerendered, "/", "/index.html"]),
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(CACHE_NAME)
      .then((cache) => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((key) => key !== CACHE_NAME)
            .map((key) => caches.delete(key)),
        ),
      )
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("fetch", (event) => {
  if (event.request.method !== "GET") return;

  event.respondWith(
    (async () => {
      const url = new URL(event.request.url);
      const cache = await caches.open(CACHE_NAME);

      if (APP_SHELL.includes(url.pathname)) {
        const cached = await cache.match(url.pathname);
        if (cached) return cached;
      }

      try {
        const response = await fetch(event.request);
        if (!(response instanceof Response)) {
          throw new Error("invalid response from fetch");
        }

        return response;
      } catch (error) {
        const cached = await cache.match(event.request);
        if (cached) {
          return cached;
        }

        if (event.request.mode === "navigate") {
          return (await cache.match("/")) || cache.match("/index.html");
        }

        throw error;
      }
    })(),
  );
});
