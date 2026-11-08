# Web app for the Canada Weather Downloader (see web/README.md).
#   docker build -t canada-weather-web .
#   docker run --rm -p 8080:8080 canada-weather-web

FROM node:22-alpine AS build
WORKDIR /app/web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
# The build turns this inventory into public/stations.json (scripts/build-stations.ts).
COPY ["src/canada_weather/data/Station Inventory EN.csv", "/app/src/canada_weather/data/"]
RUN npm run build

FROM node:22-alpine
ENV NODE_ENV=production \
    PORT=8080 \
    STATIC_ROOT=/app/client
WORKDIR /app
# The server (with the station list) is bundled into one file, so no
# node_modules are needed at runtime.
COPY --from=build /app/web/dist/server.mjs ./server.mjs
COPY --from=build /app/web/dist/client ./client
USER node
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s CMD wget -qO- http://127.0.0.1:8080/api/health || exit 1
CMD ["node", "server.mjs"]
