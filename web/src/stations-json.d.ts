// public/stations.json is generated (scripts/build-stations.ts) and bundled as JSON.
declare module "*/stations.json" {
  const value: import("./shared/stations.ts").StationsFile;
  export default value;
}
