/** The condensed station list, bundled into the server at build time. */

import data from "../../public/stations.json";
import { stationLookup } from "../shared/stations.ts";

export const stations = stationLookup(data);
