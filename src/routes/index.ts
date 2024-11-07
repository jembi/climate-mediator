import express from "express";
import { getConfig } from "../config/config";

const routes = express.Router();

const { bodySizeLimit } = getConfig();
const jsonBodyParser = express.json({
  type: "application/fhir+json",
  limit: bodySizeLimit,
});

export default routes;