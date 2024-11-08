import express from "express";
import { config } from "../config/config";

const routes = express.Router();

const bodySizeLimit = config.bodySizeLimit;
const jsonBodyParser = express.json({
  type: "application/json",
  limit: bodySizeLimit,
});

export default routes;
