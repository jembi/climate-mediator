export interface PredictionPayload {
  model_id: string;
  features: PredictionPayloadFeature[];
  orgUnitsGeoJson: OrgUnitsGeoJSON;
  n_periods: number;
}

export interface PredictionPayloadFeature {
  featureId: string;
  dhis2Id: string;
  data: Datum[];
}

export interface Datum {
  ou: string;
  pe: string;
  value: number;
}

export interface OrgUnitsGeoJSON {
  type: string;
  features: OrgUnitsGeoJSONFeature[];
}

export interface OrgUnitsGeoJSONFeature {
  type: string;
  id: string;
  properties: Properties;
  geometry: Geometry;
}

export interface Geometry {
  type: string;
  coordinates: number[];
}

export interface Properties {
  code: string;
  name: string;
  level: string;
  parent: string;
  parentGraph: string;
  groups: string[];
}
