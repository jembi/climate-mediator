import {
  ClickhouseHistoricalDisease,
  ClickhouseOrganzation,
  ClickhousePopulationData,
} from './clickhouse';

export function buildChapPayload(
  historicalDisease: ClickhouseHistoricalDisease[],
  organizations: ClickhouseOrganzation[],
  populations: ClickhousePopulationData[]
) {
  const payload = {
    model_id: 'chap_ewars_monthly',
    features: [
      {
        featureId: 'disease_cases',
        dhis2Id: 'Jzk4NxHtpz2',
        data: historicalDisease.map((disease) => {
          return {
            ou: disease.organizational_unit,
            pe: disease.period,
            value: +disease.value,
          };
        }),
      },
      {
        featureId: 'population',
        dhis2Id: 'K9QpxzIH3po',
        data: populations.map((population) => {
          return {
            ou: population.organizational_unit,
            pe: population.period,
            value: +population.value,
          };
        }),
      },
    ],
    orgUnitsGeoJson: {
      type: 'FeatureCollection',
      features: organizations.map((org) => {
        return {
          type: 'Feature',
          id: org.name,
          properties: {
            code: org.code,
            name: org.name,
            level: org.level,
            // parent: org.parent,
            // parentGraph: org.parentGraph,
            // groups: org.groups
          },
          geometry: {
            type: org.type === 'point' ? 'Point' : 'Polygon',
            coordinates:
              org.type === 'point' ? [org.longitude, org.latitude] : [org.coordinates],
          },
        };
      }),
    },
    n_periods: 72,
  };

  return payload;
}
