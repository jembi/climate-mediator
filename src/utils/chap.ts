import { ClickhouseHistoricalDisease, ClickhouseOrganzation } from "./clickhouse";

export function buildChapPayload(historicalDisease: ClickhouseHistoricalDisease[], organizations: ClickhouseOrganzation[]): any {
	const payload = {
		model_id: 'chap_ewars_monthly',
		features: [
			{
				featureId: "disease_cases",
				dhis2Id: "Jzk4NxHtpz2",
				data: historicalDisease.map((disease) => {
					return {
						ou: disease.organizational_unit,
						pe: disease.period,
						value: disease.value
					};
				}
				)
			},
			{
				featureId: "population",
				dhis2Id: "K9QpxzIH3po",

				// @todo: get population data from population table (need to create one)
				data: historicalDisease.map((data) => {
					return {
						ou: data.organizational_unit,
						pe: data.period,
						value: data.value
					};
				}
				),
			}
		],
		orgUnitsGeoJson: {
			type: "FeatureCollection",
			features: organizations.map((org) => {
				return {
					type: "Feature",
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
						type: org.type === 'point' ? "Point" : 'Polygon',
						coordinates: org.type === 'point' ? [org.longitude, org.latitude] : [org.coordinates]
					}
				};
			})
		},
	};

  return payload;
}
