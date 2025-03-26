import { ClickhouseHistoricalDisease, ClickhouseOrganzation, ClickhousePopulationData } from "./clickhouse";

export function buildChapPayload(historicalDisease: ClickhouseHistoricalDisease[], organizations: ClickhouseOrganzation[], populations: ClickhousePopulationData[]): any {
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
						value: +disease.value
					};
				}
				)
			},
			{
				featureId: "population",
				dhis2Id: "K9QpxzIH3po",
				data: populations.map((population) => {
					return {
						ou: population.organizational_unit,
						pe: population.period,
						value: +population.value
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
		// n_periods: 24
	};

	// console.log(JSON.stringify(payload))

  return (payload);
}

function fixPayload(payload: any) {
	let fixedPayload = JSON.parse(JSON.stringify(payload)); // Deep copy to avoid mutation
    const requiredMonths = 24; // At least 2 years of data
    const currentYearMonth = new Date().getFullYear() * 100 + new Date().getMonth() + 1; // YYYYMM format
    const earliestRequiredPeriod = currentYearMonth - requiredMonths;

    // Convert "value" fields from string to number
    // @ts-ignore
		fixedPayload.features.forEach(feature => {
			// @ts-ignore
        feature.data.forEach(entry => {
            if (typeof entry.value === "string") {
                entry.value = Number(entry.value);
            }
        });
    });

    // Remove duplicate orgUnits in orgUnitsGeoJson
    let seenOrgUnits = new Set();
		// @ts-ignore
    fixedPayload.orgUnitsGeoJson.features = fixedPayload.orgUnitsGeoJson.features.filter(feature => {
        if (seenOrgUnits.has(feature.id)) {
            return false; // Remove duplicate
        }
        seenOrgUnits.add(feature.id);
        return true;
    });

    // Ensure all locations in `features` exist in `orgUnitsGeoJson`
		// @ts-ignore
    let orgUnitIds = new Set(fixedPayload.orgUnitsGeoJson.features.map(feature => feature.id));
    let requiredOrgUnits = new Set(
			// @ts-ignore
        fixedPayload.features.flatMap(feature => feature.data.map(entry => entry.ou))
    );

    requiredOrgUnits.forEach(orgUnit => {
        if (!orgUnitIds.has(orgUnit)) {
            console.warn(`Missing orgUnit in GeoJSON: ${orgUnit}`);
            fixedPayload.orgUnitsGeoJson.features.push({
                type: "Feature",
                id: orgUnit,
                properties: { code: "UNKNOWN", name: orgUnit, level: "2" },
                geometry: { type: "Point", coordinates: [0, 0] } // Default if unknown
            });
        }
    });

    // Ensure at least 24 months of data for each feature
		// @ts-ignore
    fixedPayload.features.forEach(feature => {
			// @ts-ignore
        let dataMap = new Map(feature.data.map(entry => [entry.pe, entry]));

        for (let i = 0; i < requiredMonths; i++) {
            let period = (earliestRequiredPeriod + i).toString();
            if (!dataMap.has(period)) {
                console.warn(`Missing data for ${feature.featureId} in ${period}, adding null`);
                feature.data.push({ ou: "UNKNOWN", pe: period, value: null }); // Or use estimated value
            }
        }

        // Sort data by period
				// @ts-ignore
        feature.data.sort((a, b) => a.pe.localeCompare(b.pe));
    });

    return fixedPayload;
}

