package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// --- Constants & Global Data ---
const inputFileName = "gondar_dataset.csv"
const outputJsonFile = "ewars_monthly_malaria_data_seasonal_lagged_sorted.json"

// Ethiopian to Gregorian month mapping
var ethiopianToGregorianMap = map[string]struct {
	Month  string // Gregorian MM
	Offset int    // Year offset (e.g., 2008 EC -> 2015/2016 GC)
}{
	"Meskerem": {"09", 7}, "Tikemet": {"10", 7}, "Hidar": {"11", 7},
	"Tahesas": {"12", 7}, "Tir": {"01", 8}, "Yekatit": {"02", 8},
	"Megabit": {"03", 8}, "Miazia": {"04", 8}, "Ginbot": {"05", 8},
	"Sene": {"06", 8}, "Hamle": {"07", 8}, "Nehase": {"08", 8},
}

// --- JSON Structs ---

type DataPoint struct {
	OU    string  `json:"ou"`
	PE    string  `json:"pe"`
	Value float64 `json:"value"`
}

type Feature struct {
	FeatureID string      `json:"featureId"`
	DHIS2ID   string      `json:"dhis2Id"`
	Data      []DataPoint `json:"data"`
}

type GeoFeatureProperties struct {
	Code        string   `json:"code"`
	Name        string   `json:"name"`
	Level       string   `json:"level"`
	Parent      string   `json:"parent"`
	ParentGraph string   `json:"parentGraph"`
	Groups      []string `json:"groups"`
}

type GeoFeatureGeometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type GeoFeature struct {
	Type       string               `json:"type"`
	ID         string               `json:"id"`
	Properties GeoFeatureProperties `json:"properties"`
	Geometry   GeoFeatureGeometry   `json:"geometry"`
}

type OrgUnitsGeoJSON struct {
	Type     string       `json:"type"`
	Features []GeoFeature `json:"features"`
}

type FinalJSONStructure struct {
	ModelID         string          `json:"model_id"`
	Features        []Feature       `json:"features"`
	OrgUnitsGeoJSON OrgUnitsGeoJSON `json:"orgUnitsGeoJson"`
	NPeriods        int             `json:"n_periods"`
}

type WoredaMeta struct {
	Population float64
	Latitude   float64
	Longitude  float64
}

// --- Utility Functions ---

// formatPE converts YYYY-MM to DHIS2's YYYYMM format.
func formatPE(timePeriod string) string {
	t, err := time.Parse("2006-01", timePeriod)
	if err != nil {
		return strings.ReplaceAll(timePeriod, "-", "")
	}
	return t.Format("200601")
}

// extractEthiopianDate extracts the month and year from a header string
func extractEthiopianDate(header string) (string, string) {
	parts := strings.Split(header, " ")
	if len(parts) >= 2 {
		return parts[len(parts)-2], parts[len(parts)-1]
	}
	return "", ""
}

// sortDataPoints sorts the DataPoint slice first by OU (Woreda) and then by PE (Time Period).
func sortDataPoints(data []DataPoint) {
	sort.Slice(data, func(i, j int) bool {
		if data[i].OU != data[j].OU {
			return data[i].OU < data[j].OU
		}
		return data[i].PE < data[j].PE
	})
}

// getChronologicalPeriods returns a sorted list of all unique YYYYMM periods.
func getChronologicalPeriods(periods map[string]struct{}) []string {
	var chronologicalPeriods []string
	for pe := range periods {
		chronologicalPeriods = append(chronologicalPeriods, pe)
	}
	sort.Strings(chronologicalPeriods)
	return chronologicalPeriods
}

// applySeasonalImputation uses the same month's value from the previous year (YYYY-1) to fill missing data.
// If the previous year's value is not found, the missing value is set to 0.0.
func applySeasonalImputation(woreda string, periods []string, rawData map[string]float64, population float64) ([]DataPoint, []DataPoint) {
	var imputedCases []DataPoint
	var populationPoints []DataPoint

	// Map data for this specific woreda for easy lookup: Key: YYYYMM -> Value
	woredaRawCases := make(map[string]float64)
	for key, val := range rawData {
		if strings.HasPrefix(key, woreda+"|") {
			pe := strings.Split(key, "|")[1]
			woredaRawCases[pe] = val
		}
	}

	for _, pe := range periods {
		caseValue, found := woredaRawCases[pe]

		if !found {
			// Data is missing, attempt seasonal imputation

			// 1. Calculate previous year's period (YYYYMM -> (YYYY-1)MM)
			currentYearStr := pe[:4]
			currentMonthStr := pe[4:]

			currentYear, err := strconv.Atoi(currentYearStr)
			if err != nil {
				// If period format is bad, skip
				continue
			}
			prevYear := currentYear - 1
			prevPE := fmt.Sprintf("%d%s", prevYear, currentMonthStr)

			// 2. Look up the previous year's value
			prevYearValue, prevFound := woredaRawCases[prevPE]

			if prevFound {
				// Found the previous year's month value, use it for imputation
				caseValue = prevYearValue
			} else {
				// Previous year's month value is also missing (e.g., year 1 of the dataset)
				// Default to 0.0
				caseValue = 0.0
			}

		}

		// Add the data point (actual or imputed)
		imputedCases = append(imputedCases, DataPoint{
			OU:    woreda,
			PE:    pe,
			Value: math.Round(math.Abs(caseValue)),
		})

		// Population data point (constant, always present)
		populationPoints = append(populationPoints, DataPoint{
			OU:    woreda,
			PE:    pe,
			Value: population,
		})
	}
	return imputedCases, populationPoints
}

// --- Main Execution ---

func main() {
	file, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("Error opening input file %s: %v", inputFileName, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		log.Fatalf("Error reading CSV header: %v", err)
	}

	// Maps for aggregation and metadata
	woredaMetaMap := make(map[string]WoredaMeta)    // Key: Woreda Name
	woredaCaseAggregate := make(map[string]float64) // Key: Woreda|YYYYMM -> Total Cases
	uniqueTimePeriods := make(map[string]struct{})  // Key: YYYYMM

	re := regexp.MustCompile(`[^\d.]`)

	// --- 1. Read, Aggregate, and Extract Metadata/Periods ---
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV record: %v", err)
			continue
		}

		if len(record) < 6 {
			continue
		}

		woreda := strings.TrimSpace(record[2])

		// A. Extract Woreda Metadata (assuming Woreda is constant per row)
		if _, exists := woredaMetaMap[woreda]; !exists {
			popStr := re.ReplaceAllString(strings.TrimSpace(record[5]), "")
			population, errPop := strconv.ParseFloat(popStr, 64)
			lat, errLat := strconv.ParseFloat(strings.TrimSpace(record[3]), 64)
			lon, errLon := strconv.ParseFloat(strings.TrimSpace(record[4]), 64)

			if errPop == nil && errLat == nil && errLon == nil {
				woredaMetaMap[woreda] = WoredaMeta{
					Population: population,
					Latitude:   lat,
					Longitude:  lon,
				}
			} else {
				log.Printf("Skipping metadata for woreda '%s' due to invalid population/geo: %v, %v, %v", woreda, errPop, errLat, errLon)
				continue
			}
		}

		// B. Unpivot and Aggregate Cases
		for i := 6; i < len(record) && i < len(headers); i++ {
			dataValueStr := strings.TrimSpace(record[i])
			header := headers[i]

			if dataValueStr == "" {
				continue
			}

			dataValue, err := strconv.ParseFloat(dataValueStr, 64)
			if err != nil {
				continue
			}

			// Convert Ethiopian Date
			ethiopianMonth, ethiopianYearStr := extractEthiopianDate(header)
			info, ok := ethiopianToGregorianMap[ethiopianMonth]
			if !ok {
				continue
			}

			ethiopianYear, err := strconv.Atoi(ethiopianYearStr)
			if err != nil {
				continue
			}
			gregorianYear := ethiopianYear + info.Offset
			timePeriod := fmt.Sprintf("%d-%s", gregorianYear, info.Month)
			pe := formatPE(timePeriod)

			uniqueTimePeriods[pe] = struct{}{}

			aggregationKey := woreda + "|" + pe
			woredaCaseAggregate[aggregationKey] += math.Abs(dataValue)
		}
	}

	// --- 2. Apply Seasonal Imputation per Woreda ---
	var allPopulationDataPoints []DataPoint
	var allMalariaDataPoints []DataPoint

	// Get all periods present in the dataset, sorted chronologically
	chronologicalPeriods := getChronologicalPeriods(uniqueTimePeriods)

	for woreda, meta := range woredaMetaMap {
		imputedCases, populationPoints := applySeasonalImputation(
			woreda,
			chronologicalPeriods,
			woredaCaseAggregate,
			meta.Population,
		)
		allMalariaDataPoints = append(allMalariaDataPoints, imputedCases...)
		allPopulationDataPoints = append(allPopulationDataPoints, populationPoints...)
	}

	// --- 3. Sort Final Data Points (by OU then by PE) ---
	sortDataPoints(allPopulationDataPoints)
	sortDataPoints(allMalariaDataPoints)

	// --- 4. Build GeoJSON Features ---
	var geoFeatures []GeoFeature
	// Placeholder GeoJSON properties for consistency with your example
	geoPropsTemplate := GeoFeatureProperties{
		Code:        "OU_XXX",
		Level:       "2",
		Parent:      "ImspTQPwCqd",
		ParentGraph: "ImspTQPwCqd",
		Groups:      []string{"jqBqIXoXpfy", "w1Atoz18PCL"},
	}

	for woreda, meta := range woredaMetaMap {
		feature := GeoFeature{
			Type: "Feature",
			ID:   woreda,
			Properties: GeoFeatureProperties{
				Code:        geoPropsTemplate.Code,
				Name:        woreda,
				Level:       geoPropsTemplate.Level,
				Parent:      geoPropsTemplate.Parent,
				ParentGraph: geoPropsTemplate.ParentGraph,
				Groups:      geoPropsTemplate.Groups,
			},
			Geometry: GeoFeatureGeometry{
				Type:        "Point",
				Coordinates: []float64{meta.Longitude, meta.Latitude}, // GeoJSON is [lon, lat]
			},
		}
		geoFeatures = append(geoFeatures, feature)
	}

	// --- 5. Construct Final JSON Structure ---
	finalJSON := FinalJSONStructure{
		ModelID: "chap_ewars_monthly",
		Features: []Feature{
			{
				FeatureID: "population",
				DHIS2ID:   "K9QpxzIH3po",
				Data:      allPopulationDataPoints,
			},
			{
				FeatureID: "disease_cases",
				DHIS2ID:   "Jzk4NxHtpz2",
				Data:      allMalariaDataPoints,
			},
		},
		OrgUnitsGeoJSON: OrgUnitsGeoJSON{
			Type:     "FeatureCollection",
			Features: geoFeatures,
		},
		NPeriods: 6,
	}

	// --- 6. Write JSON to File ---
	jsonData, err := json.MarshalIndent(finalJSON, "", "    ")
	if err != nil {
		log.Fatalf("Error marshaling JSON: %v", err)
	}

	if err := os.WriteFile(outputJsonFile, jsonData, 0644); err != nil {
		log.Fatalf("Error writing JSON file: %v", err)
	}

	fmt.Printf("\nSuccessfully generated EWARS JSON file with Seasonal Imputation (lagged).\nOutput saved to %s\n", outputJsonFile)
}
