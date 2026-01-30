package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const inputFileName = "enriched-dataset.csv"
const outputFileName = "malaria_cases_per_facility_per_month_aggregated.csv"

// monthInfo holds the Gregorian month (MM) and the year offset
type monthInfo struct {
	GregorianMonth string
	YearOffset     int
}

// metadata stores the facility information needed for the final output
type metadata struct {
	FacilityName string
	Type         string
	Woreda       string
	Latitude     string
	Longitude    string
	Population   string
}

// ethiopianToGregorianMap defines the conversion logic
var ethiopianToGregorianMap = map[string]monthInfo{
	"Meskerem": {"09", 7}, // Sep/Oct: E.C. Year + 7
	"Tikemet":  {"10", 7}, // Oct/Nov: E.C. Year + 7
	"Hidar":    {"11", 7}, // Nov/Dec: E.C. Year + 7
	"Tahesas":  {"12", 7}, // Dec/Jan: E.C. Year + 7
	"Tir":      {"01", 8}, // Jan/Feb: E.C. Year + 8
	"Yekatit":  {"02", 8}, // Feb/Mar: E.C. Year + 8
	"Megabit":  {"03", 8}, // Mar/Apr: E.C. Year + 8
	"Miazia":   {"04", 8}, // Apr/May: E.C. Year + 8
	"Ginbot":   {"05", 8}, // May/Jun: E.C. Year + 8
	"Sene":     {"06", 8}, // Jun/Jul: E.C. Year + 8
	"Hamle":    {"07", 8}, // Jul/Aug: E.C. Year + 8
	"Nehase":   {"08", 8}, // Aug/Sep: E.C. Year + 8
}

func main() {
	// 1. Read the input CSV file
	file, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", inputFileName, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Read the header row
	headers, err := reader.Read()
	if err != nil {
		log.Fatalf("Error reading CSV header: %v", err)
	}

	// Define the aggregation map: Key is FacilityName|TimePeriod, Value is Total Cases
	aggregatedData := make(map[string]float64)
	// Map to store metadata for writing the output: Key is FacilityName, Value is metadata struct
	metadataMap := make(map[string]metadata)

	// --- 2. Unpivot, Convert, and Aggregate ---
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading CSV record: %v", err)
		}

		// The first 6 columns are metadata (id_vars)
		if len(record) < 6 {
			continue // Skip records with insufficient columns
		}

		facilityName := record[0]

		// Store metadata for later output writing
		metadataMap[facilityName] = metadata{
			FacilityName: facilityName,
			Type:         record[1],
			Woreda:       record[2],
			Latitude:     record[3],
			Longitude:    record[4],
			Population:   record[5],
		}

		// Iterate over the data columns (starting from the 7th column, index 6)
		for i := 6; i < len(record) && i < len(headers); i++ {
			dataValueStr := strings.TrimSpace(record[i])
			header := headers[i]

			if dataValueStr == "" {
				continue // Skip empty cells (NaN)
			}

			// Parse data value
			dataValue, err := strconv.ParseFloat(dataValueStr, 64)
			if err != nil {
				// Log non-fatal error for data conversion and skip
				// fmt.Printf("Skipping non-numeric value '%s' in column '%s': %v\n", dataValueStr, header, err)
				continue
			}

			// Extract Ethiopian Date components from header
			parts := strings.Split(header, " ")
			if len(parts) < 2 {
				continue // Skip malformed headers
			}
			ethiopianMonth := parts[len(parts)-2]
			ethiopianYearStr := parts[len(parts)-1]

			info, ok := ethiopianToGregorianMap[ethiopianMonth]
			if !ok {
				// Skip if month is unknown
				continue
			}

			// Convert Ethiopian Year to Gregorian Year
			ethiopianYear, err := strconv.Atoi(ethiopianYearStr)
			if err != nil {
				// Skip if year is malformed
				continue
			}
			gregorianYear := ethiopianYear + info.YearOffset
			timePeriod := fmt.Sprintf("%d-%s", gregorianYear, info.GregorianMonth)

			// --- 5. Aggregate ---
			// Create the aggregation key
			aggregationKey := facilityName + "|" + timePeriod

			// Sum the malaria cases
			aggregatedData[aggregationKey] += dataValue
		}
	}

	// --- 6. Write the aggregated output CSV ---
	outFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("Error creating output file %s: %v", outputFileName, err)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// Write header for the aggregated data
	outputHeaders := []string{
		"Health Facility Name",
		"Type",
		"Likely Woreda/Administration",
		"Latitude",
		"Longitude",
		"population",
		"time_period",
		"Total Malaria Cases",
	}
	if err := writer.Write(outputHeaders); err != nil {
		log.Fatalf("Error writing output header: %v", err)
	}

	// Prepare data for sorting (Go maps are unordered)
	var keys []string
	for key := range aggregatedData {
		keys = append(keys, key)
	}
	// Note: Sorting is typically complex in Go without external libs, so we just iterate for now.
	// For actual production use, you would sort the keys array before iterating.

	for _, key := range keys {
		parts := strings.Split(key, "|")
		facilityName := parts[0]
		timePeriod := parts[1]
		totalCases := aggregatedData[key]

		meta := metadataMap[facilityName]

		record := []string{
			meta.FacilityName,
			meta.Type,
			meta.Woreda,
			meta.Latitude,
			meta.Longitude,
			meta.Population,
			timePeriod,
			strconv.FormatFloat(totalCases, 'f', 2, 64),
		}

		if err := writer.Write(record); err != nil {
			log.Fatalf("Error writing output record: %v", err)
		}
	}

	fmt.Printf("Successfully processed and aggregated data.\nOutput saved to %s\n", outputFileName)
}
