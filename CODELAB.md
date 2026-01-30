# Climate Mediator User Guide: Data Upload and Visualization

This guide shows you how to upload your malaria data to the Climate Mediator system and create visualizations in Superset. All technical infrastructure is already set up and running on the server.

## Table of Contents

1. [Overview](#overview)
2. [Data Preparation](#data-preparation)
3. [Uploading Data](#uploading-data)
4. [Creating Superset Dashboards](#creating-superset-dashboards)
5. [Using the Query for Dataset Creation](#using-the-query-for-dataset-creation)
6. [Visualization Examples](#visualization-examples)
7. [Troubleshooting](#troubleshooting)

## Overview

The Climate Mediator system is designed to:
- Process malaria case data
- Generate predictions using AI models
- Store data in a database
- Create visualizations in Superset

**What you need to do:**
1. Prepare your data in the correct format
2. Upload it through the web interface
3. Create visualizations in Superset
4. Use the provided query to generate datasets

## Prerequisites

- Access to the Climate Mediator web interface
- Access to Superset dashboard
- Your malaria case data in CSV format

## Data Preparation

### Required Data Format

Your malaria case data should be in CSV format with these columns:

```csv
Health Facility Name,Type,Likely Woreda/Administration,Latitude,Longitude,population,time_period,Total Malaria Cases
Adama Hospital,Health Center,Adama,8.5462,39.2784,50000,202301,45
Adama Hospital,Health Center,Adama,8.5462,39.2784,50000,202302,52
...
```

### Column Descriptions

- **Health Facility Name**: Name of your health facility
- **Type**: Type of facility (Health Center, Hospital, etc.)
- **Woreda/Administration**: Administrative region name
- **Latitude/Longitude**: Geographic coordinates (decimal format)
- **Population**: Population served by the facility
- **Time Period**: Date in YYYYMM format (e.g., 202301 for January 2023)
- **Total Malaria Cases**: Number of malaria cases for that period

### Data Tips

- Use consistent naming for facilities and regions
- Ensure coordinates are in decimal format (not degrees/minutes/seconds)
- Time periods should be in YYYYMM format
- Include at least 12 months of data for better predictions

## Uploading Data

### Step 1: Access the Climate Mediator Interface

1. Open your web browser
2. Navigate to the Climate Mediator URL provided by your administrator
3. Log in with your credentials

### Step 2: Upload Your Data

1. Click on **"Upload Data"** or **"New Upload"**
2. Select your CSV file from your computer
3. Choose a bucket name (this will be your project name)
   - Use a descriptive name like "malaria-data-2024" or "region-name-data"
4. Click **"Upload"**

### Step 3: Process Your Data

1. After upload, click **"Process Data"**
2. The system will:
   - Validate your data format
   - Generate predictions using AI models
   - Store the data in the database
3. Wait for the processing to complete (this may take a few minutes)

### Step 4: Verify Upload

1. Check the status of your upload
2. You should see a success message
3. Note the table name that was created (you'll need this for Superset)

## Creating Superset Dashboards

### Step 1: Access Superset

1. Open your web browser
2. Navigate to the Superset URL provided by your administrator
3. Log in with your credentials

### Step 2: Create a New Dataset

1. Go to **Data > Datasets**
2. Click **"+ Dataset"**
3. Select your database (usually "Climate Mediator DB")
4. Choose **"Custom SQL"** option
5. Use the provided query (see next section)

## Using the Query for Dataset Creation

### The Standard Query

Use this query to create your dataset in Superset:

```sql
SELECT 
    facility_name,
    region,
    latitude,
    longitude,
    time_period,
    total_cases as historical_cases,
    predicted_cases,
    CASE 
        WHEN time_period > '202312' THEN true 
        ELSE false 
    END as is_predicted,
    population,
    facility_type
FROM malaria_predictions
WHERE bucket_name = 'YOUR_BUCKET_NAME'
ORDER BY time_period, facility_name
```

### Customizing the Query

Replace `YOUR_BUCKET_NAME` with the bucket name you used when uploading your data.

You can also modify the query to:
- Filter by specific regions: `AND region = 'Your Region'`
- Filter by date range: `AND time_period BETWEEN '202301' AND '202312'`
- Filter by facility type: `AND facility_type = 'Hospital'`

### Step 3: Create Your First Chart

1. After creating the dataset, go to **Charts**
2. Click **"+ Chart"**
3. Select **"Mapbox"** as chart type
4. Choose your dataset
5. Configure the chart:
   - **Longitude**: `longitude`
   - **Latitude**: `latitude`
   - **Size**: `historical_cases` or `predicted_cases`
   - **Color**: `is_predicted`
6. Click **"Create Chart"**

### Step 4: Create Additional Visualizations

#### Time Series Chart
1. Create a new chart
2. Select **"Line Chart"**
3. Configure:
   - **X-axis**: `time_period`
   - **Y-axis**: `historical_cases` and `predicted_cases`
   - **Group by**: `region`

#### Summary Statistics
1. Create a new chart
2. Select **"Big Number"**
3. Configure:
   - **Metric**: `SUM(historical_cases)` or `SUM(predicted_cases)`
   - **Filters**: Add time period filters

### Step 5: Create a Dashboard

1. Go to **Dashboards**
2. Click **"+ Dashboard"**
3. Name your dashboard (e.g., "Malaria Predictions 2024")
4. Add your charts to the dashboard
5. Arrange and resize charts as needed
6. Save the dashboard

## Visualization Examples

### Map Visualization

Create a map showing:
- **Red dots**: Predicted cases
- **Blue dots**: Historical cases
- **Size**: Number of cases
- **Tooltips**: Facility name, region, case numbers

### Time Series Analysis

Show trends over time:
- **Historical data**: Solid line
- **Predicted data**: Dashed line
- **Different colors**: For different regions

### Summary Cards

Display key metrics:
- Total historical cases
- Total predicted cases
- Number of facilities
- Date range

### Filters

Add interactive filters:
- **Region**: Filter by specific regions
- **Time Period**: Filter by date range
- **Facility Type**: Filter by hospital/health center
- **Prediction Status**: Show only historical or predicted data

## Troubleshooting

### Common Issues

#### 1. Upload Issues
- **File format error**: Ensure your CSV has the correct column headers
- **File too large**: Try uploading smaller files or contact your administrator
- **Network timeout**: Check your internet connection and try again

#### 2. Data Processing Issues
- **Processing failed**: Check that your data format is correct
- **Missing predictions**: Ensure you have enough historical data (at least 12 months)
- **Wrong coordinates**: Verify latitude/longitude are in decimal format

#### 3. Superset Issues
- **Can't connect to database**: Contact your administrator
- **Query error**: Check that your bucket name is correct in the query
- **Charts not loading**: Refresh the page and try again

#### 4. Visualization Issues
- **Map not showing**: Check that latitude/longitude columns are selected
- **No data in charts**: Verify your dataset has data and filters are correct
- **Tooltips not working**: Ensure the tooltip script is properly configured

### Getting Help

1. **Check the status**: Look for error messages in the interface
2. **Verify your data**: Ensure CSV format matches the requirements
3. **Contact support**: Reach out to your system administrator
4. **Check documentation**: Review this guide for common solutions

### Best Practices

1. **Data Quality**: Ensure your data is clean and consistent
2. **Naming**: Use descriptive names for buckets and dashboards
3. **Backup**: Keep copies of your original data files
4. **Testing**: Start with small datasets to test the process
5. **Documentation**: Keep notes of your bucket names and dashboard configurations

## Quick Reference

### Required CSV Format
```csv
Health Facility Name,Type,Likely Woreda/Administration,Latitude,Longitude,population,time_period,Total Malaria Cases
```

### Standard Superset Query
```sql
SELECT 
    facility_name,
    region,
    latitude,
    longitude,
    time_period,
    total_cases as historical_cases,
    predicted_cases,
    CASE 
        WHEN time_period > '202312' THEN true 
        ELSE false 
    END as is_predicted,
    population,
    facility_type
FROM malaria_predictions
WHERE bucket_name = 'YOUR_BUCKET_NAME'
ORDER BY time_period, facility_name
```

### Chart Types to Use
- **Mapbox**: For geographic visualization
- **Line Chart**: For time series analysis
- **Big Number**: For summary statistics
- **Bar Chart**: For comparisons

## Support

For technical support:
- Contact your system administrator
- Check the system status page
- Review error messages carefully
- Ensure all prerequisites are met
