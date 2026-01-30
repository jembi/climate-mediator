# Climate Mediator User Guide

## Quick Start Guide for Non-Technical Users

This guide will help you upload your malaria data and create visualizations using the Climate Mediator system.

## What You Need

1. **Your malaria data** in CSV format (see format requirements below)
2. **Access to the Climate Mediator web interface** (URL provided by your administrator)
3. **Access to Superset dashboard** (URL provided by your administrator)
4. **Dataset preparation script** (provided by your administrator)
5. **Existing dashboard template** (provided by your administrator)

## Step 1: Prepare Your Data

### Using the Dataset Preparation Script

**Before uploading your data, you must use the dataset preparation script provided by your administrator.**

The preparation script is located in the `scripts/` directory and includes:
- `data_preparation_script.py` - Python script for data validation and cleaning
- `prepare_data.bat` - Windows batch file for easy execution
- `README.md` - Detailed documentation for all scripts

#### Option 1: Using Python Script (All Platforms)

1. **Download the scripts directory** from your administrator
2. **Install Python** (if not already installed) and pandas library:
   ```bash
   pip install pandas
   ```
3. **Run the script** with your raw data:
   ```bash
   python data_preparation_script.py your_raw_data.csv prepared_data.csv
   ```

#### Option 2: Using Windows Batch File (Windows Only)

1. **Download the scripts directory** from your administrator
2. **Double-click** `prepare_data.bat` or run from command prompt:
   ```cmd
   prepare_data.bat your_raw_data.csv prepared_data.csv
   ```

#### What the Script Does

The preparation script will:
- ✅ Validate your data format and required columns
- 🧹 Clean and standardize the data
- 📊 Generate a summary report with statistics
- 💾 Create a properly formatted CSV file for upload
- ⚠️ Report any issues or missing data

#### Example Usage

```bash
# Validate data only (no output file created)
python data_preparation_script.py raw_malaria_data.csv --validate-only

# Prepare data for upload
python data_preparation_script.py raw_malaria_data.csv malaria_data_prepared.csv
```

#### Sample Data

If you need sample data to test with, use `example_data.csv` from the scripts directory.

### Required CSV Format

After running the preparation script, your CSV file should have these exact column headers:

```csv
Health Facility Name,Type,Likely Woreda/Administration,Latitude,Longitude,population,time_period,Total Malaria Cases
```

### Example Data

```csv
Health Facility Name,Type,Likely Woreda/Administration,Latitude,Longitude,population,time_period,Total Malaria Cases
Adama Hospital,Health Center,Adama,8.5462,39.2784,50000,202301,45
Adama Hospital,Health Center,Adama,8.5462,39.2784,50000,202302,52
Bahir Dar Hospital,Hospital,Bahir Dar,11.5890,37.3907,75000,202301,67
```

### Important Notes

- **Use the preparation script first**: Don't try to format data manually
- **Coordinates**: Must be in decimal format (e.g., 8.5462, not 8°32'46")
- **Time Period**: Use YYYYMM format (e.g., 202301 for January 2023)
- **Facility Names**: Use consistent naming throughout your data
- **Data Quality**: Ensure no missing values in critical columns

## Step 2: Upload Your Data

1. **Open the Climate Mediator interface** in your web browser
2. **Log in** with your credentials
3. **Click "Upload Data"** or "New Upload"
4. **Select your CSV file** from your computer
5. **Choose a bucket name** (this will be your project identifier)
   - Use a descriptive name like "malaria-data-2024" or "region-name-data"
6. **Click "Upload"**
7. **Wait for processing** (this may take a few minutes)
8. **Note your bucket name** - you'll need this for Superset

## Step 3: Import the Existing Dashboard

### Access Superset

1. **Open Superset** in your web browser
2. **Log in** with your credentials

### Import the Dashboard Template

1. **Go to Dashboards**
2. **Click the "+" button** and select **"Import Dashboard"**
3. **Upload the dashboard file** provided by your administrator
4. **Follow the import wizard**:
   - Select your database connection
   - Map the dataset to your bucket name
   - Configure any required settings
5. **Complete the import** process

### Create Your Dataset

1. **Go to Data > Datasets**
2. **Click "+ Dataset"**
3. **Select your database** (usually "Climate Mediator DB")
4. **Choose "Custom SQL"**
5. **Copy and paste this query** (replace YOUR_BUCKET_NAME with your actual bucket name):

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

6. **Click "Create Dataset"**

### Update the Dashboard

1. **Open your imported dashboard**
2. **The dashboard should automatically connect** to your new dataset
3. **If charts don't show data**:
   - Check that your bucket name is correct in the dataset
   - Verify that your data has been processed successfully
   - Refresh the dashboard

### Alternative: Create Charts Manually

If you prefer to create charts from scratch:

#### Create Your First Map

1. **Go to Charts**
2. **Click "+ Chart"**
3. **Select "Mapbox"** as chart type
4. **Choose your dataset**
5. **Configure the chart**:
   - **Longitude**: Select `longitude` column
   - **Latitude**: Select `latitude` column
   - **Size**: Select `historical_cases` or `predicted_cases`
   - **Color**: Select `is_predicted`
6. **Click "Create Chart"**

#### Create a Time Series Chart

1. **Create a new chart**
2. **Select "Line Chart"**
3. **Configure**:
   - **X-axis**: `time_period`
   - **Y-axis**: `historical_cases` and `predicted_cases`
   - **Group by**: `region`
4. **Click "Create Chart"**

#### Create Summary Statistics

1. **Create a new chart**
2. **Select "Big Number"**
3. **Configure**:
   - **Metric**: `SUM(historical_cases)` or `SUM(predicted_cases)`
4. **Click "Create Chart"**

#### Create a Custom Dashboard

1. **Go to Dashboards**
2. **Click "+ Dashboard"**
3. **Name your dashboard** (e.g., "Malaria Predictions 2024")
4. **Add your charts** to the dashboard
5. **Arrange and resize** charts as needed
6. **Save the dashboard**

## Step 4: Customize Your Visualizations

### Map Customization

- **Red dots**: Predicted cases
- **Blue dots**: Historical cases
- **Size**: Number of cases
- **Tooltips**: Show facility name, region, and case numbers

### Time Series Customization

- **Historical data**: Solid line
- **Predicted data**: Dashed line
- **Different colors**: For different regions
- **Interactive filters**: By region, time period, facility type

### Add Filters

You can add interactive filters to your dashboard:
- **Region**: Filter by specific regions
- **Time Period**: Filter by date range
- **Facility Type**: Filter by hospital/health center
- **Prediction Status**: Show only historical or predicted data

## Troubleshooting

### Common Issues

#### Upload Problems
- **File format error**: Check that your CSV has the correct column headers
- **File too large**: Try uploading smaller files or contact your administrator
- **Processing failed**: Ensure your data format is correct

#### Superset Problems
- **Can't connect**: Contact your administrator
- **Query error**: Check that your bucket name is correct in the query
- **No data showing**: Verify your dataset has data and filters are correct

#### Visualization Problems
- **Map not showing**: Check that latitude/longitude columns are selected
- **Charts not loading**: Refresh the page and try again
- **Wrong data**: Verify your bucket name in the query

### Getting Help

1. **Check error messages** in the interface
2. **Verify your data format** matches the requirements
3. **Contact your system administrator**
4. **Review this guide** for common solutions

## Best Practices

1. **Data Quality**: Ensure your data is clean and consistent
2. **Naming**: Use descriptive names for buckets and dashboards
3. **Backup**: Keep copies of your original data files
4. **Testing**: Start with small datasets to test the process
5. **Documentation**: Keep notes of your bucket names and dashboard configurations

## Quick Reference

### Required CSV Headers
```
Health Facility Name,Type,Likely Woreda/Administration,Latitude,Longitude,population,time_period,Total Malaria Cases
```

### Standard Query (replace YOUR_BUCKET_NAME)
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

### Recommended Chart Types
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
