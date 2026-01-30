// Simplified Superset Tooltip Script for Mapbox Malaria Predictions
// Easy-to-use version with clear structure and debugging

d => {
    console.log('Tooltip triggered with data:', d); // Debug log
    
    // Get data array
    const dataArray = d.layer?.props?.data || [];
    
    if (!dataArray || dataArray.length === 0) {
        return `
            <div style="
                background: #dc2626;
                color: white;
                padding: 12px;
                border-radius: 6px;
                font-family: Arial, sans-serif;
                font-size: 14px;
                max-width: 250px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.2);
            ">
                <strong>No Data Available</strong><br>
                No malaria data found for this location.
            </div>
        `;
    }

    // Find the closest data point to hovered coordinates
    const hoveredPoint = findClosestPoint(dataArray, d.coordinate);
    
    if (!hoveredPoint) {
        return `
            <div style="
                background: #f59e0b;
                color: white;
                padding: 12px;
                border-radius: 6px;
                font-family: Arial, sans-serif;
                font-size: 14px;
                max-width: 250px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.2);
            ">
                <strong>Location Not Found</strong><br>
                Unable to match coordinates to data point.
            </div>
        `;
    }

    // Extract region name
    const regionName = getRegionName(hoveredPoint);
    
    // Get all data for this location
    const locationData = getAllDataForLocation(dataArray, hoveredPoint);
    
    // Sort by date (newest first)
    locationData.sort((a, b) => new Date(b.date) - new Date(a.date));
    
    // Create tooltip content
    return createTooltipContent(regionName, locationData);
}

// Find the closest data point to the hovered coordinates
function findClosestPoint(dataArray, hoveredCoords) {
    let closestPoint = null;
    let minDistance = Infinity;
    const tolerance = 0.1; // Adjust this value based on your map zoom level
    
    dataArray.forEach(point => {
        const pointCoords = getPointCoordinates(point);
        if (!pointCoords) return;
        
        const distance = Math.sqrt(
            Math.pow(pointCoords[0] - hoveredCoords[0], 2) + 
            Math.pow(pointCoords[1] - hoveredCoords[1], 2)
        );
        
        if (distance < minDistance && distance < tolerance) {
            minDistance = distance;
            closestPoint = point;
        }
    });
    
    return closestPoint;
}

// Get coordinates from various possible data structures
function getPointCoordinates(point) {
    // Try different coordinate field names
    if (point.position && Array.isArray(point.position)) {
        return point.position;
    }
    if (point.coordinates && Array.isArray(point.coordinates)) {
        return point.coordinates;
    }
    if (point.geometry && point.geometry.coordinates) {
        return point.geometry.coordinates;
    }
    if (point.extraProps && point.extraProps.coordinates) {
        return point.extraProps.coordinates;
    }
    return null;
}

// Extract region name from various possible field names
function getRegionName(point) {
    return point.extraProps?.region || 
           point.properties?.name || 
           point.ou || 
           point.name || 
           "Unknown Region";
}

// Get all data points for the same location
function getAllDataForLocation(dataArray, referencePoint) {
    const refCoords = getPointCoordinates(referencePoint);
    if (!refCoords) return [];
    
    return dataArray
        .filter(point => {
            const pointCoords = getPointCoordinates(point);
            if (!pointCoords) return false;
            
            const tolerance = 0.001; // Very small tolerance for same location
            return Math.abs(pointCoords[0] - refCoords[0]) < tolerance && 
                   Math.abs(pointCoords[1] - refCoords[1]) < tolerance;
        })
        .map(point => ({
            value: getValue(point),
            date: getDate(point),
            isPredicted: isPredicted(point),
            period: getPeriod(point)
        }));
}

// Extract value from various possible field names
function getValue(point) {
    return point.extraProps?.median || 
           point.extraProps?.value || 
           point.value || 
           0;
}

// Extract and parse date from various possible field names
function getDate(point) {
    const dateStr = point.extraProps?.date || point.pe || point.period;
    if (!dateStr) return new Date();
    
    // Handle YYYYMM format (e.g., "202401")
    if (/^\d{6}$/.test(dateStr)) {
        const year = dateStr.substring(0, 4);
        const month = dateStr.substring(4, 6);
        return new Date(parseInt(year), parseInt(month) - 1, 1);
    }
    
    return new Date(dateStr);
}

// Determine if data point is predicted
function isPredicted(point) {
    if (point.extraProps?.isPredicted !== undefined) {
        return point.extraProps.isPredicted;
    }
    
    const date = getDate(point);
    return date > new Date(); // Future dates are predictions
}

// Get period string
function getPeriod(point) {
    return point.pe || point.period || "Unknown";
}

// Create the tooltip HTML content
function createTooltipContent(regionName, data) {
    const dataItems = data.map(item => {
        const month = item.date.toLocaleString('default', { month: 'short' });
        const year = item.date.getFullYear();
        const valueColor = item.isPredicted ? '#fbbf24' : '#ffffff';
        const label = item.isPredicted ? 'Pred' : 'Hist';
        const labelColor = item.isPredicted ? '#f59e0b' : '#6b7280';
        
        return `
            <div style="
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 8px 0;
                border-bottom: 1px solid rgba(255, 255, 255, 0.1);
                min-height: 20px;
            ">
                <span style="color: ${valueColor}; font-weight: 600; font-size: 13px;">
                    ${item.value.toLocaleString()} cases
                </span>
                <div style="text-align: right; font-size: 11px;">
                    <div style="opacity: 0.8; margin-bottom: 2px;">${month} ${year}</div>
                    <div style="
                        background: ${labelColor};
                        color: white;
                        padding: 2px 6px;
                        border-radius: 3px;
                        font-size: 9px;
                        display: inline-block;
                    ">${label}</div>
                </div>
            </div>
        `;
    }).join('');

    return `
        <div style="
            background: #1e40af;
            color: white;
            padding: 0;
            border-radius: 8px;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 14px;
            width: 320px;
            max-width: 90vw;
            box-shadow: 0 8px 32px rgba(0,0,0,0.3);
            border-left: 4px solid #10b981;
            position: relative;
            z-index: 1000;
        ">
            <div style="
                padding: 16px 16px 12px 16px;
                border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            ">
                <div style="
                    font-size: 16px;
                    font-weight: 600;
                    display: flex;
                    align-items: center;
                ">
                    <div style="
                        width: 8px;
                        height: 8px;
                        background: #10b981;
                        border-radius: 50%;
                        margin-right: 8px;
                        flex-shrink: 0;
                    "></div>
                    <span style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${regionName}</span>
                </div>
            </div>
            <div style="
                max-height: 300px;
                overflow-y: auto;
                overflow-x: hidden;
                padding: 8px 16px 16px 16px;
                scrollbar-width: thin;
                scrollbar-color: rgba(255, 255, 255, 0.3) transparent;
            ">
                ${dataItems}
            </div>
            <style>
                /* Custom scrollbar for webkit browsers */
                .tooltip-scroll::-webkit-scrollbar {
                    width: 6px;
                }
                .tooltip-scroll::-webkit-scrollbar-track {
                    background: rgba(255, 255, 255, 0.1);
                    border-radius: 3px;
                }
                .tooltip-scroll::-webkit-scrollbar-thumb {
                    background: rgba(255, 255, 255, 0.3);
                    border-radius: 3px;
                }
                .tooltip-scroll::-webkit-scrollbar-thumb:hover {
                    background: rgba(255, 255, 255, 0.5);
                }
            </style>
        </div>
    `;
}
