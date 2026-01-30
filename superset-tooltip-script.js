// Enhanced Superset Tooltip Script for Mapbox Malaria Predictions
// This script is designed to work with malaria prediction data from the climate-mediator project

d => {
    // Get the data array from the layer
    const dataArray = d.layer?.props?.data;
    
    if (!dataArray || dataArray.length === 0) {
        return createTooltip({
            title: "No Data Available",
            content: "No malaria prediction data found for this location",
            type: "error"
        });
    }

    // Find the hovered point using coordinate matching
    const hoveredPoint = findHoveredPoint(dataArray, d.coordinate);
    
    if (!hoveredPoint) {
        return createTooltip({
            title: "Location Not Found",
            content: "Unable to find data for this map location",
            type: "warning"
        });
    }

    // Extract region information
    const region = hoveredPoint.extraProps?.region || 
                  hoveredPoint.properties?.name || 
                  hoveredPoint.ou || 
                  "Unknown Region";

    // Get all data points for this location
    const locationData = dataArray.filter(entry => 
        isSameLocation(entry, hoveredPoint)
    );

    // Process and sort the data by date
    const processedData = processLocationData(locationData);
    
    // Create the tooltip content
    return createTooltip({
        title: region,
        data: processedData,
        type: "success"
    });
}

// Helper function to find the hovered point
function findHoveredPoint(dataArray, coordinate) {
    const tolerance = 0.01; // Adjust based on your map zoom level
    
    return dataArray.find(entry => {
        const entryCoords = getCoordinates(entry);
        if (!entryCoords) return false;
        
        return Math.abs(entryCoords[0] - coordinate[0]) < tolerance && 
               Math.abs(entryCoords[1] - coordinate[1]) < tolerance;
    });
}

// Helper function to get coordinates from different data structures
function getCoordinates(entry) {
    // Handle different possible coordinate formats
    if (entry.position && Array.isArray(entry.position)) {
        return entry.position;
    }
    if (entry.coordinates && Array.isArray(entry.coordinates)) {
        return entry.coordinates;
    }
    if (entry.geometry && entry.geometry.coordinates) {
        return entry.geometry.coordinates;
    }
    if (entry.extraProps?.coordinates) {
        return entry.extraProps.coordinates;
    }
    return null;
}

// Helper function to check if two entries are from the same location
function isSameLocation(entry1, entry2) {
    const coords1 = getCoordinates(entry1);
    const coords2 = getCoordinates(entry2);
    
    if (!coords1 || !coords2) return false;
    
    const tolerance = 0.001;
    return Math.abs(coords1[0] - coords2[0]) < tolerance && 
           Math.abs(coords1[1] - coords2[1]) < tolerance;
}

// Helper function to process location data
function processLocationData(locationData) {
    const processed = locationData.map(entry => {
        const value = entry.extraProps?.median || 
                     entry.extraProps?.value || 
                     entry.value || 
                     0;
        
        const date = parseDate(entry.extraProps?.date || 
                              entry.pe || 
                              entry.period);
        
        const isPredicted = entry.extraProps?.isPredicted || 
                           entry.isPredicted || 
                           isFutureDate(date);
        
        return {
            value: Math.round(value),
            date: date,
            isPredicted: isPredicted,
            period: entry.pe || entry.period || "Unknown"
        };
    });

    // Sort by date (most recent first)
    return processed.sort((a, b) => new Date(b.date) - new Date(a.date));
}

// Helper function to parse dates from different formats
function parseDate(dateString) {
    if (!dateString) return new Date();
    
    // Handle YYYYMM format (e.g., "202401")
    if (/^\d{6}$/.test(dateString)) {
        const year = dateString.substring(0, 4);
        const month = dateString.substring(4, 6);
        return new Date(parseInt(year), parseInt(month) - 1, 1);
    }
    
    // Handle other date formats
    return new Date(dateString);
}

// Helper function to determine if a date is in the future
function isFutureDate(date) {
    const now = new Date();
    return date > now;
}

// Helper function to create styled tooltip
function createTooltip({ title, content, data, type }) {
    const baseStyles = {
        fontFamily: "'Inter', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
        fontSize: "14px",
        fontWeight: "500",
        width: "320px",
        maxWidth: "90vw",
        borderRadius: "8px",
        boxShadow: "0 8px 32px rgba(0, 0, 0, 0.3)",
        border: "none",
        overflow: "hidden",
        position: "relative",
        zIndex: "1000"
    };

    const typeStyles = {
        success: {
            backgroundColor: "#1e40af",
            color: "#ffffff",
            borderLeft: "4px solid #10b981"
        },
        warning: {
            backgroundColor: "#f59e0b",
            color: "#ffffff",
            borderLeft: "4px solid #fbbf24"
        },
        error: {
            backgroundColor: "#dc2626",
            color: "#ffffff",
            borderLeft: "4px solid #f87171"
        }
    };

    const styles = { ...baseStyles, ...typeStyles[type] };

    if (content) {
        return `
            <div style="${objectToStyles(styles)}">
                <div style="padding: 16px;">
                    <div style="font-size: 16px; font-weight: 600; margin-bottom: 8px;">
                        ${title}
                    </div>
                    <div style="font-size: 13px; opacity: 0.9;">
                        ${content}
                    </div>
                </div>
            </div>
        `;
    }

    if (data && data.length > 0) {
        const dataItems = data.map(item => {
            const month = item.date.toLocaleString('default', { month: 'short' });
            const year = item.date.getFullYear();
            const textColor = item.isPredicted ? '#fbbf24' : '#ffffff';
            const label = item.isPredicted ? 'Pred' : 'Hist';
            
            return `
                <div style="
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    padding: 8px 0;
                    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
                    min-height: 20px;
                ">
                    <div style="color: ${textColor}; font-weight: 600; font-size: 13px;">
                        ${item.value.toLocaleString()} cases
                    </div>
                    <div style="
                        display: flex;
                        flex-direction: column;
                        align-items: flex-end;
                        font-size: 11px;
                    ">
                        <div style="opacity: 0.8; margin-bottom: 2px;">${month} ${year}</div>
                        <div style="
                            background: ${item.isPredicted ? '#f59e0b' : 'rgba(255, 255, 255, 0.2)'};
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
            <div style="${objectToStyles(styles)}">
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
                        <span style="white-space: nowrap; overflow: hidden; text-overflow: ellipsis;">${title}</span>
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
            </div>
        `;
    }

    return `
        <div style="${objectToStyles(styles)}">
            <div style="padding: 16px;">
                <div style="font-size: 16px; font-weight: 600;">
                    ${title}
                </div>
            </div>
        </div>
    `;
}

// Helper function to convert object to CSS styles
function objectToStyles(obj) {
    return Object.entries(obj)
        .map(([key, value]) => `${key.replace(/([A-Z])/g, '-$1').toLowerCase()}: ${value}`)
        .join('; ');
}
