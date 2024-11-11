export function validateJsonFile(file: Buffer) {
  const json = file.toString();
  try {
    JSON.parse(json);
  } catch (e) {
    return false;
  }
  return true;
}

export function validateCsvFile(file: Buffer) {
  const csv = file.toString();
  // Split the string by newlines to get individual lines
  const lines = csv.trim().split("\n");

  // Check if we have data and at least one line
  if (lines.length === 0) return false;

  // Get the number of columns by splitting the first line by commas
  const columnCount = lines[0].split(",").length;

  // Check each line to ensure it has the same number of columns
  for (const line of lines) {
    // Ignore empty lines (could happen at end of data)
    if (line.trim() === "") continue;

    // Split line by commas, accounting for potential quoted fields
    const columns = line.match(/(".*?"|[^",]+)(?=\s*,|\s*$)/g);

    // Check if the column count matches the first line
    if (!columns || columns.length !== columnCount) {
      return false;
    }
  }

  return true;
}
