export function validateJsonFile(file: Buffer) {
  const json = file.toString();
  try {
    JSON.parse(json);
  } catch (e) {
    return false;
  }
  return true;
}

export function getCsvHeaders(file: Buffer) {
  //convert the buffer to a string
  const csv = file.toString();
  //check if the new line character is \n or \r\n
  const newLineChar = csv.includes('\r\n') ? '\r\n' : '\n';
  //get the first line of the csv file
  const firstLine = csv.split(newLineChar)[0];
  //split the first line by commas
  const columns = firstLine.split(',');

  if (columns.length === 0) return false;

  return columns;
}
