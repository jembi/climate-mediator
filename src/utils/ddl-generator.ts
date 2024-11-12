export function generateDDL(fields: string[], tableName: string) {
  return `CREATE TABLE IF NOT EXISTS ${tableName} (
    ${fields.map((field) => `${field} VARCHAR`).join(", ")}
  )`
}

export function flattenJson(json: any): string[] {
    const fields: string[] = []
    
    return fields
}