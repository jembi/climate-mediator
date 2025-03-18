import path from "path";
import fs from 'fs/promises';
import logger from "../logger";

export const saveToTmp = async (fileBuffer: Buffer, fileName: string): Promise<string> => {
	const tmpDir = path.join(process.cwd(), 'tmp');
	await fs.mkdir(tmpDir, { recursive: true });

	const fileUrl = path.join(tmpDir, fileName);
	await fs.writeFile(fileUrl, fileBuffer);
	logger.info(`File saved: ${fileUrl}`);

	return fileUrl;
};

export function sanitizeTableName(tableName: string): string {
  return tableName.replace(/[^a-zA-Z0-9_-]/g, '_');
}

