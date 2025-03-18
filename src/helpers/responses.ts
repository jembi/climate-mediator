export interface UploadResponse {
	status: 'success' | 'error';
	code: string;
	message: string;
}

// Helper functions
export const createErrorResponse = (code: string, message: string): UploadResponse => ({
	status: 'error',
	code,
	message,
});
  
export const createSuccessResponse = (code: string, message: string): UploadResponse => ({
	status: 'success',
	code,
	message,
});
