import axios, { AxiosRequestConfig } from "axios";
import logger from '../logger';

// export async function downloadFileFromUrl(url: string): Promise<ArrayBuffer> {
// 	logger.info(`Downloading file from ${url}`);

// 	try {
// 		const response = await axios({
// 			method: "get",
// 			url,
// 			responseType: "arraybuffer"
// 		});

// 		logger.info(`File downloaded successfully from ${url}`);
		
// 		return response.data;
// 	} catch (err) {
// 		logger.error(`Failed to download file from ${url}: ${err}`);
// 		throw err;
// 	}
// }

//I have commented the old function that does not cater for Auth

export async function downloadFileFromUrl(
	url: string,
	auth?: {
	  username: string;
	  password: string;
	} | {
	  token: string;
	  tokenType?: string; 
	}
  ): Promise<ArrayBuffer> {
	logger.info(`Downloading file from ${url}`);
  
	try {
	  const config: AxiosRequestConfig = {
		method: "get",
		url,
		responseType: "arraybuffer"
	  };
  
	  if (auth) {
		if ('username' in auth) {
		  config.auth = {
			username: auth.username,
			password: auth.password
		  };
		} else {
		  config.headers = {
			Authorization: `${auth.tokenType || 'Bearer'} ${auth.token}`
		  };
		}
	  }
  
	  const response = await axios(config);
  
	  logger.info(`File downloaded successfully from ${url}`);
	  
	  return response.data;
	} catch (err) {
	  logger.error(`Failed to download file from ${url}: ${err}`);
	  throw err;
	}
  }

export function validateUrl(url: string) {
	try {
		new URL(url);
		return true;
	} catch (err) {
		return false;
	}
}
