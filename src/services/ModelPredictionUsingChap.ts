import winston from "winston";
import axios from 'axios'
import { GetResultRequest, GetResultResponse, GetStatusRequest, GetStatusResponse, ModelPrediction, PredictRequest, PredictResponse } from "./ModelPrediction";

export class ModelPredictionUsingChap implements ModelPrediction {
	constructor(
		private readonly chapUrl: string,
		private readonly logger: winston.Logger) {
	}

	async predict(request: PredictRequest): Promise<PredictResponse> {
		try {
			this.logger.info('Starting predicting model using CHAP');

			const url = `${this.chapUrl}/predict`;

			const res = (await axios(url, {
				method: 'POST',
				headers: {
					'Content-Type': 'application/json',
				},
				data: request.data
			})).data;

			this.logger.info('Successfully queued model for prediction using CHAP');

			return Promise.resolve(res);
		} catch (err) {
			this.logger.error('Error predicting model using CHAP:', err);
			throw err;
		}
	}
	
	async getStatus(request?: GetStatusRequest): Promise<GetStatusResponse> {
		try {
			this.logger.info('Getting prediction status using CHAP');

			const url = `${this.chapUrl}/status`;

			const res = (await axios(url, {
				method: 'GET',
				headers: {
					'Content-Type': 'application/json',
				},
			})).data;

			this.logger.info('Successfully got prediction status using CHAP');
			this.logger.info(JSON.stringify(res))

			return res
		} catch (err) {
			this.logger.error(`Error getting predicting status using CHAP: ${err}`,);
			throw err;
		}
	}
	
	async getResult(request?: GetResultRequest): Promise<GetResultResponse> {
		try {
			this.logger.info('Getting predicting results using CHAP');

			const url = `${this.chapUrl}/get-results`;

			const res = (await axios(url, {
				method: 'GET',
				headers: {
					'Content-Type': 'application/json',
				},
			})).data;

			this.logger.info('Successfully got prediction results using CHAP');
			this.logger.info(JSON.stringify(res))

			return {data: res}
		} catch (err) {
			this.logger.error('Error getting predicting results using CHAP:', err);
			throw err;
		}
	}
}