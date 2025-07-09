export type PredictRequest = {
  data: string;
};

export type PredictResponse = {
  status?: string;
  ready?: boolean;
  message?: string;
};

export type GetStatusResponse = {
  status?: string;
  ready?: boolean;
  message?: string;
};

export type GetStatusRequest = {};

export type GetResultRequest = {};

export type GetResultResponse = {
  data: Object | string;
};

export interface ModelPrediction {
  predict: (request: PredictRequest) => Promise<PredictResponse>;
  getStatus: (request?: GetStatusRequest) => Promise<GetStatusResponse>;
  getResult: (request?: GetResultRequest) => Promise<GetResultResponse>;
}
